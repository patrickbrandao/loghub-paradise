package receiver

// Tests of the independent review of 2026-11-07 (SPEC §21.1, items 21-29)
// in this package: the RELP budget taken before the payload and granted in
// FIFO order (item 3 of the review, SPEC §4.5).

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// The budget is taken from the DECLARED length, before a byte of the payload
// is read. The frame's head alone makes the reader park on the exhausted
// budget; reading the frame first, the reader sat in the payload read and
// never reached the budget.
func TestRELPTakesTheBudgetBeforeThePayload(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	b := NewBudget(64<<10, h.m)
	held, _ := b.Acquire(context.Background(), b.Max()) // exhaust it
	r := startRELP(t, h, RELPConfig{Budget: b})

	c, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	relpSend(t, c, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	if _, body := readRSP(t, c, br); !strings.HasPrefix(body, "200 OK") {
		t.Fatalf("open answered %q", body)
	}
	msg := "<13>" + strings.Repeat("p", 30000)
	fmt.Fprintf(c, "2 syslog %d ", len(msg)) // the head only
	deadline := time.Now().Add(3 * time.Second)
	for h.m.RELPInflightWaitsTotal.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if h.m.RELPInflightWaitsTotal.Load() == 0 {
		t.Fatal("the reader did not wait for the budget before the payload arrived")
	}
	b.Release(held)
	fmt.Fprintf(c, "%s\n", msg)
	if txnr, body := readRSP(t, c, br); txnr != 2 || body != "200 OK" {
		t.Fatalf("after the release: %d %q", txnr, body)
	}
}

func waitWaiting(t *testing.T, b *Budget, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for b.waiting() != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := b.waiting(); got != n {
		t.Fatalf("%d waiters, want %d", got, n)
	}
}

// Acquire is FIFO. A charge clamped to the ceiling (it needs the budget empty)
// is not starved by smaller ones that would fit in the gap, and a waiter that
// gives up lets the next one in.
func TestBudgetIsFIFO(t *testing.T) {
	b := NewBudget(100, newMetrics())
	held, _ := b.Acquire(context.Background(), 60)

	bigDone := make(chan int64, 1)
	go func() {
		n, _ := b.Acquire(context.Background(), 1000) // clamped to 100
		bigDone <- n
	}()
	waitWaiting(t, b, 1)
	smallDone := make(chan int64, 1)
	go func() {
		n, _ := b.Acquire(context.Background(), 10) // 60 + 10 fits
		smallDone <- n
	}()
	select {
	case <-smallDone:
		t.Fatal("a small charge jumped ahead of the clamped one waiting")
	case <-time.After(100 * time.Millisecond):
	}
	b.Release(held)
	big := <-bigDone
	if big != 100 {
		t.Fatalf("clamped charge = %d", big)
	}
	select {
	case <-smallDone:
		t.Fatal("the small charge got bytes the clamped one holds")
	case <-time.After(50 * time.Millisecond):
	}
	b.Release(big)
	if n := <-smallDone; n != 10 || b.Used() != 10 {
		t.Fatalf("small = %d, used = %d", n, b.Used())
	}

	// A head of the queue that gives up lets the next one in.
	ctx, cancel := context.WithCancel(context.Background())
	headDone := make(chan bool, 1)
	go func() {
		_, ok := b.Acquire(ctx, 100)
		headDone <- ok
	}()
	waitWaiting(t, b, 1)
	nextDone := make(chan int64, 1)
	go func() {
		n, _ := b.Acquire(context.Background(), 20)
		nextDone <- n
	}()
	waitWaiting(t, b, 2)
	cancel()
	if ok := <-headDone; ok {
		t.Fatal("the cancelled head was granted")
	}
	select {
	case n := <-nextDone:
		if n != 20 || b.Used() != 30 {
			t.Fatalf("next = %d, used = %d", n, b.Used())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter behind a cancelled head was never granted")
	}
}
