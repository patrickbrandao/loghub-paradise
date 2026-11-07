package receiver

// Port of the independent review of go-syslog-standard-server, 2026-11-07
// (its SPEC §21.1, item 23): the RELP budget taken from the declared DATALEN
// before the payload is read, and granted in FIFO order (SPEC §5.6).

import (
	"bufio"
	"context"
	"fmt"
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
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{RELP: RELPConfig{Budget: b}}})
	rc := dialRELP(t, tr.Addr())

	msg := "<13>" + strings.Repeat("p", 30000)
	rc.write(fmt.Sprintf("2 syslog %d ", len(msg))) // the head only
	waitFor(t, "the reader waiting for the budget before the payload arrived", func() bool {
		return h.m.RELPInflightWaitsTotal.Load() == 1
	})
	b.Release(held)
	rc.write(msg + "\n")
	rc.expect(2, "200 OK")
	waitFor(t, "the budget back to zero", func() bool { return b.Used() == 0 })
}

// The wait for the budget may outlast the idle timeout: the deadline is
// re-armed once the bytes are granted, so the payload that follows is read
// instead of the session ending in a timeout armed before the wait.
func TestRELPIdleClockRestartsAfterTheBudgetWait(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	b := NewBudget(64<<10, h.m)
	held, _ := b.Acquire(context.Background(), b.Max())
	tr := startTCP(t, h, TCPConfig{StreamConfig: StreamConfig{
		IdleTimeout: 300 * time.Millisecond, RELP: RELPConfig{Budget: b}}})
	rc := dialRELP(t, tr.Addr())

	msg := "<13>after a long wait"
	rc.write(fmt.Sprintf("2 syslog %d ", len(msg)))
	waitFor(t, "the reader waiting for the budget", func() bool {
		return h.m.RELPInflightWaitsTotal.Load() == 1
	})
	time.Sleep(500 * time.Millisecond) // past the idle timeout armed before ReadHead
	b.Release(held)
	time.Sleep(50 * time.Millisecond) // the grant and the re-arm
	rc.write(msg + "\n")
	rc.expect(2, "200 OK")
}

// The reader lets go of a large frame buffer before it parks on the window or
// the budget (SPEC §5.6): the previous frame is already copied.
func TestRELPReaderDropsTheBufferBeforeAWait(t *testing.T) {
	body := strings.Repeat("z", 40<<10)
	in := fmt.Sprintf("1 syslog %d %s\n", len(body), body)
	rd := NewRELPReader(bufio.NewReaderSize(strings.NewReader(in), relpReadBuffer), 1<<20, 64<<10)
	if _, err := rd.ReadFrame(); err != nil {
		t.Fatal(err)
	}
	if cap(rd.buf) <= relpKeepBlock {
		t.Fatalf("cap = %d: the frame did not grow the buffer", cap(rd.buf))
	}
	rd.DropBuffer()
	if cap(rd.buf) > relpKeepBlock {
		t.Fatalf("cap = %d after DropBuffer", cap(rd.buf))
	}
}

// The charge comes from the declared length cut at syslog.max_message_bytes:
// what the copy will hold, not what the wire carries (SPEC §5.3, §5.6).
func TestRELPHeadChargeIsTheKeptLength(t *testing.T) {
	in := "1 syslog 100000 "
	rd := NewRELPReader(bufio.NewReader(strings.NewReader(in)), 1<<20, 2<<10)
	hd, err := rd.ReadHead()
	if err != nil {
		t.Fatal(err)
	}
	if hd.Size != 100000 || rd.Kept(hd) != 2<<10 {
		t.Fatalf("size = %d, kept = %d", hd.Size, rd.Kept(hd))
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
