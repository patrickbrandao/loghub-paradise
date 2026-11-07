package receiver

// Tests of the §21.1 items fixed on 2026-11-07 in this package: the fixed cost
// of a connection (item 1), the RELP in-flight budget (item 2), the counter of
// per-IP refusals (item 18) and the relp_software name (item 20).

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// Item 1: the reader every connection holds for its whole life is 4 KiB. At
// 64 KiB a few thousand idle connections were an OOM kill outside
// ring.max_bytes (SPEC §5.5).
func TestConnectionReadBufferIsSmall(t *testing.T) {
	if tcpReadBuffer > 4<<10 {
		t.Fatalf("tcpReadBuffer = %d; every idle TCP or RELP connection holds it", tcpReadBuffer)
	}
}

// Item 1: a client that declares a large octet count and then sends a few
// bytes costs the first small block, not a 64 KiB chunk (SPEC §5.5).
func TestCountedReadStartsSmall(t *testing.T) {
	r := bufio.NewReaderSize(strings.NewReader("abc"), tcpReadBuffer)
	var buf []byte
	if err := readCounted(r, &buf, 1<<20); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v", err)
	}
	if string(buf) != "abc" {
		t.Fatalf("buf = %q", buf)
	}
	// A literal, not minFrameCap: the rule is "a small first block", and a
	// test against the constant would follow the constant anywhere.
	if cap(buf) > 4<<10 {
		t.Fatalf("cap = %d after three bytes of a 1 MiB declaration; want at most 4 KiB", cap(buf))
	}
}

// Item 1: a large frame still arrives whole, and the buffer it grew is not
// kept for the rest of the connection's life.
func TestFramerDropsALargeBufferBetweenFrames(t *testing.T) {
	big := "<13>" + strings.Repeat("x", 200<<10)
	in := fmt.Sprintf("%d %s4 <13>", len(big), big)
	f := NewFramer(bufio.NewReaderSize(strings.NewReader(in), tcpReadBuffer), envelope.FramingOctet, false, 1<<20)
	got, err := f.Next()
	if err != nil || string(got) != big {
		t.Fatalf("large frame: err = %v, len = %d", err, len(got))
	}
	got, err = f.Next()
	if err != nil || string(got) != "<13>" {
		t.Fatalf("small frame: err = %v, %q", err, got)
	}
	if cap(f.buf) > frameKeepCap {
		t.Fatalf("cap = %d after a small frame; the 200 KiB buffer was kept", cap(f.buf))
	}
}

// Item 2: Acquire waits while the ceiling is exhausted and wakes on Release;
// a charge above the ceiling is clamped so it can never wait forever.
func TestBudgetWaitsAndClamps(t *testing.T) {
	m := newMetrics()
	b := NewBudget(100, m)
	got, ok := b.Acquire(context.Background(), 1000)
	if !ok || got != 100 {
		t.Fatalf("clamped acquire = %d, %v; want 100, true", got, ok)
	}
	done := make(chan int64, 1)
	go func() {
		n, _ := b.Acquire(context.Background(), 10)
		done <- n
	}()
	select {
	case <-done:
		t.Fatal("Acquire returned with the budget exhausted")
	case <-time.After(100 * time.Millisecond):
	}
	if m.RELPInflightWaitsTotal.Load() != 1 {
		t.Fatalf("waits = %d, want 1", m.RELPInflightWaitsTotal.Load())
	}
	b.Release(got)
	if n := <-done; n != 10 {
		t.Fatalf("second acquire = %d", n)
	}
	if b.Used() != 10 || m.RELPInflightBytes.Load() != 10 {
		t.Fatalf("used = %d, gauge = %d, want 10", b.Used(), m.RELPInflightBytes.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := b.Acquire(ctx, 95); ok {
		t.Fatal("Acquire succeeded past the ceiling with a cancelled context")
	}
}

// Item 2, the wiring: a RELP transaction waits for the budget BEFORE it is
// copied and enqueued, and the reader answers it once the budget frees up.
// Without the Acquire in serve, the frame would be acknowledged at once.
func TestRELPWaitsForTheInflightBudget(t *testing.T) {
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
	relpSend(t, c, 2, "syslog", "<13>held by the budget")

	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if line, err := br.ReadString('\n'); err == nil {
		t.Fatalf("answered %q with the in-flight budget exhausted", line)
	}
	if n := h.ring.Stats().Items; n != 0 {
		t.Fatalf("the ring holds %d items; the transaction went past the budget", n)
	}
	if h.m.RELPInflightWaitsTotal.Load() == 0 {
		t.Fatal("the wait was not counted")
	}

	b.Release(held)
	br = bufio.NewReader(c)
	if txnr, body := readRSP(t, c, br); txnr != 2 || body != "200 OK" {
		t.Fatalf("after the release: %d %q", txnr, body)
	}
	deadline := time.Now().Add(2 * time.Second)
	for b.Used() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if b.Used() != 0 {
		t.Fatalf("budget still holds %d bytes after the transaction was decided", b.Used())
	}
}

// Item 18: the per-IP refusal is counted, by transport (SPEC §13.1).
func TestPerIPRefusalIsCounted(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	tr := startTCP(t, h, TCPConfig{MaxConnsPerIP: 1})
	first, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	fmt.Fprintf(first, "<13>first\n")
	h.waitItems(t, 1)

	second, err := net.Dial("tcp", tr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	deadline := time.Now().Add(3 * time.Second)
	for h.m.ConnsRefusedTotal[obs.TransportTCP].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := h.m.ConnsRefusedTotal[obs.TransportTCP].Load(); got != 1 {
		t.Fatalf("syslog_conns_refused_total{transport=tcp} = %d, want 1", got)
	}
	if got := h.m.ConnsRefusedTotal[obs.TransportRELP].Load(); got != 0 {
		t.Fatalf("relp counter = %d, want 0", got)
	}
}

// Item 20: the open handshake names this server, never go-syslog-server.
func TestRELPAdvertisesItsOwnName(t *testing.T) {
	h := newHarness(t, 100, "debug", nil)
	r := startRELP(t, h, RELPConfig{})
	c, err := net.Dial("tcp", r.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	relpSend(t, c, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	_, body := readRSP(t, c, br)
	if !strings.Contains(body, "relp_software=go-syslog-standard-server,test") {
		t.Fatalf("handshake = %q", body)
	}
}
