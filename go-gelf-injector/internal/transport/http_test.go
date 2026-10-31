package transport

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// server answers each request of a connection with codes[i] (then 200), and
// hangs up after hangAfter responses when > 0, without "Connection: close".
func server(t *testing.T, codes []int, hangAfter int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				for i := 0; ; i++ {
					r, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					r.Body.Close()
					code := 200
					if i < len(codes) {
						code = codes[i]
					}
					c.Write([]byte("HTTP/1.1 " + strconv.Itoa(code) + " X\r\nRetry-After: 0\r\nContent-Length: 0\r\n\r\n"))
					if hangAfter > 0 && i+1 == hangAfter {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

type outcomes struct {
	mu sync.Mutex
	m  map[uint64][]string
}

func (o *outcomes) done(seq uint64) sched.DoneFunc {
	return func(outcome string, _ bool) {
		o.mu.Lock()
		o.m[seq] = append(o.m[seq], outcome)
		o.mu.Unlock()
	}
}

func (o *outcomes) get(seq uint64) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.m[seq]
}

func dialH1(t *testing.T, addr string, perConn uint64) *h1Conn {
	t.Helper()
	d, err := Dialer(Options{Network: "http", Target: addr, Path: "/gelf", Host: "h", Timeout: time.Second,
		ResponseTimeout: 2 * time.Second, Pipeline: 1, PerConn: perConn, Retries: 3, RetryMaxWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c, err := d(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return c.(*h1Conn)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200 && !cond(); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not reached")
	}
}

func unsentSeqs(h *h1Conn) []uint64 {
	var s []uint64
	for _, p := range h.Unsent() {
		s = append(s, p.Seq)
	}
	return s
}

// A retry and a new request that meet a connection the server already hung
// up on were never written: both go back to the worker, neither fails
// (review finding 1, 2026-10-31).
func TestDeadConnGivesBackRetryAndNewRequest(t *testing.T) {
	o := &outcomes{m: map[uint64][]string{}}
	h := dialH1(t, server(t, []int{429}, 1), 0)
	m := &source.Message{Data: []byte("{}")}
	if err := h.Send(1, m, o.done(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.dead != nil && len(h.retries) == 1 })
	if err := h.Send(2, m, o.done(2)); err == nil {
		t.Fatal("Send on a dead connection succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.Close(ctx)
	if got := unsentSeqs(h); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("unsent %v", got)
	}
	if o.get(1) != nil || o.get(2) != nil {
		t.Fatalf("done called: %v %v", o.get(1), o.get(2))
	}
}

// With --requests-per-conn 2, a retry that becomes the second request spends
// the connection: Flush says so, and a later request is given back instead
// of written behind the "Connection: close" (review finding 2).
func TestSpentByRetryTakesNothingMore(t *testing.T) {
	o := &outcomes{m: map[uint64][]string{}}
	h := dialH1(t, server(t, []int{429}, 0), 2)
	m := &source.Message{Data: []byte("{}")}
	if err := h.Send(1, m, o.done(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return len(h.retries) == 1 })
	if err := h.Flush(); !errors.Is(err, sched.ErrSpent) {
		t.Fatalf("Flush after the spending resend: %v", err)
	}
	if err := h.Send(3, m, o.done(3)); !errors.Is(err, sched.ErrSpent) {
		t.Fatalf("Send on a spent connection: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.Close(ctx)
	if got := o.get(1); len(got) != 1 || got[0] != "200" {
		t.Fatalf("seq 1: %v", got)
	}
	if got := unsentSeqs(h); len(got) != 1 || got[0] != 3 || o.get(3) != nil {
		t.Fatalf("unsent %v, seq 3 outcomes %v", got, o.get(3))
	}
}

// A keep-alive server that closes an idle connection (its idle timeout, or a
// shutdown between requests) ends it as planned: the next request is given
// back unwritten and the worker is told ErrSpent, not a broken connection.
func TestIdleCloseIsPlanned(t *testing.T) {
	o := &outcomes{m: map[uint64][]string{}}
	h := dialH1(t, server(t, nil, 1), 0)
	m := &source.Message{Data: []byte("{}")}
	if err := h.Send(1, m, o.done(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.dead != nil })
	if err := h.Send(2, m, o.done(2)); !errors.Is(err, sched.ErrSpent) {
		t.Fatalf("Send after an idle close: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h.Close(ctx)
	u := h.Unsent()
	if len(u) != 1 || u[0].Seq != 2 || !u[0].Unwritten || o.get(2) != nil {
		t.Fatalf("unsent %+v", u)
	}
	if got := o.get(1); len(got) != 1 || got[0] != "200" {
		t.Fatalf("seq 1: %v", got)
	}
}
