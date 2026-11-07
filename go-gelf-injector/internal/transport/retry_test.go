package transport

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// answerServer answers the requests of its connections with codes, in
// order across connections, with Retry-After: after; it closes each
// connection after one response (the idle timeout of a keep-alive server).
func answerServer(t *testing.T, after int, codes ...int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for i := 0; ; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			r, err := http.ReadRequest(bufio.NewReader(c))
			if err == nil {
				r.Body.Close()
				code := 200
				if i < len(codes) {
					code = codes[i]
				}
				c.Write([]byte("HTTP/1.1 " + strconv.Itoa(code) + " X\r\nRetry-After: " + strconv.Itoa(after) + "\r\nContent-Length: 0\r\n\r\n"))
			}
			c.Close()
		}
	}()
	return ln.Addr().String()
}

func dialRetry(t *testing.T, addr string, retries int) *h1Conn {
	t.Helper()
	d, err := Dialer(Options{Network: "http", Target: addr, Path: "/gelf", Host: "h", Timeout: time.Second,
		ResponseTimeout: 2 * time.Second, Pipeline: 1, Retries: retries, RetryMaxWait: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	c, err := d(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return c.(*h1Conn)
}

// A 429 whose Retry-After still runs when the server closes the idle
// connection is handed on with its schedule (pcap/replay item 4 of the
// review of 2026-11-07, the same hole as WI's), and the next connection
// adopts it: it goes when the Retry-After ends, not at once, and --retries
// counts the tries of both connections (alignment 8(e)).
func TestRetryHandedOnAndAdopted(t *testing.T) {
	addr := answerServer(t, 1, 429, 429)
	o := &outcomes{m: map[uint64][]string{}}
	h := dialRetry(t, addr, 1)
	if err := h.Send(1, &source.Message{Data: []byte(`{"short_message":"x"}`)}, o.done(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.dead != nil && len(h.retries) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Close(ctx)
	us := h.Unsent()
	if len(us) != 1 || us[0].Tries != 1 || us[0].Last != "429" || !us[0].Due.After(time.Now()) || o.get(1) != nil {
		t.Fatalf("unsent %+v, outcomes %v", us, o.get(1))
	}
	// The next connection: the resend waits for the Retry-After left, and
	// its 429 is the last try (--retries 1 counted on the first one).
	h2 := dialRetry(t, addr, 1)
	start := time.Now()
	if err := h2.Adopt(us[0]); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	h2.Close(ctx2)
	if got := o.get(1); len(got) != 1 || got[0] != "429" {
		t.Fatalf("outcome %v", got)
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Fatalf("resent after %s, before the Retry-After left", d)
	}
}
