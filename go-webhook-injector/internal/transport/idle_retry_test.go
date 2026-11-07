package transport

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/source"
)

// A 429 whose Retry-After is still running when the server closes the idle
// connection is handed on with its schedule, not ended with the 429 (review
// of 2026-11-07, pcap/replay item 4: Close handed on only for a spent or
// "Connection: close" connection).
func TestIdleCloseHandsOnRetry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r, err := http.ReadRequest(bufio.NewReader(c))
		if err != nil {
			return
		}
		r.Body.Close()
		// "Not now, in 3 s" -- then the idle timeout of the server.
		c.Write([]byte("HTTP/1.1 429 X\r\nRetry-After: 3\r\nContent-Length: 0\r\n\r\n"))
	}()
	u, _ := url.Parse("http://" + ln.Addr().String())
	d, err := Dialer(Options{URL: u, Timeout: time.Second, ResponseTimeout: 2 * time.Second, Pipeline: 1,
		Retries: 3, RetryMaxWait: 10 * time.Second,
		Build: func(seq uint64, m *source.Message) Req {
			return Req{Method: "POST", URI: "/" + strconv.FormatUint(seq, 10), Host: "h", Body: m.Body}
		}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := d(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h := c.(*h1Conn)
	o := &outcomes{m: map[uint64][]string{}}
	if err := h.Send(1, &source.Message{Body: []byte("x")}, o.done(1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.dead != nil && len(h.retries) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Close(ctx)
	us := h.Unsent()
	if len(us) != 1 || us[0].Seq != 1 || us[0].Tries != 1 || us[0].Last != "429" || !us[0].Due.After(time.Now()) {
		t.Fatalf("unsent %+v", us)
	}
	if o.get(1) != nil {
		t.Fatalf("ended with %v instead of handed on", o.get(1))
	}
}
