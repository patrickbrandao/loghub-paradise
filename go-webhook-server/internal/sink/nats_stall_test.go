package sink

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ring"
)

// blackHoleNATS completes the NATS handshake (INFO, CONNECT, PING/PONG) and
// then, at the first PUB/HPUB of each connection, stops reading that socket
// for good, without closing it: what a frozen host, a paused container or a
// partition that drops packets looks like to the client (review of
// 2026-11-21, item 001). Every connection is accepted, so a reconnection lands
// on another black hole. With tc, the INFO says tls_required and the rest of
// the conversation is TLS: the sink's own dialer sits under the client's TLS
// (SPEC §8.7.9). stalled counts the connections that got as far as a publish,
// so a test cannot pass on a client that never connected.
func blackHoleNATS(t *testing.T, tc *tls.Config) (url string, stalled *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stalled = new(atomic.Int32)
	done := make(chan struct{})
	var conns sync.WaitGroup
	t.Cleanup(func() {
		close(done)
		ln.Close()
		conns.Wait()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Add(1)
			go func(c net.Conn) {
				defer conns.Done()
				defer c.Close() // the socket, not the TLS: a close_notify would block on a full socket
				info := `{"server_id":"BH","server_name":"bh","version":"2.14.6","proto":1,"host":"127.0.0.1",` +
					`"port":4222,"headers":true,"max_payload":67108864`
				if tc != nil {
					info += `,"tls_required":true`
				}
				if _, err := c.Write([]byte("INFO " + info + "}\r\n")); err != nil {
					return
				}
				var rw io.ReadWriter = c
				if tc != nil {
					s := tls.Server(c, tc)
					if err := s.Handshake(); err != nil {
						return
					}
					rw = s
				}
				rd := bufio.NewReader(rw)
				for {
					line, err := rd.ReadString('\n')
					if err != nil {
						return
					}
					f := strings.Fields(line)
					if len(f) == 0 {
						continue
					}
					switch f[0] {
					case "PING":
						_, _ = rw.Write([]byte("PONG\r\n"))
					case "PUB", "HPUB":
						stalled.Add(1)
						<-done // never read this socket again
						return
					}
				}
			}(c)
		}
	}()
	return "nats://" + ln.Addr().String(), stalled
}

// newBlackHoleNATS is a sink against a black hole; with withTLS, a TLS one
// whose certificate the sink trusts (sink.nats.tls with sink.nats.tls_ca).
func newBlackHoleNATS(t *testing.T, opTimeout time.Duration, withTLS bool) (*NATS, *atomic.Int32) {
	t.Helper()
	cfg := NATSConfig{Stream: "LOGHUB_WEBHOOK", Subject: "loghub.{space}.{type}", Workspace: "default",
		ConnectTimeout: 2 * time.Second, OpTimeout: opTimeout}
	var tc *tls.Config
	if withTLS {
		certFile, keyFile := natsTLSCert(t)
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatal(err)
		}
		tc = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
		cfg.TLS, cfg.TLSCA = true, certFile
	}
	url, stalled := blackHoleNATS(t, tc)
	cfg.URLs = []string{url}
	n, err := NewNATS(cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n, stalled
}

// SPEC §8.7.1: a Write against a peer that stopped reading returns within
// sink.op_timeout, as network -- with sink.nats.tls too, where the deadline
// reaches the socket through the client's TLS (SPEC §8.7.9). Before
// 2026-11-23 the inline flush of an 8 MiB envelope waited out nats.go's
// one-minute FlusherTimeout.
func TestNATSWriteHonoursOpTimeoutAgainstAPeerThatStopsReading(t *testing.T) {
	for _, tc := range []struct {
		name string
		tls  bool
	}{{"plain", false}, {"tls", true}} {
		t.Run(tc.name, func(t *testing.T) {
			n, stalled := newBlackHoleNATS(t, time.Second, tc.tls)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			start := time.Now()
			err := n.Write(ctx, []*envelope.Item{sizedItem(8 << 20)})
			took := time.Since(start)
			if err == nil {
				t.Fatal("a write to a peer that reads nothing succeeded")
			}
			if c := ClassOf(err); c != ClassNetwork {
				t.Errorf("class %v (%v), want network", c, err)
			}
			if took > 4*time.Second {
				t.Fatalf("Write with sink.op_timeout = 1s returned after %s: %v", took.Round(time.Millisecond), err)
			}
			if stalled.Load() == 0 {
				t.Fatalf("the write never reached the black hole (%v): the client did not connect", err)
			}
		})
	}
}

// SPEC §8.7.1, §10.3: the first missed deadline closes the socket, so the
// other workers fail at once instead of each waiting its own deadline on the
// same full socket, and the stopped workers return within about one
// sink.op_timeout. Before 2026-11-23 two workers with batches of 2 MiB took
// 2m58s to return after the cancel (go-webhook-nats-server); with the deadline
// alone and no close, the eight workers here took one deadline each, in a row.
func TestNATSStalledSocketReleasesEveryWorkerWithinOneOpTimeout(t *testing.T) {
	n, stalled := newBlackHoleNATS(t, time.Second, false)
	r := ring.NewMemory(100, 1<<30, 0)
	for i := 0; i < 16; i++ {
		if err := r.Enqueue(context.Background(), sizedItem(2<<20)); err != nil {
			t.Fatal(err)
		}
	}
	h := NewHealth(&scriptedSink{}, HealthConfig{}, quietLog(), testMetrics())
	w := NewWorkers(r, n, h, WorkerConfig{Workers: 8, Batch: 2, OpTimeout: time.Second,
		BackoffMin: 50 * time.Millisecond, BackoffMax: 100 * time.Millisecond}, quietLog(), testMetrics())
	ctx, stop := context.WithCancel(context.Background())
	w.Start(ctx)
	time.Sleep(2500 * time.Millisecond) // every worker is retrying against the black hole
	if stalled.Load() == 0 {
		t.Fatal("no publish reached the black hole: the client did not connect")
	}

	start := time.Now()
	stop()
	if !w.WaitFor(5 * time.Second) {
		t.Fatalf("the workers did not return within 5s of the cancel")
	}
	took := time.Since(start)
	t.Logf("the workers returned %s after the cancel", took.Round(time.Millisecond))
	if took > 2*time.Second {
		t.Errorf("the workers took %s to return after the cancel, want about one sink.op_timeout (1s)",
			took.Round(time.Millisecond))
	}
	if got := len(w.Pending()) + len(r.Drain()); got != 16 {
		t.Errorf("held + queued = %d, want the 16 items", got)
	}
}

// SPEC §8.7.7: Health is not held behind a publish stalled on the
// connection's lock for longer than about one sink.op_timeout. Before
// 2026-11-23 it returned after about a minute (go-webhook-nats-server: 59.9s).
func TestNATSHealthIsNotHeldBehindAStalledPublish(t *testing.T) {
	n, stalled := newBlackHoleNATS(t, time.Second, false)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = n.Write(ctx, []*envelope.Item{sizedItem(8 << 20)})
	}()
	time.Sleep(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	err := n.Health(ctx)
	if took := time.Since(start); took > 4*time.Second {
		t.Fatalf("Health with a 1s deadline returned after %s: %v", took.Round(time.Millisecond), err)
	}
	if err == nil {
		t.Error("Health passed against a peer that reads nothing")
	}
	if stalled.Load() == 0 {
		t.Error("the publish never reached the black hole: the client did not connect")
	}
}

// The socket itself: the first write that misses its deadline closes it and
// reports the stall once; the next write fails at once instead of waiting.
func TestStallConnClosesAtTheFirstMissedWriteDeadline(t *testing.T) {
	client, server := net.Pipe() // a pipe nobody reads: every write blocks
	defer server.Close()
	var stalls atomic.Int32
	d := newStallDialer(time.Second, func(string) { stalls.Add(1) })
	c := &stallConn{Conn: client, dialer: d}

	_ = c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := c.Write([]byte("PUB x 1\r\nx\r\n")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("first write: %v, want the deadline", err)
	}
	_ = c.SetWriteDeadline(time.Time{})
	start := time.Now()
	if _, err := c.Write([]byte("PING\r\n")); err == nil {
		t.Fatal("a write after the stall succeeded: the socket was not closed")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("the write after the stall took %s, want an immediate failure", took)
	}
	if got := stalls.Load(); got != 1 {
		t.Errorf("onStall ran %d times, want 1", got)
	}
}

// SPEC §8.7.1: the write deadline is sink.op_timeout, not the library's
// minute, and the socket is the sink's own, closed at the first missed
// deadline -- with sink.nats.tls too, where the client's TLS wraps that socket
// (SPEC §8.7.9). If someone "simplifies" either option, only this test and
// the black-hole tests above fail (review of 2026-11-21, item 001).
func TestNATSOptionsBoundTheSocketWrite(t *testing.T) {
	for _, mut := range []func(*NATSConfig){
		nil,
		func(c *NATSConfig) { c.TLS = true },
	} {
		n := newTestNATS(t, mut)
		o := nats.GetDefaultOptions()
		for _, opt := range n.opts {
			if err := opt(&o); err != nil {
				t.Fatal(err)
			}
		}
		if o.FlusherTimeout != n.cfg.OpTimeout {
			t.Errorf("tls=%v: FlusherTimeout %s, want sink.op_timeout %s", n.cfg.TLS, o.FlusherTimeout,
				n.cfg.OpTimeout)
		}
		if _, ok := o.CustomDialer.(*stallDialer); !ok {
			t.Errorf("tls=%v: CustomDialer is %T, want the *stallDialer that closes a stalled socket",
				n.cfg.TLS, o.CustomDialer)
		}
		if o.Secure != n.cfg.TLS {
			t.Errorf("Secure %v with sink.nats.tls %v: the dialer must not replace the TLS", o.Secure, n.cfg.TLS)
		}
	}
}
