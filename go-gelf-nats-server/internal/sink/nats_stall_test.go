package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/ring"
)

// blackHoleNATS completes the NATS handshake (INFO, CONNECT, PING/PONG) and
// then, at the first PUB/HPUB of each connection, stops reading that socket
// for good, without closing it: what a frozen host, a paused container or a
// partition that drops packets looks like to the client (SPEC §10.7, review
// of 2026-11-21, item 001). Every connection is accepted, so a reconnection
// lands on another black hole.
func blackHoleNATS(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
				defer c.Close()
				info := `{"server_id":"BH","server_name":"bh","version":"2.14.6","proto":1,"host":"127.0.0.1",` +
					`"port":4222,"headers":true,"max_payload":67108864}`
				if _, err := c.Write([]byte("INFO " + info + "\r\n")); err != nil {
					return
				}
				rd := bufio.NewReader(c)
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
						_, _ = c.Write([]byte("PONG\r\n"))
					case "PUB", "HPUB":
						<-done // never read this socket again
						return
					}
				}
			}(c)
		}
	}()
	return "nats://" + ln.Addr().String()
}

func newBlackHoleNATS(t *testing.T, opTimeout time.Duration) *NATS {
	t.Helper()
	return newTestNATS(t, func(c *NATSConfig) {
		c.URLs = []string{blackHoleNATS(t)}
		c.ConnectTimeout = 2 * time.Second
		c.OpTimeout = opTimeout
	})
}

// bigLines are n GELF documents of about 48 KiB each: every publish passes the
// 32 KiB past which nats.go flushes inside the call.
func bigLines(n int) []string {
	pad := strings.Repeat("x", 48<<10)
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"version":"1.1","host":"web-01","short_message":"big-%03d %s","level":5}`, i, pad)
	}
	return out
}

// A Write against a peer that stopped reading returns within sink.op_timeout,
// as network. Before 2026-11-23 it waited out nats.go's one-minute
// FlusherTimeout.
func TestNATSWriteHonoursOpTimeoutAgainstAPeerThatStopsReading(t *testing.T) {
	n := newBlackHoleNATS(t, time.Second)
	b := batchOf(t, bigLines(160)...) // ~7.5 MiB
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	err := n.Write(ctx, b)
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
}

// The first missed deadline closes the socket, so the other workers fail at
// once instead of each waiting its own deadline on the same full socket, and
// the stopped workers return within about one sink.op_timeout.
func TestNATSStalledSocketReleasesEveryWorkerWithinOneOpTimeout(t *testing.T) {
	n := newBlackHoleNATS(t, time.Second)
	r := ring.NewMemory(1000, 1<<30, 0)
	items := itemsOf(bigLines(320)...)
	enqueueAll(t, r, items)
	m := metrics()
	h := NewHealth(n, HealthConfig{}, discard(), m)
	cfg := workerCfg(20)
	cfg.Workers, cfg.OpTimeout = 8, time.Second
	cfg.BackoffMin, cfg.BackoffMax = 50*time.Millisecond, 100*time.Millisecond
	cfg.Convert = convert.DefaultConfig()
	w := NewWorkers(r, n, h, cfg, discard(), m)
	ctx, stop := context.WithCancel(context.Background())
	w.Start(ctx)
	time.Sleep(2500 * time.Millisecond) // every worker is retrying against the black hole

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
	if got := len(w.Pending()) + len(r.Drain()); got != len(items) {
		t.Errorf("held + queued = %d, want the %d items", got, len(items))
	}
}

// Health is not held behind a publish stalled on the connection's lock for
// longer than about one sink.op_timeout. Before 2026-11-23 it waited a minute.
func TestNATSHealthIsNotHeldBehindAStalledPublish(t *testing.T) {
	n := newBlackHoleNATS(t, time.Second)
	b := batchOf(t, bigLines(160)...)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = n.Write(ctx, b)
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

// The two options of item 001 are set: if someone "simplifies" one, only this
// and the black-hole tests above notice.
func TestNATSOptionsBoundTheSocketWrite(t *testing.T) {
	n := newTestNATS(t, nil)
	o := nats.GetDefaultOptions()
	for _, opt := range n.opts {
		if err := opt(&o); err != nil {
			t.Fatal(err)
		}
	}
	if o.FlusherTimeout != n.cfg.OpTimeout {
		t.Errorf("FlusherTimeout %s, want sink.op_timeout %s", o.FlusherTimeout, n.cfg.OpTimeout)
	}
	if _, ok := o.CustomDialer.(*stallDialer); !ok {
		t.Errorf("CustomDialer is %T, want the *stallDialer that closes a stalled socket", o.CustomDialer)
	}
}
