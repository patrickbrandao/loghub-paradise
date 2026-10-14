package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/sink"
)

// downSink answers every health check as unreachable: a destination whose
// client was built but whose first check never passes.
type downSink struct{ sink.Sink }

func (downSink) Health(context.Context) error {
	return &sink.Error{Class: sink.ClassNetwork, Op: "test", Err: errors.New("never ready")}
}

// SPEC §10.1 step 3 (W pendência 22): ONE sink.boot_timeout, counted from the
// start of step 3, covers building the Redis client and waiting for the first
// health check. Redis appears at 1.5s of a 2s budget and its check never
// passes: the boot must end around 2s, not 1.5s + a whole second 2s.
func TestRedisBootHasOneDeadline(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	proxy.Pause()
	// Resumed well before the attempt at 1.5s (0, .1, .3, .7, 1.5s): at 1.5s
	// exactly the attempt raced the resume and sometimes found it paused.
	go func() {
		time.Sleep(1200 * time.Millisecond)
		proxy.Resume()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var buf syncBuffer
	args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory",
		"--sink.boot_timeout=2s", "--sink.health_timeout=200ms"},
		redisArgs(proxy.Addr(), stream)...)
	start := time.Now()
	code := runContext(ctx, args, testID, func(a *App) {
		a.log = obs.NewLogger(&buf, "text", "info")
		a.wrapSink = func(s sink.Sink) sink.Sink { return downSink{s} }
	})
	took := time.Since(start)
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if !strings.Contains(buf.String(), `msg="sink ready" type=redis`) {
		t.Fatalf("control: the Redis client was never built, so the wait for the check never ran:\n%s", buf.String())
	}
	if took > 3*time.Second {
		t.Fatalf("the boot took %s with sink.boot_timeout=2s: the wait for the first check started a "+
			"second deadline after the client was built", took)
	}
}

// The client loop alone also keeps to the budget: the backoff does not sleep
// past it. A Redis that never appears, 2.5s: attempts at 0, .1, .3, .7, 1.5s,
// then a 1.6s backoff that used to end at 3.1s.
func TestRedisBootClientLoopKeepsTheDeadline(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)
	proxy := newTCPProxy(t, os.Getenv(redisAddrEnv))
	proxy.Pause()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.boot_timeout=2500ms"},
		redisArgs(proxy.Addr(), stream)...)
	start := time.Now()
	code := runContext(ctx, args, testID, nil)
	took := time.Since(start)
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if took < 2400*time.Millisecond || took > 2900*time.Millisecond {
		t.Fatalf("the boot took %s with sink.boot_timeout=2.5s", took)
	}
}

// SPEC §10.1 step 3 (W §19.1 item 38): the attempt keeps to the budget inside
// the client too. rueidis.NewClient takes no context and dials and handshakes
// under its own Dialer.Timeout (5s), so an address that accepts TCP and never
// speaks -- a balancer without a backend, a Redis stuck -- held the first
// attempt 5s with sink.boot_timeout=1s. No Redis needed: the listener is the
// silent server.
func TestRedisBootDeadlineHoldsAgainstASilentServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c) // accepted, never answered
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.boot_timeout=1s"},
		redisArgs(ln.Addr().String(), "whs-silent")...)
	start := time.Now()
	code := runContext(ctx, args, testID, nil)
	took := time.Since(start)
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if took > 2*time.Second {
		t.Fatalf("the boot took %s with sink.boot_timeout=1s against a server that never speaks", took)
	}
}

// SPEC §8.2.5 (W pendência 12): a Redis under allkeys-* ends the boot at once,
// exit 3, with the line every fatal boot refusal has.
func TestRedisEvictingServerEndsTheBoot(t *testing.T) {
	addr := os.Getenv("WHS_TEST_REDIS_LRU_ADDR")
	if addr == "" {
		t.Skip("set WHS_TEST_REDIS_LRU_ADDR to a Redis with --maxmemory-policy allkeys-lru (docs/TESTING.md §5)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var buf syncBuffer
	args := append([]string{"--server.bind=" + freePort(t), "--admin.bind=" + freePort(t),
		"--ring.data_dir=" + t.TempDir(), "--ring.mode=memory", "--sink.boot_timeout=120s"},
		redisArgs(addr, fmt.Sprintf("whs-test-lru-%d", time.Now().UnixNano()))...)
	start := time.Now()
	code := runContext(ctx, args, testID, func(a *App) { a.log = obs.NewLogger(&buf, "text", "info") })
	if code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("a fatal refusal waited %s", took)
	}
	if out := buf.String(); !strings.Contains(out, "the sink refused this server permanently") ||
		!strings.Contains(out, "allkeys-lru") {
		t.Errorf("the boot log does not name the eviction policy as a permanent refusal:\n%s", out)
	}
}
