package app

// Tests written after the independent review of 2026-09-14: a reload proved by
// its effect (SPEC §13.3), and two Redis failures that the write probe and the
// dial classification must turn into a clean exit 3 (SPEC §8.2.4, §10.1).

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func containsKey(keys []string, want string) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

// SPEC §13.3, the effect and not just the accounting: a reloadable key applied
// by SIGHUP changes what the LIVE component does, in both directions. The
// limiter is the observable one -- rate 1, burst 1 turns the second of two
// back-to-back requests into a 429.
func TestReloadReachesTheLiveLimiter(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "whs.ini")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const off = "[ratelimit]\nenabled = false\nrate = 1\nburst = 1\n"
	const on = "[ratelimit]\nenabled = true\nrate = 1\nburst = 1\n"

	write(off)
	in := start(t, dir, "--ring.mode=memory", "--sink.type=none", "--config="+ini)
	twice := func() (int, int) {
		a, _ := in.post("/hook", "x")
		b, _ := in.post("/hook", "x")
		return a, b
	}

	if a, b := twice(); a != http.StatusOK || b != http.StatusOK {
		t.Fatalf("limiter off at boot: %d, %d", a, b)
	}

	write(on)
	if r := in.app.reload(); !containsKey(r.applied, "ratelimit.enabled") {
		t.Fatalf("enable reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if a, b := twice(); a != http.StatusOK || b != http.StatusTooManyRequests {
		t.Fatalf("limiter on after reload: %d, %d; want 200 then 429", a, b)
	}

	// Back to the boot value: a change against what is RUNNING, applied.
	write(off)
	if r := in.app.reload(); !containsKey(r.applied, "ratelimit.enabled") {
		t.Fatalf("disable reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	if a, b := twice(); a != http.StatusOK || b != http.StatusOK {
		t.Fatalf("limiter off again after reload: %d, %d", a, b)
	}
}

// SPEC §8.2.4 through the PROBE: a Redis demoted to a read-only replica still
// answers reads, so a PING would keep the sink UP while every XADD fails with
// READONLY and the ring fills to 429. The write probe notices, and the process
// exits 3 with its backlog dumped instead of looking healthy forever.
func TestRedisReadOnlyReplicaIsFatalViaTheWriteProbe(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)

	in := start(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory", "--shutdown.drain_timeout=3s")...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() {
		_ = c.Do(context.Background(), c.B().Arbitrary("REPLICAOF", "NO", "ONE").Build()).Error()
	})
	// A master that does not exist: the instance turns read-only and stays
	// so, serving reads while it retries the sync.
	if err := c.Do(ctx, c.B().Arbitrary("REPLICAOF", "127.0.0.1", "1").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if code := in.waitExit(30 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d: the write probe must turn READONLY into FATAL", code, ExitSink)
	}
}

// SPEC §8.2.4 at the DIAL: rueidis authenticates inside NewClient, and a wrong
// password is a permanent refusal. It must end the boot at once, not be retried
// as "Redis not reachable yet" for the whole of sink.boot_timeout.
func TestRedisWrongPasswordIsFatalAtBoot(t *testing.T) {
	c := redisClient(t)
	stream := redisStream(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Do(ctx, c.B().Arbitrary("ACL", "SETUSER", "whs-review", "on", ">right", "~*", "+@all").Build()).Error(); err != nil {
		t.Skipf("ACL SETUSER unavailable on this Redis: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Do(context.Background(), c.B().Arbitrary("ACL", "DELUSER", "whs-review").Build()).Error()
	})

	started := time.Now()
	in := startExpectingExit(t, t.TempDir(), redisArgs(os.Getenv(redisAddrEnv), stream,
		"--ring.mode=memory", "--sink.boot_timeout=20s",
		"--sink.redis.username=whs-review", "--sink.redis.password=wrong")...)

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Fatalf("exit code %d, want %d", code, ExitSink)
		}
		if took := time.Since(started); took > 5*time.Second {
			t.Fatalf("exit took %s; a bad password must not wait out sink.boot_timeout", took)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("still retrying a bad password after 15 s with boot_timeout at 20 s")
	}
}
