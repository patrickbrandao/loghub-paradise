package sink

// Tests that pin behaviours the rest of the suite left unproved: a fatal reply
// to the write probe or to the dial is fatal at once (SPEC §10.3), a probe that
// outlives sink.health_timeout counts as kind="timeout" (SPEC §13.1), and the
// sqlite heartbeat detects a file that is no longer writable (SPEC §10.4).

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// blockingSink holds the probe until its context expires.
type blockingSink struct{}

func (blockingSink) Write(context.Context, []Prepared) error { return nil }
func (blockingSink) Health(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
func (blockingSink) Close() error { return nil }
func (blockingSink) Type() string { return "blocking" }

// SPEC §10.3: a permanent refusal answered to the PROBE -- READONLY from a
// promoted replica, WRONGTYPE on the stream key -- is FATAL at the first
// check. The fuses exist for a destination that is unreachable, not for one
// that answered "never".
func TestHealthFatalReplyIsFatalAtOnce(t *testing.T) {
	fs := newFakeSink()
	fs.healthy.Store(false)
	fs.healthErr = fatalErr("writeprobe", errors.New("READONLY You can't write against a read only replica."))
	m := metrics()
	h := NewHealth(fs, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: time.Second,
		MaxRetries: 1000, FailAfter: time.Hour, // neither fuse can be what trips
	}, discard(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	select {
	case <-h.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("a fatal reply to the probe did not end the process at once")
	}
	if !IsFatal(h.FatalErr()) {
		t.Fatalf("FatalErr = %v, want the probe's fatal class retained", h.FatalErr())
	}
	if got := m.SinkErrorsTotal[obs.SinkErrFatal].Load(); got != 1 {
		t.Errorf("kind=fatal counted %d, want exactly 1: one probe, one verdict", got)
	}
	if m.SinkUp.Load() != 0 {
		t.Error("syslog_sink_up still 1 after a fatal probe")
	}
}

// SPEC §13.1: a probe that outlives sink.health_timeout is a "slow" incident,
// counted as kind="timeout" like a slow write, not as the network refusing.
func TestHealthProbeTimeoutIsCountedApart(t *testing.T) {
	m := metrics()
	h := NewHealth(blockingSink{}, HealthConfig{
		Interval: 5 * time.Millisecond, Timeout: 10 * time.Millisecond,
		MaxRetries: 1000, FailAfter: time.Hour,
	}, discard(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Run(ctx)

	waitFor(t, 3*time.Second, func() bool { return m.SinkErrorsTotal[obs.SinkErrTimeout].Load() > 0 })
	if got := m.SinkErrorsTotal[obs.SinkErrNetwork].Load(); got != 0 {
		t.Errorf("kind=network counted %d for probes that only timed out", got)
	}
}

// fakeRedisRefusing listens like a Redis server whose only answer is the given
// error reply, once per command received. rueidis authenticates inside the
// dial, so this is what a wrong password looks like from the client's side.
func fakeRedisRefusing(t *testing.T, reply string) string {
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
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					for i := bytes.Count(buf[:n], []byte("*")); i > 0; i-- {
						if _, err := c.Write([]byte(reply)); err != nil {
							return
						}
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

// SPEC §10.3, the note under the reply table: a refusal answered during the DIAL
// (rueidis sends HELLO/AUTH inside NewClient) is classified like any other
// reply -- fatal at once -- and not retried as a connection failure until
// sink.boot_timeout runs out. A port that refuses the connection stays a
// network failure, which the boot loop keeps retrying.
func TestNewRedisClassifiesTheDialReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("WRONGPASS at the handshake is fatal", func(t *testing.T) {
		addr := fakeRedisRefusing(t, "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
		_, err := NewRedis(ctx, RedisConfig{Addrs: []string{addr}, Stream: "s"}, discard(), metrics())
		if err == nil {
			t.Fatal("NewRedis succeeded against a server that refuses every command")
		}
		if !IsFatal(err) {
			t.Fatalf("err = %v (class %s), want fatal: waiting cannot fix a bad password", err, ClassOf(err))
		}
	})

	t.Run("a refused connection is a network failure", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		dead := ln.Addr().String()
		ln.Close()
		_, err = NewRedis(ctx, RedisConfig{Addrs: []string{dead}, Stream: "s"}, discard(), metrics())
		if err == nil {
			t.Fatal("NewRedis succeeded against a closed port")
		}
		if IsFatal(err) {
			t.Fatalf("err = %v classified fatal; a closed port is retried, not fatal", err)
		}
	})
}

// SPEC §10.4: the heartbeat proves the file is still WRITABLE. A file that
// turned read-only underneath the process -- a remount, a permission change --
// fails the probe with the fatal class, because waiting cannot heal it.
func TestSQLiteHeartbeatDetectsAReadOnlyFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Health(context.Background()); err != nil {
		t.Fatalf("Health on a fresh file: %v", err)
	}
	s.Close()

	sidecars := []string{path, path + "-wal", path + "-shm"}
	for _, p := range sidecars {
		if _, err := os.Stat(p); err == nil {
			if err := os.Chmod(p, 0o444); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() {
		for _, p := range sidecars {
			_ = os.Chmod(p, 0o600)
		}
	})

	s, err = NewSQLite(SQLiteConfig{Path: path, Synchronous: "OFF"}, discard(), metrics())
	if err != nil {
		t.Fatalf("reopening a read-only events.db: %v -- the heartbeat, not the open, is what must notice", err)
	}
	defer s.Close()

	err = s.Health(context.Background())
	if err == nil {
		t.Fatal("Health passed on a read-only file: the probe does not prove writability")
	}
	if !IsFatal(err) {
		t.Fatalf("Health = %v (class %s), want fatal: a read-only file does not heal by waiting", err, ClassOf(err))
	}
}
