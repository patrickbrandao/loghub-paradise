package sink

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/rediscli"
)

// Nothing in this file needs a real server. The integration half lives in
// redis_integration_test.go and skips without SYSLOG_TEST_REDIS_ADDR.

// The sink maps the table shared with ring=redis onto its own classes, and a
// fatal or logic refusal names the envelope.
func TestClassifyRedisMapping(t *testing.T) {
	if got := ClassOf(classifyRedis("xadd", context.DeadlineExceeded, "u")); got != ClassNetwork {
		t.Errorf("deadline = %s", got)
	}
	if got := ClassOf(classifyRedis("xadd", errors.New("broken pipe"), "u")); got != ClassNetwork {
		t.Errorf("broken pipe = %s", got)
	}
	if classifyRedis("x", nil, "") != nil {
		t.Error("nil classified")
	}
	// rueidis.RedisError cannot be built from outside its package; the reply
	// table itself is tested in rediscli, and the real replies below.
	if rediscli.ClassifyReply("OOM command not allowed") != rediscli.Fatal {
		t.Fatal("OOM must be fatal")
	}
}

func TestNewRedisConfig(t *testing.T) {
	if _, err := NewRedis(RedisConfig{}, discard(), metrics()); err == nil {
		t.Error("no address accepted")
	}
	if _, err := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{"x:1"}}}, discard(), metrics()); err == nil {
		t.Error("{space} without a workspace accepted")
	}
	r, err := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{"x:1"}}, Workspace: "default"}, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	// SPEC §3.6: the default stream is the NATS subject's name, {space}
	// expanded per envelope.
	if got := r.streamFor("acme.prod"); got != "loghub.acme.prod.syslog" {
		t.Errorf("stream = %q", got)
	}
	if got := r.streamFor("default"); got != "loghub.default.syslog" {
		t.Errorf("stream = %q", got)
	}
	lit, _ := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{"x:1"}}, Stream: "events"}, discard(), metrics())
	if got := lit.streamFor("acme"); got != "events" {
		t.Errorf("a literal stream name was expanded: %q", got)
	}
	if _, err := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{"x:1"}, TLS: true, TLSCA: "/nonexistent"},
		Workspace: "d"}, discard(), metrics()); err == nil {
		t.Error("a missing CA file accepted")
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

// SPEC §10.3: a refusal answered during the DIAL (rueidis sends HELLO/AUTH
// inside NewClient) is classified like any other reply -- fatal at once -- and
// not retried as a connection failure until sink.boot_timeout runs out. A port
// that refuses the connection stays a network failure, which the boot loop
// keeps retrying.
func TestRedisClassifiesTheDialReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("WRONGPASS at the handshake is fatal", func(t *testing.T) {
		addr := fakeRedisRefusing(t, "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
		r, err := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{addr}}, Workspace: "default"},
			discard(), metrics())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		err = r.Health(ctx)
		if err == nil {
			t.Fatal("Health succeeded against a server that refuses every command")
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
		r, err := NewRedis(RedisConfig{Config: rediscli.Config{Addrs: []string{dead}}, Workspace: "default"},
			discard(), metrics())
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		err = r.Health(ctx)
		if err == nil {
			t.Fatal("Health succeeded against a closed port")
		}
		if IsFatal(err) {
			t.Fatalf("err = %v classified fatal; a closed port is retried, not fatal", err)
		}
		if err := r.Write(ctx, batchN(t, 1)); err == nil || IsFatal(err) {
			t.Fatalf("Write against a closed port = %v, want network", err)
		}
	})
}
