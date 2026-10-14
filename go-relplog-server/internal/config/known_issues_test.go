package config

import (
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
)

// Regression tests from the port of the go-relplog-pg-server and
// go-relplog-nats-server known-issues round (2026-10-14, SPEC §22.1).

// A ring.max_bytes below what one message of syslog.max_message_bytes costs
// the EFFECTIVE ring (SPEC §9.2) is refused: that message could never be
// queued, its transaction would be held until relp.backpressure_timeout, and
// librelp would retransmit it on every reconnect, the emitter's session stuck
// behind it for ever (SPEC §13.2, §15.1).
func TestRingMaxBytesFitsTheLargestMessage(t *testing.T) {
	// memory: the default 64 KiB message holds the 64 KiB class + 384 = 65920.
	mustFail(t, []string{"--ring.max_bytes=65919"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--ring.max_bytes=65920"}, nil)
	// The smallest class: a 2 KiB message costs 2048 + 384.
	mustFail(t, []string{"--syslog.max_message_bytes=2KiB", "--ring.max_bytes=2431"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=2KiB", "--ring.max_bytes=2432"}, nil)
	// Between classes the next class is charged: 3000 bytes hold an 8 KiB buffer.
	mustFail(t, []string{"--syslog.max_message_bytes=3000", "--ring.max_bytes=8575"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=3000", "--ring.max_bytes=8576"}, nil)
	// Above the largest class the buffer is the message itself.
	mustFail(t, []string{"--syslog.max_message_bytes=100000", "--ring.max_bytes=100383"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=100000", "--ring.max_bytes=100384"}, nil)

	// sqlite and redis count len(raw): the floor is the message itself.
	for _, mode := range []string{"sqlite", "redis"} {
		mustFail(t, []string{"--ring.mode=" + mode, "--ring.max_bytes=65535"}, nil, "ring.max_bytes")
		mustLoad(t, []string{"--ring.mode=" + mode, "--ring.max_bytes=65536"}, nil)
	}
	// ring=redis on the sink's own Redis degrades to memory, and the memory
	// floor applies to what will run.
	mustFail(t, []string{"--ring.mode=redis", "--sink.type=redis", "--ring.max_bytes=65536"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--ring.mode=redis", "--sink.type=redis", "--ring.max_bytes=65920"}, nil)
	// ring=none does not use the key: positive is enough.
	mustLoad(t, []string{"--ring.mode=none", "--ring.max_bytes=1"}, nil)
	mustFail(t, []string{"--ring.mode=none", "--ring.max_bytes=0"}, nil, "ring.max_bytes")
	// Control: the defaults pass (512 MiB against 65920).
	mustLoad(t, nil, nil)
}

// The memory floor is what the ring itself charges: an empty ring of
// ringMinBytes(n) takes an n-byte message (FullFor, the receiver's pre-check,
// says so), and one byte less does not. Pins the validation to ring.Memory's
// accounting, so a change of the pool classes or of ItemOverhead moves both or
// fails here.
func TestRingMinBytesIsWhatTheRingCharges(t *testing.T) {
	for _, n := range []int64{480, 2048, 2049, 3000, 8192, 8193, 65536, 65537, 100000, MaxMessageBytes} {
		floor := ringMinBytes(n)
		if ring.NewMemory(10, floor, 0).FullFor(int(n)) {
			t.Errorf("n=%d: an empty ring of %d bytes refuses it", n, floor)
		}
		if !ring.NewMemory(10, floor-1, 0).FullFor(int(n)) {
			t.Errorf("n=%d: an empty ring of %d bytes takes it; the floor is too high", n, floor-1)
		}
	}
}

// admin.bind and listen.tcp or listen.tls on the same TCP address are refused
// at validation (SPEC §13.2, §14), not at boot step 12 after the admin port
// served the whole boot -- or, on macOS/BSD where SO_REUSEADDR lets
// 127.0.0.1:P and *:P coexist, never: the admin port silently took
// localhost's RELP connections. The rule of sameStreamSocket: no name is
// resolved, a host name on either side with the same port is a collision, and
// two distinct IP literals share a port.
func TestAdminBindCollidesWithListen(t *testing.T) {
	for _, args := range [][]string{
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.1:20514"},
		{"--listen.tcp=:9103"},                                                    // wildcard against the default 127.0.0.1:9103
		{"--listen.tcp=0.0.0.0:9103"},                                             // the same wildcard, spelled out
		{"--listen.tcp=[::]:9103"},                                                // and the IPv6 one
		{"--admin.bind=localhost:20514"},                                          // a name against the default :20514
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=localhost:20514"},          // a name may resolve to the literal
		{"--listen.tcp=[::ffff:127.0.0.1]:20514", "--admin.bind=127.0.0.1:20514"}, // the mapped form is the same address
	} {
		mustFail(t, args, nil, "admin.bind")
	}
	// listen.tls too: the same rules against the other stream listener.
	tlsArgs := func(listen string, extra ...string) []string {
		return append([]string{"--listen.tcp=", "--listen.tls=" + listen}, extra...)
	}
	for _, args := range [][]string{
		tlsArgs("127.0.0.1:6514", "--admin.bind=127.0.0.1:6514"),
		tlsArgs(":9103"),
		tlsArgs("127.0.0.1:6514", "--admin.bind=localhost:6514"),
	} {
		mustFail(t, args, nil, "admin.bind")
	}
	// Controls: two distinct literals share a port; admin off; other ports;
	// port 0 asks the kernel and never collides; an empty listen.tls is not
	// compared (the default :20514 of listen.tcp against 127.0.0.1:9103).
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.2:20514"}, nil)
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind=:20514"}, nil)
	mustLoad(t, []string{"--listen.tcp=:20514", "--admin.bind=127.0.0.1:20515"}, nil)
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:0", "--admin.bind=127.0.0.1:0"}, nil)
	mustLoad(t, nil, nil)
	// A valid TLS listener on another port, so the refusals above are the
	// collision and not the missing certificate (CheckFiles reads the files).
	mustLoad(t, tlsArgs("127.0.0.1:6514", "--tls.cert_file=c.pem", "--tls.key_file=k.pem"), nil)
	mustFail(t, tlsArgs("127.0.0.1:9103", "--tls.cert_file=c.pem", "--tls.key_file=k.pem"), nil, "admin.bind")
}

// An idle timeout below relp.backpressure_timeout is a WARN, never an error
// (SPEC §13.2): a held transaction then closes its session on the idle, before
// the backpressure timeout -- counted backpressure (SPEC §5.4), retransmitted
// all the same. Each transport has its own idle: tcp.idle_timeout with
// listen.tcp, tls.idle_timeout with listen.tls.
func TestIdleBelowBackpressureWarns(t *testing.T) {
	const want = "tcp.idle_timeout (10s) is below relp.backpressure_timeout (30s)"
	if l := mustLoad(t, []string{"--tcp.idle_timeout=10s"}, nil); !hasWarning(l, want) {
		t.Fatalf("no warning for idle 10s < backpressure 30s: %q", l.Warnings)
	}
	tlsOn := []string{"--listen.tls=127.0.0.1:6514", "--tls.cert_file=c.pem", "--tls.key_file=k.pem"}
	const wantTLS = "tls.idle_timeout (10s) is below relp.backpressure_timeout (30s)"
	if l := mustLoad(t, append(tlsOn, "--tls.idle_timeout=10s"), nil); !hasWarning(l, wantTLS) {
		t.Fatalf("no warning for the TLS idle 10s < backpressure 30s: %q", l.Warnings)
	}
	// Controls: the defaults (120s, 30s), equal values, an idle of 0 (never),
	// and the idle of a transport that is not listening do not warn.
	for _, args := range [][]string{
		nil, {"--tcp.idle_timeout=30s"}, {"--tcp.idle_timeout=0s"},
		{"--tls.idle_timeout=10s"},
		append(append([]string{}, tlsOn...), "--listen.tcp=", "--tcp.idle_timeout=10s"),
		append(append([]string{}, tlsOn...), "--tls.idle_timeout=0s"),
	} {
		if l := mustLoad(t, args, nil); hasWarning(l, "idle_timeout") {
			t.Fatalf("%v: unexpected warning %q", args, l.Warnings)
		}
	}
}

// A key DERIVED in the boot snapshot and in the fresh one (SPEC §16.2, a
// "derived" note) is not reported by a SIGHUP: it changes only because the key
// it derives from changed, and that one is reported (SPEC §16.3). A key that
// goes from explicit to derived, or the other way round, is.
func TestReloadDoesNotReportDerivedKeys(t *testing.T) {
	d1, d2 := t.TempDir(), t.TempDir()
	boot := mustLoad(t, []string{"--server.data_dir=" + d1}, nil)
	fresh := mustLoad(t, []string{"--server.data_dir=" + d2}, nil)
	if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "server.data_dir" {
		t.Fatalf("data_dir changed: ignored = %q, want only server.data_dir "+
			"(ring.sqlite.path, sink.sqlite.path and emergency.dir are derived from it)", ignored)
	}
	// Control: explicit at the boot, derived now -- reported.
	explicit := mustLoad(t, []string{"--server.data_dir=" + d1, "--emergency.dir=" + d1 + "/dumps"}, nil)
	derived := mustLoad(t, []string{"--server.data_dir=" + d1}, nil)
	if _, ignored := Compare(explicit, nil, derived); len(ignored) != 1 || ignored[0] != "emergency.dir" {
		t.Fatalf("emergency.dir explicit -> derived: ignored = %q, want emergency.dir", ignored)
	}
	// And the other way round.
	if _, ignored := Compare(derived, nil, explicit); len(ignored) != 1 || ignored[0] != "emergency.dir" {
		t.Fatalf("emergency.dir derived -> explicit: ignored = %q, want emergency.dir", ignored)
	}
}

// The pool of the SQL and ClickHouse sinks is derived from sink.workers
// (SPEC §10.5): changing sink.workers reports sink.workers alone. A pool_size
// set on one side is a real change and is reported.
func TestReloadDoesNotReportTheDerivedPoolSize(t *testing.T) {
	for _, typ := range []string{"postgres", "mysql", "clickhouse"} {
		pool := "sink." + typ + ".pool_size"
		boot := mustLoad(t, []string{"--sink.type=" + typ, "--sink.workers=4"}, nil)
		fresh := mustLoad(t, []string{"--sink.type=" + typ, "--sink.workers=8"}, nil)
		if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "sink.workers" {
			t.Fatalf("%s: workers 4 -> 8: ignored = %q, want only sink.workers (%s is derived)", typ, ignored, pool)
		}
		explicit := mustLoad(t, []string{"--sink.type=" + typ, "--sink.workers=8", "--" + pool + "=9"}, nil)
		if _, ignored := Compare(fresh, nil, explicit); len(ignored) != 1 || ignored[0] != pool {
			t.Fatalf("%s: derived -> explicit pool: ignored = %q, want %s", typ, ignored, pool)
		}
	}
}

// sameStreamSocket reads the values a bind means, not their spellings
// (independent review of 2026-10-14, syslog D1 / RELP D4; SPEC §13.2): any
// unspecified literal is the wildcard, and the port is the number net.Listen
// would bind -- leading zeros and service names included. A port that is
// neither is refused here rather than at bind.
func TestSameStreamSocketReadsValuesNotSpellings(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"[::0]:9102", "127.0.0.1:9102", true},
		{"[0:0::0]:9102", "127.0.0.1:9102", true},
		{"[::ffff:0.0.0.0]:9102", "127.0.0.1:9102", true},
		{"127.0.0.1:0514", "127.0.0.1:514", true},
		{"0.0.0.0:0514", "127.0.0.1:514", true},
		{"127.0.0.1:http", "127.0.0.1:80", true},
		{"127.0.0.1:no-such-service-xyz", "127.0.0.1:80", true},
		// Controls: the rule did not become "always".
		{"127.0.0.1:0514", "127.0.0.1:515", false},
		{"127.0.0.1:514", "127.0.0.2:0514", false},
		{"[::0]:0", "127.0.0.1:0", false},
		{"127.0.0.1:00", "127.0.0.1:0", false},
		{"[::1]:9102", "127.0.0.1:9102", false},
	} {
		if got := sameStreamSocket(c.a, c.b); got != c.want {
			t.Errorf("sameStreamSocket(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
