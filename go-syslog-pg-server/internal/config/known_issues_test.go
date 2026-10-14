package config

import (
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
)

// Regression tests from the port of the RP/RN known-issues round (2026-10-14,
// SPEC §22.1 / §21.1).

// A ring.max_bytes below what one message of syslog.max_message_bytes costs
// the ring (its pool class plus ItemOverhead, SPEC §9.2, §15.1) is refused:
// that message could never be queued, even in an empty ring (SPEC §13.2).
func TestRingMaxBytesFitsTheLargestMessage(t *testing.T) {
	// The default 64 KiB message: the 64 KiB class + 256 = 65792.
	mustFail(t, []string{"--ring.max_bytes=65791"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--ring.max_bytes=65792"}, nil)
	// The smallest class: a 2 KiB message costs 2048 + 256.
	mustFail(t, []string{"--syslog.max_message_bytes=2KiB", "--ring.max_bytes=2303"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=2KiB", "--ring.max_bytes=2304"}, nil)
	// Between classes the next class is charged: 3000 bytes hold an 8 KiB buffer.
	mustFail(t, []string{"--syslog.max_message_bytes=3000", "--ring.max_bytes=8447"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=3000", "--ring.max_bytes=8448"}, nil)
	// Above the largest class the buffer is the message itself.
	mustFail(t, []string{"--syslog.max_message_bytes=100000", "--ring.max_bytes=100255"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--syslog.max_message_bytes=100000", "--ring.max_bytes=100256"}, nil)
	// Control: the defaults pass (512 MiB against 65792).
	mustLoad(t, nil, nil)
}

// The floor is what the ring itself charges: an empty ring of ringMinBytes(n)
// takes an n-byte message (FullFor, the receiver's pre-check, says so), and
// one byte less does not. Pins the validation to ring.Memory's accounting, so
// a change of the pool classes or of ItemOverhead moves both or fails here.
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

// admin.bind and listen.tcp on the same TCP address are refused at validation
// (SPEC §13.2), not when the transport opens after the admin port served the
// whole boot -- or, on macOS/BSD where SO_REUSEADDR lets 127.0.0.1:P and *:P
// coexist, never: the admin port silently took localhost's TCP connections.
// The rule of sameStreamSocket in go-syslog-server: no name is resolved, a
// host name on either side with the same port is a collision, and two
// distinct IP literals share a port.
func TestAdminBindCollidesWithListen(t *testing.T) {
	for _, args := range [][]string{
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.1:20514"},
		{"--listen.tcp=:9102"},                                                    // wildcard against the default 127.0.0.1:9102
		{"--listen.tcp=0.0.0.0:9102"},                                             // the same wildcard, spelled out
		{"--listen.tcp=[::]:9102"},                                                // and the IPv6 one
		{"--admin.bind=localhost:1514"},                                           // a name against the default :1514
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=localhost:20514"},          // a name may resolve to the literal
		{"--listen.tcp=[::ffff:127.0.0.1]:20514", "--admin.bind=127.0.0.1:20514"}, // the mapped form is the same address
	} {
		mustFail(t, args, nil, "admin.bind")
	}
	// Controls: two distinct literals share a port; admin off; other ports;
	// port 0 asks the kernel and never collides; UDP is another protocol.
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.2:20514"}, nil)
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind=:1514"}, nil)
	mustLoad(t, []string{"--listen.tcp=:20514", "--admin.bind=127.0.0.1:20515"}, nil)
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:0", "--admin.bind=127.0.0.1:0"}, nil)
	mustLoad(t, []string{"--listen.udp=:9102", "--listen.tcp=:1514"}, nil)
	mustLoad(t, nil, nil)
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
		t.Fatalf("data_dir changed: ignored = %q, want only server.data_dir (emergency.dir is derived from it)", ignored)
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

// The same for sink.postgres.pool_size, derived from sink.workers (SPEC §10.5).
func TestReloadDoesNotReportTheDerivedPoolSize(t *testing.T) {
	boot := mustLoad(t, []string{"--sink.workers=4"}, nil)
	fresh := mustLoad(t, []string{"--sink.workers=7"}, nil)
	if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "sink.workers" {
		t.Fatalf("workers changed: ignored = %q, want only sink.workers (pool_size is derived from it)", ignored)
	}
	// Control: an explicit pool_size that changes is reported.
	boot = mustLoad(t, []string{"--sink.workers=4", "--sink.postgres.pool_size=10"}, nil)
	fresh = mustLoad(t, []string{"--sink.workers=4", "--sink.postgres.pool_size=12"}, nil)
	if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "sink.postgres.pool_size" {
		t.Fatalf("explicit pool_size changed: ignored = %q, want sink.postgres.pool_size", ignored)
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
