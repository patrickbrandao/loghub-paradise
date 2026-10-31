package config

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/ring"
)

// Regression tests from the port of the RP/RN known-issues round (2026-10-14,
// SPEC §22.1).

// A ring.max_bytes below what one message of convert.max_message_bytes costs
// the EFFECTIVE ring, in its own measure (SPEC §9.2, §15.1), is refused: that
// message could never be queued, even in an empty ring (SPEC §13.2).
func TestRingMaxBytesFitsTheLargestMessage(t *testing.T) {
	// memory: the default 128 KiB document is above the largest pool class
	// (64 KiB), so the buffer is the document itself: 131072 + 384 = 131456.
	mustFail(t, []string{"--ring.max_bytes=131455"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--ring.max_bytes=131456"}, nil)
	// The smallest class: a 1 KiB document costs the 2 KiB class + 384.
	mustFail(t, []string{"--convert.max_message_bytes=1KiB", "--ring.max_bytes=2431"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--convert.max_message_bytes=1KiB", "--ring.max_bytes=2432"}, nil)
	// Between classes the next class is charged: 3000 bytes hold an 8 KiB buffer.
	mustFail(t, []string{"--convert.max_message_bytes=3000", "--ring.max_bytes=8575"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--convert.max_message_bytes=3000", "--ring.max_bytes=8576"}, nil)
	// Above the largest class the buffer is the message itself.
	mustFail(t, []string{"--convert.max_message_bytes=100000", "--ring.max_bytes=100383"}, nil, "ring.max_bytes")
	mustLoad(t, []string{"--convert.max_message_bytes=100000", "--ring.max_bytes=100384"}, nil)
	// ring=redis degraded to memory (same server as sink=redis, SPEC §9.7)
	// takes the memory floor.
	mustFail(t, []string{"--ring.mode=redis", "--sink.type=redis", "--ring.max_bytes=131455"}, nil, "ring.max_bytes")
	// sqlite and redis count len(raw): the floor is the message itself.
	for _, mode := range []string{"--ring.mode=sqlite", "--ring.mode=redis"} {
		mustFail(t, []string{mode, "--ring.max_bytes=131071"}, nil, "ring.max_bytes")
		mustLoad(t, []string{mode, "--ring.max_bytes=131072"}, nil)
	}
	// ring=none does not use the key: positive is enough.
	mustLoad(t, []string{"--ring.mode=none", "--ring.max_bytes=1"}, nil)
	// Control: the defaults pass (512 MiB against 131456).
	mustLoad(t, nil, nil)
}

// The memory floor is what the ring itself charges: an empty ring of
// ringMinBytes(n) takes an n-byte message copied as the receiver copies it
// (item.CopyRaw, a buffer of its pool class), and one byte less does not.
// Pins the validation to ring.Memory's accounting, so a change of the pool
// classes or of ItemOverhead moves both or fails here.
func TestRingMinBytesIsWhatTheRingCharges(t *testing.T) {
	for _, n := range []int64{1024, 2048, 2049, 3000, 8192, 8193, 65536, 65537, 100000, MaxMessageBytes} {
		floor := ringMinBytes(n)
		buf, class := item.CopyRaw(make([]byte, n))
		if err := ring.NewMemory(10, floor, 0).Enqueue(context.Background(), item.New(item.Head{}, buf, class)); err != nil {
			t.Errorf("n=%d: an empty ring of %d bytes refuses it: %v", n, floor, err)
		}
		buf, class = item.CopyRaw(make([]byte, n))
		err := ring.NewMemory(10, floor-1, 0).Enqueue(context.Background(), item.New(item.Head{}, buf, class))
		if !errors.Is(err, ring.ErrFull) {
			t.Errorf("n=%d: an empty ring of %d bytes = %v, want ErrFull; the floor is too high", n, floor-1, err)
		}
	}
}

// admin.bind and a stream listener (listen.tcp, listen.tls, listen.http,
// listen.https) on the same TCP address are refused at validation
// (SPEC §13.2), not at boot step 12 after the admin port served the whole boot
// -- or, on macOS/BSD where SO_REUSEADDR lets 127.0.0.1:P and *:P coexist,
// never: the admin port silently took localhost's connections. The rule of
// sameStreamSocket: no name is resolved, a host name on either side with the
// same port is a collision, and two distinct IP literals share a port.
func TestAdminBindCollidesWithListen(t *testing.T) {
	tlsArgs := []string{"--tls.cert_file=/c", "--tls.key_file=/k"}
	for _, args := range [][]string{
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.1:20514"},
		{"--listen.tcp=:9104"},                                                    // wildcard against the default 127.0.0.1:9104
		{"--listen.tcp=0.0.0.0:9104"},                                             // the same wildcard, spelled out
		{"--listen.tcp=[::]:9104"},                                                // and the IPv6 one
		{"--admin.bind=localhost:12201"},                                          // a name against the default :12201
		{"--admin.bind=127.0.0.1:12202"},                                          // the default HTTP port
		{"--listen.http=:9104"},                                                   // HTTP against the default admin
		{"--listen.tcp=127.0.0.1:20514", "--admin.bind=localhost:20514"},          // a name may resolve to the literal
		{"--listen.tcp=[::ffff:127.0.0.1]:20514", "--admin.bind=127.0.0.1:20514"}, // the mapped form is the same address
		append([]string{"--listen.tls=127.0.0.1:12203", "--admin.bind=127.0.0.1:12203"}, tlsArgs...),
		append([]string{"--listen.tls=:9104"}, tlsArgs...),
		append([]string{"--listen.tls=127.0.0.1:12203", "--admin.bind=localhost:12203"}, tlsArgs...),
		append([]string{"--listen.https=:9104"}, tlsArgs...),
		append([]string{"--listen.https=127.0.0.1:12204", "--admin.bind=localhost:12204"}, tlsArgs...),
	} {
		err := mustFail(t, args, nil, "admin.bind")
		if !strings.Contains(err.Error(), "same TCP address") {
			t.Errorf("%v: refused for another reason: %v", args, err)
		}
	}
	// Controls: two distinct literals share a port; admin off; other ports;
	// port 0 asks the kernel and never collides; UDP is another protocol;
	// listen.tls and listen.https empty (the default) never collide.
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:20514", "--admin.bind=127.0.0.2:20514"}, nil)
	mustLoad(t, []string{"--admin.enabled=false", "--admin.bind=:12201"}, nil)
	mustLoad(t, []string{"--listen.tcp=:20514", "--admin.bind=127.0.0.1:20515"}, nil)
	mustLoad(t, []string{"--listen.tcp=127.0.0.1:0", "--admin.bind=127.0.0.1:0"}, nil)
	mustLoad(t, []string{"--listen.udp=:9104", "--listen.tcp=:12201"}, nil)
	mustLoad(t, append([]string{"--listen.tls=127.0.0.1:12203", "--admin.bind=127.0.0.2:12203"}, tlsArgs...), nil)
	mustLoad(t, append([]string{"--listen.https=127.0.0.1:12204", "--admin.bind=127.0.0.2:12204"}, tlsArgs...), nil)
	mustLoad(t, []string{"--listen.tls=", "--listen.https=", "--admin.bind=127.0.0.1:12203"}, nil)
	mustLoad(t, []string{"--listen.http=", "--admin.bind=127.0.0.1:12202"}, nil)
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

// The same for sink.<type>.pool_size, derived from sink.workers with the
// three relational destinations (SPEC §10.5). A sink.workers the validation
// LOWERED ("lowered to 4", not "derived") is still reported when it changes.
func TestReloadDoesNotReportTheDerivedPoolSize(t *testing.T) {
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		base := []string{"--sink.type=" + s}
		boot := mustLoad(t, append(base, "--sink.workers=4"), nil)
		fresh := mustLoad(t, append(base, "--sink.workers=7"), nil)
		if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "sink.workers" {
			t.Fatalf("%s: workers changed: ignored = %q, want only sink.workers (pool_size is derived from it)", s, ignored)
		}
		// Control: an explicit pool_size that changes is reported.
		boot = mustLoad(t, append(base, "--sink.workers=4", "--sink."+s+".pool_size=10"), nil)
		fresh = mustLoad(t, append(base, "--sink.workers=4", "--sink."+s+".pool_size=12"), nil)
		if _, ignored := Compare(boot, nil, fresh); len(ignored) != 1 || ignored[0] != "sink."+s+".pool_size" {
			t.Fatalf("%s: explicit pool_size changed: ignored = %q, want sink.%s.pool_size", s, ignored, s)
		}
	}
}

// sameStreamSocket reads the values a bind means, not their spellings
// (independent review of 2026-10-14 of the syslog server; SPEC §13.2): any
// unspecified literal is the wildcard, and the port is the number net.Listen
// would bind -- leading zeros and service names included. A port that is
// neither is refused here rather than at bind.
func TestSameStreamSocketReadsValuesNotSpellings(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"[::0]:9104", "127.0.0.1:9104", true},
		{"[0:0::0]:9104", "127.0.0.1:9104", true},
		{"[::ffff:0.0.0.0]:9104", "127.0.0.1:9104", true},
		{"127.0.0.1:0514", "127.0.0.1:514", true},
		{"0.0.0.0:0514", "127.0.0.1:514", true},
		{"127.0.0.1:http", "127.0.0.1:80", true},
		{"127.0.0.1:no-such-service-xyz", "127.0.0.1:80", true},
		// Controls: the rule did not become "always".
		{"127.0.0.1:0514", "127.0.0.1:515", false},
		{"127.0.0.1:514", "127.0.0.2:0514", false},
		{"[::0]:0", "127.0.0.1:0", false},
		{"127.0.0.1:00", "127.0.0.1:0", false},
		{"[::1]:9104", "127.0.0.1:9104", false},
	} {
		if got := sameStreamSocket(c.a, c.b); got != c.want {
			t.Errorf("sameStreamSocket(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
