package config

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// mustRefuse loads args (and env) and wants an error that names want.
func mustRefuse(t *testing.T, args []string, env map[string]string, want string) {
	t.Helper()
	_, err := Load(Options{Args: args, LookupEnv: envMap(env)})
	if err == nil {
		t.Fatalf("%v %v was accepted", args, env)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("%v %v: error %q does not mention %q", args, env, err, want)
	}
}

// SPEC §8.2.2 (W pendência 13): a CA without tls is the same configuration error
// it is for every other destination. Silently connecting in the clear sends
// the Redis password in plaintext, the opposite of what the operator wanted.
func TestRedisTLSCAWithoutTLSIsRefused(t *testing.T) {
	mustRefuse(t, []string{"--sink.type=redis", "--sink.redis.tls_ca=/etc/ca.pem"}, nil,
		"sink.redis.tls_ca is set but sink.redis.tls=false")
	mustRefuse(t, nil, map[string]string{"WHS_SINK_REDIS_TLS_CA": "/etc/ca.pem"},
		"sink.redis.tls_ca is set but")
	load(t, Options{Args: []string{"--sink.type=redis", "--sink.redis.tls_ca=/etc/ca.pem", "--sink.redis.tls=true"}})
	// Like the other destinations, the rule bites only when redis is the sink.
	load(t, Options{Args: []string{"--sink.type=sqlite", "--sink.redis.tls_ca=/etc/ca.pem"}})
}

// SPEC §13.5 (W pendência 24): ParseFloat accepts NaN and ±Inf, and NaN passes
// every range check, because every comparison with it is false.
func TestNonFiniteNumbersAreRefused(t *testing.T) {
	for _, v := range []string{"NaN", "nan", "Inf", "-Inf", "+inf"} {
		mustRefuse(t, []string{"--ring.high_water=" + v}, nil, "must be finite")
		mustRefuse(t, []string{"--log.access_sample=" + v}, nil, "must be finite")
		mustRefuse(t, []string{"--ratelimit.enabled=true", "--ratelimit.rate=" + v}, nil, "must be finite")
		mustRefuse(t, []string{"--ratelimit.enabled=true", "--ratelimit.burst=" + v}, nil, "must be finite")
		// With the limiter off the range is not checked, and a SIGHUP may turn
		// it on: the parser is what keeps NaN out.
		mustRefuse(t, []string{"--ratelimit.enabled=false", "--ratelimit.rate=" + v}, nil, "must be finite")
		mustRefuse(t, nil, map[string]string{"WHS_RING_HIGH_WATER": v}, "must be finite")
		ini := writeINI(t, "[log]\naccess_sample = "+v+"\n")
		mustRefuse(t, []string{"--config", ini}, nil, "line 2")
	}
	// Control: finite values in range still load.
	load(t, Options{Args: []string{"--ring.high_water=0.5", "--log.access_sample=1e-3",
		"--ratelimit.enabled=true", "--ratelimit.rate=2.5", "--ratelimit.burst=1e3"}})
}

// SPEC §11, §13.5 (W pendência 25): an empty admin.bind made net.Listen bind
// every interface on a random port, publishing /metrics wherever the host is
// reachable, with a WARN only.
func TestEmptyAdminBindIsRefused(t *testing.T) {
	mustRefuse(t, []string{"--admin.bind="}, nil, "admin.bind is empty")
	mustRefuse(t, nil, map[string]string{"WHS_ADMIN_BIND": ""}, "admin.bind is empty")
	mustRefuse(t, []string{"--admin.bind=9101"}, nil, "not host:port")
	mustRefuse(t, []string{"--admin.bind=127.0.0.1"}, nil, "not host:port")
	// SplitHostPort takes an empty port without error, and net.Listen then
	// picks one -- with ":" on every interface (W §19.1 item 40).
	for _, v := range []string{"127.0.0.1:", ":", "[::1]:", "localhost:abc", "127.0.0.1:0",
		"127.0.0.1:65536", "127.0.0.1:+80", "127.0.0.1:080"} {
		mustRefuse(t, []string{"--admin.bind=" + v}, nil, "not host:port")
	}
	load(t, Options{Args: []string{"--admin.bind=[::1]:9101"}})
	// Turning the port off is how the operator says "no admin port", and then
	// neither the bind nor its exposure warning matters.
	l := load(t, Options{Args: []string{"--admin.enabled=false", "--admin.bind="}})
	if hasWarning(l.Warnings, "admin.bind") {
		t.Errorf("warnings = %v: admin.bind warned with the admin port off", l.Warnings)
	}
	load(t, Options{Args: []string{"--admin.bind=:9101"}}) // every interface, on purpose: WARN only
}

// SPEC §13.2 (W pendência 26): the dump is a config file that reads back the
// same. The INI reader trims the value and removes one layer of quotes, so a
// value padded with spaces, or wrapped in quotes of its own, came back changed.
func TestDumpINIRoundTripsQuotedAndPaddedValues(t *testing.T) {
	ini := writeINI(t, "[emergency]\ndir = \" /tmp/e \"\n"+
		"[sink.redis]\nstream = '\"q\"'\n"+
		"[headers]\nexclude = '\"Cookie\"'\n")
	l := load(t, Options{Args: []string{"--config", ini}, LookupEnv: envMap(map[string]string{
		"WHS_SINK_SQLITE_PATH": " /tmp/padded.db ",
	})})
	if l.Config.Emergency.Dir != " /tmp/e " || l.Config.SinkRedis.Stream != `"q"` ||
		l.Config.SinkSQL.Path != " /tmp/padded.db " {
		t.Fatalf("the provocation did not load as written: dir=%q stream=%q path=%q",
			l.Config.Emergency.Dir, l.Config.SinkRedis.Stream, l.Config.SinkSQL.Path)
	}
	var buf bytes.Buffer
	if err := l.DumpINI(&buf); err != nil {
		t.Fatal(err)
	}
	back := load(t, Options{Args: []string{"--config", writeINI(t, buf.String())}})
	if !reflect.DeepEqual(back.Config, l.Config) {
		t.Fatalf("the dump did not read back:\n%+v\n%+v\n%s", back.Config, l.Config, buf.String())
	}
	for k, v := range l.Effective() {
		if bv := back.Effective()[k]; bv != v {
			t.Errorf("%s: %q -> %q", k, v, bv)
		}
	}
}

func TestQuoteINI(t *testing.T) {
	for in, want := range map[string]string{
		"plain":    "plain",
		"":         "",
		" padded ": `" padded "`,
		"\ttab":    "\"\ttab\"",
		`"q"`:      `""q""`,
		`'q'`:      `"'q'"`,
		`"q'`:      `"q'`,
		`"`:        `"`,
		`a"b"`:     `a"b"`,
	} {
		if got := quoteINI(in); got != want {
			t.Errorf("quoteINI(%q) = %q, want %q", in, got, want)
		}
		// What the INI reader does to the value after the '='.
		if got := unquote(strings.TrimSpace(quoteINI(in))); got != in {
			t.Errorf("%q does not read back: %q", in, got)
		}
	}
}

// SPEC §7.2 (W §19.1 item 35): with ring=memory, a ring.max_bytes below what
// the largest accepted event costs the ring -- its pool class plus
// ring.ItemOverhead -- is exit 1. Below it that event got 429 always, with
// the ring empty, and its sender retried it for ever.
func TestRingMaxBytesCoversTheLargestEvent(t *testing.T) {
	small := []string{"--ring.mode=memory", "--server.max_body_bytes=1KiB",
		"--server.max_header_bytes=1KiB", "--server.max_uri_bytes=1KiB"}
	// MaxEventBytes is ~36 KiB here, in the 64 KiB class: the floor is the
	// class, not the event.
	ev := envelope.MaxEventBytes(1<<10, 1<<10, 1<<10)
	if ev <= 32<<10 || ev > 64<<10 {
		t.Fatalf("MaxEventBytes = %d: the case no longer lands in the 64 KiB class", ev)
	}
	floor := int64(64<<10) + 256
	mustRefuse(t, append(small, "--ring.max_bytes="+strconv.FormatInt(floor-1, 10)), nil, "ring.max_bytes")
	mustRefuse(t, append(small, "--ring.max_bytes=40KiB"), nil, "costs the ring")
	load(t, Options{Args: append(small, "--ring.max_bytes="+strconv.FormatInt(floor, 10))})
	// The defaults: a 5 MiB body above every class, so the event itself.
	mustRefuse(t, []string{"--ring.mode=memory", "--ring.max_bytes=4MiB"}, nil, "ring.max_bytes")
	load(t, Options{Args: []string{"--ring.mode=memory"}})
}

// SPEC §7.2 (W §19.1 item 43): with ring=sqlite the floor is the largest
// event itself, which is what that ring charges (the JLH's length, no pool
// class and no per-item overhead) -- the rule of the syslog and RELP parents
// for their rings that count the data.
func TestSQLiteRingMaxBytesCoversTheLargestEvent(t *testing.T) {
	small := []string{"--ring.mode=sqlite", "--server.max_body_bytes=1KiB",
		"--server.max_header_bytes=1KiB", "--server.max_uri_bytes=1KiB"}
	ev := envelope.MaxEventBytes(1<<10, 1<<10, 1<<10)
	mustRefuse(t, append(small, "--ring.max_bytes="+strconv.FormatInt(ev-1, 10)), nil, "ring.max_bytes")
	load(t, Options{Args: append(small, "--ring.max_bytes="+strconv.FormatInt(ev, 10))})
	load(t, Options{Args: []string{"--ring.mode=sqlite"}})
}

// SPEC §13.2 (W §19.1 item 41): a text value never carries a control byte. The INI
// cannot write one: the dump split the value over two lines, which read back
// as "expected 'key = value'" or as an injected key or section. The error
// names the key, never the value; the tab, which the INI takes, stays.
func TestControlBytesInTextValuesAreRefused(t *testing.T) {
	for _, tc := range []struct {
		args []string
		env  map[string]string
		key  string
	}{
		{nil, map[string]string{"WHS_EMERGENCY_DIR": "/data/a\nsecret = x"}, "WHS_EMERGENCY_DIR"},
		{[]string{"--emergency.dir=/data/a\rsecret"}, nil, "--emergency.dir"},
		{[]string{"--headers.exclude=Cookie,Se\x00cret"}, nil, "--headers.exclude"},
		{[]string{"--emergency.dir=/data/\x7fsecret"}, nil, "--emergency.dir"},
	} {
		_, err := Load(Options{Args: tc.args, LookupEnv: envMap(tc.env)})
		if err == nil {
			t.Errorf("%q %q was accepted", tc.args, tc.env)
			continue
		}
		if !strings.Contains(err.Error(), tc.key) || !strings.Contains(err.Error(), "control byte") {
			t.Errorf("error %q does not name %s and the control byte", err, tc.key)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Errorf("error %q echoes the value", err)
		}
	}
	l := load(t, Options{Args: []string{"--emergency.dir=/data/a\tb"}})
	if l.Config.Emergency.Dir != "/data/a\tb" {
		t.Fatalf("a tab did not load: %q", l.Config.Emergency.Dir)
	}
}
