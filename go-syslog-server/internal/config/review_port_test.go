package config

import (
	"strings"
	"testing"
)

// Configuration findings of the independent review of the port of the R, RP
// and RN fixes into this server (2026-10-13). Each case failed before its fix,
// or pins a configuration the fixes must keep accepting.

// The 16 Mi ceiling of ring.capacity_items is the memory ring's: it makes a
// channel of that many slots. ring=sqlite counts rows in an int64 and
// ring=redis compares doubles in Lua, and a 50-million-row ring was refused
// with them. The memory ring keeps it -- the one asked for, and the one
// ring=redis degrades to on the sink's own server (SPEC §9.7, §13.2).
func TestRingItemsCeilingIsTheMemoryRings(t *testing.T) {
	const big = "--ring.capacity_items=50000000"
	for _, mode := range []string{"sqlite", "redis"} {
		l := mustLoad(t, []string{"--ring.mode=" + mode, big, "--ring.max_bytes=16GiB"}, nil)
		if l.Config.Ring.CapacityItems != 50000000 {
			t.Errorf("ring=%s: capacity_items = %d", mode, l.Config.Ring.CapacityItems)
		}
	}
	mustFail(t, []string{"--ring.mode=memory", big}, nil, "ring.capacity_items")
	mustFail(t, []string{"--ring.mode=redis", "--sink.type=redis", big}, nil, "degraded from redis")
	// The sanity ceiling still refuses a typo, in every mode.
	for _, mode := range []string{"sqlite", "redis"} {
		mustFail(t, []string{"--ring.mode=" + mode, "--ring.capacity_items=35184372088832"}, nil,
			"ring.capacity_items")
	}
	// Control: the memory ceiling itself loads, degraded or not.
	mustLoad(t, []string{"--ring.mode=memory", "--ring.capacity_items=16777216"}, nil)
	mustLoad(t, []string{"--ring.mode=redis", "--sink.type=redis", "--ring.capacity_items=16777216"}, nil)
}

// sink.batch above 65 536 is a legitimate ClickHouse configuration (one
// INSERT is one part to merge: bigger is better there). The ceiling is 1 Mi;
// MySQL keeps its own 3 640.
func TestSinkBatchCeilingAdmitsALargeClickHouseBatch(t *testing.T) {
	mustLoad(t, []string{"--sink.type=clickhouse", "--sink.batch=100000", "--sink.batch_max_bytes=64MiB"}, nil)
	mustLoad(t, []string{"--sink.batch=1048576"}, nil)
	mustFail(t, []string{"--sink.batch=1048577"}, nil, "sink.batch")
	mustFail(t, []string{"--sink.type=mysql", "--sink.batch=3641"}, nil, "sink.batch")
}

// ring.max_inflight had no ceiling: the NATS MaxPending,
// 2*(workers*batch + max_inflight), overflowed into a negative number that
// NewNATS silently replaced with 4096.
func TestMaxInflightHasACeiling(t *testing.T) {
	for _, v := range []string{"1048577", "4611686018427387904", "9223372036854775807"} {
		mustFail(t, []string{"--ring.max_inflight=" + v}, nil, "ring.max_inflight")
	}
	l := mustLoad(t, []string{"--ring.max_inflight=1048576", "--sink.workers=1024", "--sink.batch=1048576"}, nil)
	c := l.Config
	if p := 2 * (c.Sink.Workers*c.Sink.Batch + c.Ring.MaxInflight); p <= 0 {
		t.Fatalf("MaxPending at every ceiling is %d: it overflows", p)
	}
}

// ring=redis with sink=none consumes the shared stream and discards entries
// that other replicas enqueued for a destination: the boot says so.
func TestRedisRingWithSinkNoneWarns(t *testing.T) {
	l := mustLoad(t, []string{"--ring.mode=redis", "--sink.type=none"}, nil)
	if !hasWarning(l, "DISCARDS entries") {
		t.Errorf("no warning for ring=redis + sink=none: %q", l.Warnings)
	}
	for _, args := range [][]string{
		{"--ring.mode=redis", "--sink.type=sqlite"},
		{"--ring.mode=sqlite", "--sink.type=none"},
	} {
		if l := mustLoad(t, args, nil); hasWarning(l, "DISCARDS entries") {
			t.Errorf("%v warned about a shared stream it does not discard", args)
		}
	}
}

// URL and address forms that two readers read differently (SPEC §10.9,
// §13.2): an empty host= in a PostgreSQL URL is libpq's default socket
// directory while this server dialed the authority; TLS over a PostgreSQL
// socket was retried as a network error until sink.boot_timeout (MySQL
// already refused it); a ',' in a Redis URL's host became two seeds of
// sink.redis.addrs.
func TestEBURLFormsThatMeanTwoThings(t *testing.T) {
	for _, tc := range []struct{ url, want string }{
		{"postgres://u:Zsecret@h/db?host=", "host= of the PostgreSQL URL is empty"},
		{"postgres://u:Zsecret@h:5433/db?host=&sslmode=disable", "host= of the PostgreSQL URL is empty"},
		{"postgres://u:Zsecret@/db?host=/run/pg&sslmode=require", "unix socket"},
		{"postgres://u:Zsecret@/db?host=/run/pg&sslmode=verify-full", "unix socket"},
		{"redis://u:Zsecret@a,b:6379/0", "holds a ','"},
		{"rediss://a,b/0", "holds a ','"},
	} {
		err := mustFail(t, nil, map[string]string{"LOGHUB_EB_URL": tc.url}, tc.want)
		if strings.Contains(err.Error(), "ecret") {
			t.Errorf("%s: echoed: %v", tc.url, err)
		}
	}
	pg := []string{"--sink.type=postgres", "--sink.postgres.username=u", "--sink.postgres.database=db"}
	mustFail(t, append(pg, "--sink.postgres.addr=/run/pg", "--sink.postgres.tls=true"), nil, "unix socket")
	mustFail(t, append(pg, "--sink.postgres.addr=/run/pg:5433", "--sink.postgres.tls=true"), nil, "unix socket")

	// Controls: the same URLs without the defect load and mean one thing.
	l := mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u@/db?host=/run/pg&sslmode=disable"})
	if l.Config.SinkPG.Addr != "/run/pg" || l.Config.SinkPG.TLS {
		t.Errorf("socket URL: addr %q tls %v", l.Config.SinkPG.Addr, l.Config.SinkPG.TLS)
	}
	l = mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "postgres://u@h/db?sslmode=require"})
	if l.Config.SinkPG.Addr != "h:5432" || !l.Config.SinkPG.TLS {
		t.Errorf("TCP URL: addr %q tls %v", l.Config.SinkPG.Addr, l.Config.SinkPG.TLS)
	}
	mustLoad(t, append(pg, "--sink.postgres.addr=pg:5432", "--sink.postgres.tls=true"), nil)
	l = mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "redis://h:6380/2"})
	if got := l.Config.SinkRedis.Addrs; len(got) != 1 || got[0] != "h:6380" {
		t.Errorf("redis URL: addrs %q", got)
	}
}

// The health check has a connection of its own (SPEC §10.5), so the pool is
// the writers' and pool_size = workers is the default. With ring=none the
// writers are the receivers: max_inflight equal to the pool is quiet, above it
// still warns (writers queue inside the pool, out of the ring's accounting).
func TestPoolIsTheWritersWithRingNone(t *testing.T) {
	for _, s := range []string{"postgres", "mysql", "clickhouse"} {
		base := []string{"--ring.mode=none", "--sink.type=" + s, "--sink.workers=4"}
		l := mustLoad(t, append(base, "--ring.max_inflight=4"), nil)
		if hasWarning(l, "ring.max_inflight") {
			t.Errorf("%s: max_inflight = pool warned: %q", s, l.Warnings)
		}
		if l := mustLoad(t, append(base, "--ring.max_inflight=5"), nil); !hasWarning(l, "ring.max_inflight=5") {
			t.Errorf("%s: max_inflight above the pool did not warn: %q", s, l.Warnings)
		}
	}
}
