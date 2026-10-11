package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/ident"
)

// sink=clickhouse adds three pairs to the matrix of SPEC §16.1, which then has
// 21. These run against a real server addressed by WHS_TEST_CLICKHOUSE_DSN
// (the native protocol) and skip without it:
//
//	WHS_TEST_CLICKHOUSE_DSN='clickhouse://whs:whs@127.0.0.1:19000/whs_test' \
//	  go test -race -timeout 900s ./internal/app/
//
// The destination is the inbox table that mirrors loghub.inbox (SPEC §3.9).
// Each test creates its own from docker/clickhouse/inbox.sql under a unique
// name and drops it afterwards.

const clickhouseDSNEnv = "WHS_TEST_CLICKHOUSE_DSN"

type chEnv struct {
	t                  *testing.T
	conn               driver.Conn
	addr               string
	user, pass, dbname string
}

func chTest(t *testing.T) *chEnv {
	t.Helper()
	dsn := os.Getenv(clickhouseDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the clickhouse sink tests (see docs/TESTING.md §5.5)", clickhouseDSNEnv)
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %v", clickhouseDSNEnv, err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("%s is not reachable: %v", clickhouseDSNEnv, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &chEnv{t: t, conn: conn, addr: opts.Addr[0], user: opts.Auth.Username,
		pass: opts.Auth.Password, dbname: opts.Auth.Database}
}

// table creates a fresh inbox from the canonical DDL under a unique name, with
// each edit applied to the DDL text first (old => new).
func (e *chEnv) table(edits ...string) string {
	e.t.Helper()
	name := fmt.Sprintf("whs_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/clickhouse/inbox.sql")
	if err != nil {
		e.t.Fatalf("reading the canonical DDL: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	s := strings.Join(lines, "\n")
	s = strings.Replace(s[:strings.LastIndexByte(s, ';')], "CREATE TABLE IF NOT EXISTS inbox (",
		"CREATE TABLE `"+name+"` (", 1)
	for i := 0; i+1 < len(edits); i += 2 {
		if !strings.Contains(s, edits[i]) {
			e.t.Fatalf("the DDL has no %q to edit", edits[i])
		}
		s = strings.Replace(s, edits[i], edits[i+1], 1)
	}
	if err := e.conn.Exec(context.Background(), s); err != nil {
		e.t.Fatalf("%v\n%s", err, s)
	}
	e.t.Cleanup(func() { _ = e.conn.Exec(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") })
	return name
}

// envelopes rebuilds every envelope in the table from its columns, FINAL so a
// redelivery the merge has not collapsed yet counts once (SPEC §3.9). The JSON
// column reorders keys, so the nine top-level fields are put back in the
// canonical order the JLH fixes before the oracle sees them.
func (e *chEnv) envelopes(table string) [][]byte {
	e.t.Helper()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"json_type_escape_dots_in_keys":           1,
		"output_format_json_quote_64bit_integers": 0,
	}))
	rows, err := e.conn.Query(ctx, "SELECT toString(uuid), toString(from_uuid), toString(space), "+
		"toString(type), level, sid, step, event_at, event_nsec, toString(payload) FROM `"+table+
		"` FINAL ORDER BY event_at, uuid")
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var c canonical
		var level uint8
		var sid uint64
		var step, nsec uint16
		var at time.Time
		var payload string
		if err := rows.Scan(&c.UUID, &c.From, &c.Space, &c.Type, &level, &sid, &step, &at, &nsec,
			&payload); err != nil {
			e.t.Fatal(err)
		}
		c.Level, c.SID, c.Step = int(level), int64(sid), int(step)
		c.setEvent(at, int64(nsec))
		c.Payload = json.RawMessage(payload)
		out = append(out, c.bytes(e.t))
	}
	if err := rows.Err(); err != nil {
		e.t.Fatal(err)
	}
	return out
}

// ids reads every uuid in the table, each envelope checked by the oracle.
func (e *chEnv) ids(table string, id ident.Identity, mode string) []string {
	e.t.Helper()
	return uuidsOf(verify(e.t, e.envelopes(table), id, mode))
}

func (e *chEnv) args(table string, extra ...string) []string {
	return append([]string{
		"--sink.type=clickhouse",
		"--sink.clickhouse.addrs=" + e.addr,
		"--sink.clickhouse.username=" + e.user,
		"--sink.clickhouse.password=" + e.pass,
		"--sink.clickhouse.database=" + e.dbname,
		"--sink.clickhouse.table=" + table,
	}, extra...)
}

// ------------------------------------------------------- the three new pairs

// SPEC §16.1: with sink=clickhouse every ring mode accepts traffic, reports
// the pair in whs_config_info, exits 0 and leaves every accepted uuid in the
// table, each envelope with the mode its pair promises (SPEC §3.2).
func TestClickHouseAllRingModes(t *testing.T) {
	e := chTest(t)
	for _, tc := range realSinkModes {
		t.Run(tc.ring, func(t *testing.T) {
			table := e.table()
			in := start(t, t.TempDir(), e.args(table,
				"--ring.mode="+tc.ring,
				"--ring.sqlite.synchronous=OFF")...)

			const n = 40
			ids := in.postN(n)
			if len(ids) != n {
				t.Fatalf("%d of %d requests were accepted", len(ids), n)
			}
			if in.mode != tc.mode {
				t.Errorf("the 200 answered mode %q, want %q", in.mode, tc.mode)
			}
			m := in.metrics()
			want := fmt.Sprintf(`whs_config_info{ring_mode="%s",sink_type="clickhouse"} 1`, tc.ring)
			if !strings.Contains(m, want) {
				t.Errorf("metrics missing %q", want)
			}
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit code %d, want %d", code, ExitOK)
			}
			got := e.ids(table, testID, tc.mode)
			if lost := missing(ids, got); len(lost) > 0 {
				t.Errorf("%d of %d accepted events never reached the table", len(lost), len(ids))
			}
			if extra := missing(got, ids); len(extra) > 0 {
				t.Errorf("%d rows in the table were never answered to a client", len(extra))
			}
		})
	}
}

// SPEC §11.1: ring=none writes through from the handler, and the wrapper of
// SPEC §10.2 must still observe every write and expose the sink's metrics.
func TestClickHouseRingNoneIsInstrumented(t *testing.T) {
	e := chTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=none")...)
	if len(in.postN(5)) != 5 {
		t.Fatal("not every request was accepted")
	}
	m := in.metrics()
	for _, want := range []string{
		"whs_events_written_total 5",
		"whs_sink_duration_seconds_count 5",
		"whs_sink_batch_size_count 5",
		`whs_sink_clickhouse_pool_conns{state="max"}`,
		"whs_sink_clickhouse_dedup_blocks_total 0",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// --------------------------------------------------------- the wire contract

// SPEC §3.2 and §3.9 end to end through the JSON column: a binary body
// travels in base64 and an empty body as an empty content, both exact after
// ClickHouse decomposes the document; a query with %00 lands with its NUL.
func TestClickHouseNULQueryAndBinaryBodyRoundTrip(t *testing.T) {
	e := chTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	binID, emptyID := postRoundTrip(in)
	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 2")
	}) {
		t.Error("the drain never reported both events as written")
	}
	if strings.Contains(in.metrics(), `whs_events_dropped_total{reason="sink_logic"} 1`) {
		t.Error("an event was dropped as sink_logic")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	checkRoundTrip(t, byUUID(t, verify(t, e.envelopes(table), testID, envelope.ModeFast)),
		binID, emptyID, false)
}

// SPEC §3.9: the query's keys are the client's, and none of them -- dots, a
// literal %2E, the empty key -- may take the batch down: as keys of the Map
// hint they are stored as they are, and the envelope comes back whole.
func TestClickHouseClientQueryKeysDoNotDropTheBatch(t *testing.T) {
	e := chTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	good := in.postN(3)
	code, odd := in.post("/keys?a.b=1&a=2&a%252Eb=3&=e&.=d&a..b=4&k=x&k=y", `{"q":1}`)
	if code != http.StatusOK {
		t.Fatalf("the request with dotted keys = %d", code)
	}
	more := in.postN(3)
	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 7")
	}) {
		t.Error("the drain never reported all seven events as written")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	got := byUUID(t, verify(t, e.envelopes(table), testID, envelope.ModeFast))
	if lost := missing(append(append(good, odd), more...), keysOfEnv(got)); len(lost) > 0 {
		t.Fatalf("%d events answered 200 are not in the table; a refusal took the batch down", len(lost))
	}
	q := got[odd].Payload.Query
	for k, want := range map[string]string{"a.b": "1", "a": "2", "a%2Eb": "3", "": "e", ".": "d", "a..b": "4"} {
		if v, ok := q[k]; !ok || fmt.Sprint(v) != want {
			t.Errorf("query[%q] = %v, want %q", k, v, want)
		}
	}
}

// SPEC §3.2: a request line carrying bytes that are not UTF-8 is a real
// event, and it must not take the rest of its batch down.
func TestClickHouseInvalidUTF8PathDoesNotDropTheBatch(t *testing.T) {
	e := chTest(t)
	table := e.table()
	in := start(t, t.TempDir(), e.args(table, "--ring.mode=memory")...)

	good := in.postN(3)
	badID := postInvalidUTF8(t, in)
	more := in.postN(3)

	if !waitFor(10*time.Second, func() bool {
		return strings.Contains(in.metrics(), "whs_events_written_total 7")
	}) {
		t.Error("the drain never reported all seven events as written")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}
	got := byUUID(t, verify(t, e.envelopes(table), testID, envelope.ModeFast))
	if lost := missing(append(append(good, badID), more...), keysOfEnv(got)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the table; a refusal took the batch down", len(lost))
	}
	checkInvalidUTF8(t, got[badID])
}

// --------------------------------------------------------------- boot checks

// SPEC §8.8.5 and §10.1: a table that violates the contract stops the boot AT
// ONCE with exit 3 -- not after sink.boot_timeout -- and the public port never
// opens. The control is the DDL as shipped, which boots. The sink suite
// refuses one table per rule; these are the rules end to end.
func TestClickHouseSchemaMismatchExitsThreeImmediately(t *testing.T) {
	e := chTest(t)

	t.Run("control", func(t *testing.T) {
		in := start(t, t.TempDir(), e.args(e.table(), "--ring.mode=memory")...)
		if code := in.stop(); code != ExitOK {
			t.Fatalf("the canonical table: exit code %d", code)
		}
	})

	cases := []struct {
		name  string
		table func() string
	}{
		{"missing table", func() string { return "whs_missing_" + strconv.FormatInt(time.Now().UnixNano(), 10) }},
		{"merge tree", func() string { return e.table("ENGINE = ReplacingMergeTree", "ENGINE = MergeTree") }},
		{"key without uuid", func() string { return e.table("ORDER BY (event_at, uuid)", "ORDER BY (event_at)") }},
		{"wrong column type", func() string { return e.table("sid         UInt64", "sid Int64") }},
		{"payload without hints", func() string {
			return e.table("payload     JSON(params Map(String, String), query Map(String, Dynamic))", "payload JSON")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := startExpectingExit(t, t.TempDir(), e.args(tc.table(),
				"--ring.mode=memory", "--sink.boot_timeout=120s")...)

			start := time.Now()
			select {
			case code := <-in.done:
				if code != ExitSink {
					t.Errorf("exit code %d, want %d", code, ExitSink)
				}
			case <-time.After(20 * time.Second):
				in.cancel()
				t.Fatal("the process kept retrying a schema that will never match")
			}
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("exit took %s; a fatal boot check must not wait for boot_timeout", took)
			}
			if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
				c.Close()
				t.Error("the public port accepted a connection despite a refused schema")
			}
		})
	}
}

// SPEC §10.1: an unreachable ClickHouse is retried until sink.boot_timeout and
// then exits 3 with the public port still shut.
func TestClickHouseUnreachableKeepsPublicPortShut(t *testing.T) {
	if os.Getenv(clickhouseDSNEnv) == "" {
		t.Skipf("set %s to run the clickhouse sink tests", clickhouseDSNEnv)
	}
	dead := freePort(t)
	in := startExpectingExit(t, t.TempDir(),
		"--ring.mode=memory", "--sink.type=clickhouse",
		"--sink.clickhouse.addrs="+dead, "--sink.boot_timeout=2s")

	select {
	case code := <-in.done:
		if code != ExitSink {
			t.Errorf("exit code %d, want %d", code, ExitSink)
		}
	case <-time.After(30 * time.Second):
		in.cancel()
		t.Fatal("the process never gave up on an unreachable ClickHouse")
	}
	if c, err := net.DialTimeout("tcp", in.pub, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("the public port accepted a connection while the sink was unreachable")
	}
}

// ------------------------------------------------------------- availability

// SPEC §15.3: the destination goes away and comes back. While it is away the
// server refuses with 503 and /readyz fails; when it returns, nothing that
// was answered 200 is missing from the table.
func TestClickHouseDownThenBackLosesNothing(t *testing.T) {
	e := chTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)

	args := e.args(table, "--ring.mode=memory",
		"--sink.fail_timeout=60s", "--sink.max_retries=1000",
		"--sink.clickhouse.addrs="+proxy.Addr())
	in := start(t, t.TempDir(), args...)

	before := in.postN(10)

	proxy.Pause()
	if !waitFor(20*time.Second, func() bool {
		code, _ := in.post("/hook", `{"during":true}`)
		return code == http.StatusServiceUnavailable
	}) {
		t.Fatal("the server never started refusing while the sink was down")
	}
	if code, _ := in.readyz(); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while the sink was down, want 503", code)
	}

	proxy.Resume()
	if !waitFor(30*time.Second, func() bool {
		code, _ := in.readyz()
		return code == http.StatusOK
	}) {
		t.Fatal("the server never recovered after the sink came back")
	}

	after := in.postN(10)
	if len(after) != 10 {
		t.Errorf("%d of 10 requests accepted after recovery", len(after))
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit code %d, want 0", code)
	}
	if lost := missing(append(before, after...), e.ids(table, testID, envelope.ModeFast)); len(lost) > 0 {
		t.Errorf("%d events answered 200 are not in the table", len(lost))
	}
}

// ---------------------------------------------------------- emergency cycle

// SPEC §9 end to end: the sink dies for good, the ring still holds events
// answered 200, they go to an emergency dump, the process exits 3, and the
// next boot reingests them with every column filled from the envelope itself,
// reachable by their columns.
func TestClickHouseFatalDumpsAndReingestsWithQueryableColumns(t *testing.T) {
	e := chTest(t)
	table := e.table()
	proxy := newTCPProxy(t, e.addr)
	dir := t.TempDir()

	args := e.args(table, "--ring.mode=memory",
		"--sink.workers=1",
		"--sink.batch_max_delay=10s", "--sink.batch=512",
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--shutdown.drain_timeout=3s",
		"--sink.clickhouse.addrs="+proxy.Addr())

	in := start(t, dir, args...)
	const n = 8
	ids := in.postN(n)
	if len(ids) != n {
		t.Fatalf("%d of %d accepted", len(ids), n)
	}

	proxy.Pause()
	if code := in.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit code %d, want %d", code, ExitSink)
	}
	checkDump(t, dir, testID, envelope.ModeFast, ids)

	proxy.Resume()
	in2 := start(t, dir, args...)
	if code := in2.stop(); code != ExitOK {
		t.Errorf("the second run exited %d, want 0", code)
	}
	if left := emergencyFiles(t, filepath.Join(dir, "emergency")); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	got := e.ids(table, testID, envelope.ModeFast)
	if lost := missing(ids, got); len(lost) > 0 {
		t.Fatalf("%d of %d events answered 200 were lost across the crash", len(lost), len(ids))
	}

	var reachable uint64
	if err := e.conn.QueryRow(context.Background(), "SELECT count() FROM `"+table+"` FINAL"+`
		 WHERE from_uuid = toUUID(?) AND space = ? AND type = 'webhook'
		   AND level = 9 AND step = 0 AND sid = ?
		   AND event_at > now64(6) - INTERVAL 1 HOUR AND event_at <= now64(6)`,
		testID.From, testID.Space, uint64(os.Getpid())).Scan(&reachable); err != nil {
		t.Fatal(err)
	}
	if reachable != n {
		t.Errorf("%d of %d reingested rows are reachable by their columns", reachable, n)
	}
}
