package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// sink=clickhouse adds a column to the matrix of SPEC §19. These run against a
// real server addressed by RELPLOG_TEST_CLICKHOUSE_DSN (the native protocol)
// and skip without it; the ring=redis pair also needs RELPLOG_TEST_REDIS_ADDR:
//
//	RELPLOG_TEST_CLICKHOUSE_DSN='clickhouse://sls:sls@127.0.0.1:19000/sls_test' \
//	  go test -race -run ClickHouse ./internal/app/
//
// The server never runs DDL (invariant §20.17): each test creates its table
// from docker/clickhouse/inbox_syslog.sql under a unique name and drops it.

const clickhouseDSNEnv = "RELPLOG_TEST_CLICKHOUSE_DSN"

type chEnv struct {
	conn               driver.Conn
	addr               string
	user, pass, dbname string
}

func chTest(t *testing.T) *chEnv {
	t.Helper()
	dsn := os.Getenv(clickhouseDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the clickhouse suite", clickhouseDSNEnv)
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
	return &chEnv{conn: conn, addr: opts.Addr[0], user: opts.Auth.Username, pass: opts.Auth.Password,
		dbname: opts.Auth.Database}
}

// table creates a fresh inbox_syslog from the canonical DDL under a unique
// name, each edit applied to the DDL text first (old => new).
func (e *chEnv) table(t *testing.T, edits ...string) string {
	t.Helper()
	name := fmt.Sprintf("sls_app_%d", time.Now().UnixNano())
	b, err := os.ReadFile("../../docker/clickhouse/inbox_syslog.sql")
	if err != nil {
		t.Fatal(err)
	}
	var keep []string
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			keep = append(keep, line)
		}
	}
	s := strings.TrimSuffix(strings.TrimSpace(strings.Join(keep, "\n")), ";")
	s = strings.Replace(s, "CREATE TABLE IF NOT EXISTS inbox_syslog (", "CREATE TABLE `"+name+"` (", 1)
	for i := 0; i+1 < len(edits); i += 2 {
		if !strings.Contains(s, edits[i]) {
			t.Fatalf("the DDL has no %q to edit", edits[i])
		}
		s = strings.Replace(s, edits[i], edits[i+1], 1)
	}
	if err := e.conn.Exec(context.Background(), s); err != nil {
		t.Fatalf("applying docker/clickhouse/inbox_syslog.sql: %v\n%s", err, s)
	}
	t.Cleanup(func() { _ = e.conn.Exec(context.Background(), "DROP TABLE IF EXISTS `"+name+"`") })
	return name
}

func (e *chEnv) args(dir, table string, extra ...string) []string {
	return append(append(baseArgs(dir),
		"--sink.type=clickhouse",
		"--sink.clickhouse.addrs="+e.addr,
		"--sink.clickhouse.username="+e.user,
		"--sink.clickhouse.password="+e.pass,
		"--sink.clickhouse.database="+e.dbname,
		"--sink.clickhouse.table="+table,
	), extra...)
}

// count reads the table with FINAL: a redelivery the merge has not collapsed
// yet counts once (SPEC §3.7.2).
func (e *chEnv) count(t *testing.T, table string) int {
	t.Helper()
	var n uint64
	if err := e.conn.QueryRow(context.Background(), "SELECT count() FROM `"+table+"` FINAL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return int(n)
}

// envelopes rebuilds every envelope in the table the way a consumer of the
// mirror does (SPEC §3.7.1, §3.7.2): the header from the columns, event from
// event_at plus event_nsec, the payload as the JSON column gives it, and the
// one read rule of the hint -- "structured-data":{} is absent. Each passes
// through mustJLH.
func (e *chEnv) envelopes(t *testing.T, table string) []jlhEnv {
	t.Helper()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"json_type_escape_dots_in_keys":           1,
		"output_format_json_quote_64bit_integers": 0,
	}))
	rows, err := e.conn.Query(ctx, "SELECT toString(uuid), toString(from_uuid), toString(space), "+
		"toString(type), level, sid, step, event_at, event_nsec, toString(payload) FROM `"+table+
		"` FINAL ORDER BY event_at, uuid")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []jlhEnv
	for rows.Next() {
		var (
			uuid, from, space, typ, payload string
			level                           uint8
			sid                             uint64
			step, nsec                      uint16
			at                              time.Time
		)
		if err := rows.Scan(&uuid, &from, &space, &typ, &level, &sid, &step, &at, &nsec, &payload); err != nil {
			t.Fatal(err)
		}
		var p map[string]any
		dec := json.NewDecoder(strings.NewReader(payload))
		dec.UseNumber()
		if err := dec.Decode(&p); err != nil {
			t.Fatalf("payload column is not JSON: %v\n%s", err, payload)
		}
		if sd, ok := p["structured-data"].(map[string]any); ok && len(sd) == 0 {
			delete(p, "structured-data")
		}
		at = at.UTC()
		sub := int64(at.Nanosecond()) + int64(nsec)
		env := map[string]any{
			"uuid": uuid, "from": from, "space": space, "type": typ, "level": level, "sid": sid,
			"step": step, "payload": p,
			"event": map[string]any{"uts": at.Unix(), "sub": sub,
				"iso": time.Unix(at.Unix(), sub).UTC().Format("2006-01-02T15:04:05.000Z")},
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(env); err != nil {
			t.Fatal(err)
		}
		out = append(out, mustJLH(t, bytes.TrimSuffix(buf.Bytes(), []byte("\n"))))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// chLines is the traffic of the end-to-end tests: the shapes of sqlLines plus
// the ones the JSON column must keep -- an SD element without parameters, SD
// names with dots and a repeated PARAM-NAME, which the Map hint stores as
// keys (SPEC §3.7.2).
var chLines = append(append([]string(nil), sqlLines...),
	`<165>1 2026-09-12T14:33:01Z h app - ID1 [origin][a.b x.y="1" x.y="2"][a b.c="3"] ch-sd`)

// checkCHEnvelopes says every line arrived exactly once, whole.
func checkCHEnvelopes(t *testing.T, envs []jlhEnv) {
	t.Helper()
	if len(envs) != len(chLines) {
		t.Fatalf("%d envelopes, want %d", len(envs), len(chLines))
	}
	seen := map[string]bool{}
	for _, e := range envs {
		m := e.Payload.message()
		if seen[m] {
			t.Errorf("%q stored twice", m)
		}
		seen[m] = true
		if e.From != testID.From {
			t.Errorf("from = %s", e.From)
		}
		if m == "ch-sd" {
			for _, want := range []string{`"origin":{}`, `"x.y":["1","2"]`, `"b.c":"3"`} {
				if !bytes.Contains(e.bytes, []byte(want)) {
					t.Errorf("the SD came back without %s:\n%s", want, e.bytes)
				}
			}
		}
		if strings.Contains(m, "sql-full") && string(e.Payload.rawBytes()) != sqlLines[0] {
			t.Errorf("payload.raw changed: %q", e.Payload.rawBytes())
		}
	}
	for _, want := range []string{"sql-full \x00 com nul", "sql-nil", "sql-nopid", "sql-nul-columns", "ch-sd"} {
		if !seen[want] {
			t.Errorf("%q is missing", want)
		}
	}
}

// SPEC §19: with sink=clickhouse every ring mode delivers every line, reports
// the pair in relplog_config_info and exits 0; each row rebuilds the envelope.
func TestClickHouseAllRingModes(t *testing.T) {
	e := chTest(t)
	rings := []struct {
		mode string
		args func(t *testing.T, dir string) []string
	}{
		{"memory", func(t *testing.T, dir string) []string { return baseArgs(dir) }},
		{"sqlite", func(t *testing.T, dir string) []string {
			return append(baseArgs(dir), "--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF")
		}},
		{"none", func(t *testing.T, dir string) []string { return append(baseArgs(dir), "--ring.mode=none") }},
		{"redis", func(t *testing.T, dir string) []string {
			r := redisTest(t)
			return newSharedRing(t, r).args(r, dir)
		}},
	}
	for _, rc := range rings {
		t.Run(rc.mode, func(t *testing.T) {
			table := e.table(t)
			dir := t.TempDir()
			args := append(rc.args(t, dir), e.args(dir, table)[len(baseArgs(dir)):]...)
			in := launch(t, testID, args, nil)
			in.waitReady()
			sendRELP(t, in.app.TransportAddr("tcp"), chLines...)
			waitFor(t, 30*time.Second, "every row in clickhouse", func() bool { return e.count(t, table) == len(chLines) })
			if code := in.stop(); code != ExitOK {
				t.Errorf("exit %d", code)
			}
			checkCHEnvelopes(t, e.envelopes(t, table))
		})
	}
}

// SPEC §14.1: the destination's metrics are exposed, and relplog_config_info
// names the pair.
func TestClickHouseMetrics(t *testing.T) {
	e := chTest(t)
	table := e.table(t)
	dir := t.TempDir()
	in := launch(t, testID, e.args(dir, table), nil)
	in.waitReady()
	sendRELP(t, in.app.TransportAddr("tcp"), sqlLines[:2]...)
	waitFor(t, 20*time.Second, "both rows", func() bool { return e.count(t, table) == 2 })
	if got := in.metric(`relplog_config_info{ring_mode="memory",sink_type="clickhouse",transports="tcp"}`); got != "1" {
		t.Errorf("relplog_config_info = %s", got)
	}
	if got := in.metric(`relplog_sink_clickhouse_pool_conns{state="max"}`); got != "5" {
		t.Errorf("pool max = %s, want sink.workers + 1 = 5", got)
	}
	if got := in.metric("relplog_sink_clickhouse_dedup_blocks_total"); got != "0" {
		t.Errorf("dedup blocks = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}

// SPEC §10.10.5 and §13.1: a table that violates the contract stops the boot at
// once with exit 3 and no transport opens. The sink suite refuses one table per
// rule; these are the rules end to end.
func TestClickHouseSchemaMismatchExitsThree(t *testing.T) {
	e := chTest(t)
	const payload = "payload     JSON(`structured-data` Map(String, Map(String, Dynamic)))"
	for _, tc := range []struct {
		name  string
		edits []string
	}{
		{"missing table", nil},
		{"merge tree", []string{"ENGINE = ReplacingMergeTree", "ENGINE = MergeTree"}},
		{"key without uuid", []string{"ORDER BY (event_at, uuid)", "ORDER BY (event_at)"}},
		{"signed sid", []string{"sid         UInt64", "sid Int64"}},
		{"the webhook table", []string{payload, "payload JSON(params Map(String, String), query Map(String, Dynamic))"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := fmt.Sprintf("sls_missing_%d", time.Now().UnixNano())
			if tc.edits != nil {
				table = e.table(t, tc.edits...)
			}
			refusedBoot(t, e.args(t.TempDir(), table))
		})
	}
	t.Run("control", func(t *testing.T) {
		in := launch(t, testID, e.args(t.TempDir(), e.table(t)), nil)
		in.waitReady()
		if code := in.stop(); code != ExitOK {
			t.Errorf("the canonical table: exit %d", code)
		}
	})
}

// SPEC §5.7, §13.3 with a real destination: ClickHouse goes away and comes
// back. While it is away /readyz fails and the RELP transactions are held,
// unanswered; when it returns each gets its "200 OK", nothing is missing and
// nothing is stored twice.
func TestClickHouseDownThenBackLosesNothing(t *testing.T) {
	e := chTest(t)
	table := e.table(t)
	proxy := newTCPProxy(t, e.addr)
	dir := t.TempDir()
	in := launch(t, testID, e.args(dir, table,
		"--sink.clickhouse.addrs="+proxy.addr(),
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000",
		"--sink.op_timeout=1s", "--sink.health_timeout=500ms",
		// The recovery below may take longer than the default 30 s hold.
		"--relp.backpressure_timeout=120s"), nil)
	in.waitReady()
	tcp := in.app.TransportAddr("tcp")

	before := make([]string, 10)
	for i := range before {
		before[i] = fmt.Sprintf("<13>1 - h app - - - before-%04d", i)
	}
	sendRELP(t, tcp, before...)
	waitFor(t, 20*time.Second, "the first ten in the table", func() bool { return e.count(t, table) == 10 })

	proxy.pause()
	waitFor(t, 20*time.Second, "/readyz to fail while ClickHouse is away", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "sink is DOWN")
	})
	during := make([]string, 10)
	for i := range during {
		during[i] = fmt.Sprintf("<13>1 - h app - - - during-%04d", i)
	}
	rc := dialRELP(t, tcp)
	held := rc.syslog(during...)
	rc.expectNothing(300 * time.Millisecond)

	proxy.resume()
	waitFor(t, 60*time.Second, "the recovery", func() bool { code, _ := in.get("/readyz"); return code == 200 })
	for i, body := range rc.responses(held) {
		if body != relpRSPOK {
			t.Errorf("held transaction %d answered %q after the recovery", i, body)
		}
	}
	rc.close()
	noDrops(t, in.app.metrics)
	waitFor(t, 30*time.Second, "everything in the table", func() bool { return e.count(t, table) == 20 })
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	got := map[string]bool{}
	for _, env := range e.envelopes(t, table) {
		got[env.Payload.message()] = true
	}
	for _, m := range append(before, during...) {
		if k := m[strings.LastIndex(m, " ")+1:]; !got[k] {
			t.Errorf("%s is missing from the table", k)
		}
	}
}
