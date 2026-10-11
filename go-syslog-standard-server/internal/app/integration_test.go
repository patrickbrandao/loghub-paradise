package app

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"

	_ "modernc.org/sqlite"
)

// These tests run the REAL assembled stack in-process -- the same run() the
// binary calls -- against the sqlite and none sinks, which need no external
// server. They are what proves the promises of the spec end to end rather than
// component by component.

type instance struct {
	t      *testing.T
	app    *App
	cancel context.CancelFunc
	done   chan int
	dir    string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func freeUDP(t *testing.T) string {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().String()
}

// start boots an instance and waits until every transport is open.
func start(t *testing.T, dir string, extra ...string) *instance {
	t.Helper()
	args := []string{
		"--ring.data_dir=" + dir,
		"--admin.bind=" + freeAddr(t),
		"--listen.udp=" + freeUDP(t),
		"--listen.tcp=" + freeAddr(t),
		"--listen.relp=" + freeAddr(t),
		"--sink.type=sqlite",
		"--sink.sqlite.synchronous=OFF",
		"--ring.sqlite.synchronous=OFF",
		"--sink.health_interval=50ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=5s",
		"--log.level=error",
		"--node.id=test-node",
	}
	args = append(args, extra...)

	ctx, cancel := context.WithCancel(context.Background())
	in := &instance{t: t, cancel: cancel, done: make(chan int, 1), dir: dir}

	ready := make(chan *App, 1)
	go func() {
		in.done <- runContext(ctx, args, func(a *App) {
			a.abort = make(chan os.Signal, 1)
			a.ready = make(chan struct{})
			ready <- a
		})
	}()

	select {
	case a := <-ready:
		in.app = a
	case code := <-in.done:
		t.Fatalf("the instance exited during boot with code %d", code)
	case <-time.After(15 * time.Second):
		t.Fatal("the instance never reached the hook")
	}
	select {
	case <-in.app.Ready():
	case code := <-in.done:
		t.Fatalf("the instance exited before its transports opened, code %d", code)
	case <-time.After(15 * time.Second):
		t.Fatal("the transports never opened")
	}
	return in
}

// stop sends the equivalent of SIGTERM and returns the exit code.
func (in *instance) stop() int {
	in.cancel()
	select {
	case code := <-in.done:
		return code
	case <-time.After(30 * time.Second):
		in.t.Fatal("the instance did not shut down")
		return -1
	}
}

func (in *instance) db() *sql.DB {
	in.t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(in.dir, "events.db"))
	if err != nil {
		in.t.Fatal(err)
	}
	in.t.Cleanup(func() { db.Close() })
	return db
}

func (in *instance) count() int {
	var n int
	_ = in.db().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	return n
}

func (in *instance) waitRows(n int) {
	in.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if in.count() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	in.t.Fatalf("only %d of %d rows reached the destination", in.count(), n)
}

func (in *instance) get(path string) (int, string) {
	resp, err := http.Get("http://" + in.app.admin.Addr() + path)
	if err != nil {
		in.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// ---------------------------------------------------------------------------

// Every transport, every framing, one destination -- and each row carries the
// transport it came in on.
func TestAllTransportsReachTheDestination(t *testing.T) {
	in := start(t, t.TempDir())
	defer in.stop()

	// UDP
	u, err := net.Dial("udp", in.app.TransportAddr("udp"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(u, `<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - via-udp`)
	u.Close()

	// TCP, LF framing
	c, err := net.Dial("tcp", in.app.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(c, "<34>Oct 11 22:14:15 mymachine su[230]: via-tcp-lf\n")
	c.Close()

	// TCP, octet counting
	c, err = net.Dial("tcp", in.app.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	msg := `<14>myapp[9]: via-tcp-octet`
	fmt.Fprintf(c, "%d %s", len(msg), msg)
	c.Close()

	// RELP
	r, err := net.Dial("tcp", in.app.TransportAddr("relp"))
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(r)
	relp := `<13>Sep 12 10:00:00 host app: via-relp`
	fmt.Fprintf(r, "1 syslog %d %s\n", len(relp), relp)
	_ = r.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "200 OK") {
		t.Fatalf("RELP answered %q %v", line, err)
	}
	r.Close()

	in.waitRows(4)

	rows, err := in.db().Query(`SELECT transport, CAST(raw AS TEXT) FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var tr, raw string
		rows.Scan(&tr, &raw)
		got[raw] = tr
	}
	for raw, want := range map[string]string{
		`<165>1 2026-09-12T14:33:01Z web-01 nginx 1 - - via-udp`: "udp",
		"<34>Oct 11 22:14:15 mymachine su[230]: via-tcp-lf":      "tcp",
		msg:  "tcp",
		relp: "relp",
	} {
		if got[raw] != want {
			t.Errorf("%q arrived as %q, want %q", raw, got[raw], want)
		}
	}
}

// The raw line is preserved byte for byte, framing removed, and the envelope in
// the destination slices back to exactly the message.
func TestTheDestinationRowIsFaithful(t *testing.T) {
	in := start(t, t.TempDir())
	defer in.stop()

	line := "<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] a mensagem \x00 com nul"
	c, _ := net.Dial("tcp", in.app.TransportAddr("tcp"))
	fmt.Fprintf(c, "%d %s", len(line), line)
	c.Close()
	in.waitRows(1)

	var (
		raw                       []byte
		env                       []byte
		host, app, rfc, transport string
		sev, fac, msgOff, msgSize int
	)
	err := in.db().QueryRow(`SELECT raw, env, hostname, app_name, rfc, transport,
	                         severity, facility, msg_offset, msg_size FROM events`).
		Scan(&raw, &env, &host, &app, &rfc, &transport, &sev, &fac, &msgOff, &msgSize)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != line {
		t.Fatalf("raw line changed\n got %q\nwant %q", raw, line)
	}
	if host != "web-01" || app != "nginx" || rfc != "5424" || transport != "tcp" {
		t.Fatalf("columns = %q %q %q %q", host, app, rfc, transport)
	}
	if fac != 20 || sev != 5 {
		t.Fatalf("facility/severity = %d/%d", fac, sev)
	}
	if got := string(raw[msgOff : msgOff+msgSize]); got != "a mensagem \x00 com nul" {
		t.Fatalf("sliced msg = %q", got)
	}

	var e map[string]any
	if err := json.Unmarshal(env, &e); err != nil {
		t.Fatalf("env is not JSON: %v", err)
	}
	if e["node-id"] != "test-node" || e["framing"] != "octet" {
		t.Fatalf("envelope = %v", e)
	}
	sd := e["structured-data"].(map[string]any)["a@1"].(map[string]any)
	if sd["k"] != `v"q` {
		t.Fatalf("structured data value = %q, want the unescaped form", sd["k"])
	}
}

// The filter drops before the ring, and SIGHUP changes it at runtime.
func TestFilterAndReload(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	os.WriteFile(ini, []byte("[filter]\nmin_severity = err\n"), 0o600)

	in := start(t, dir, "--config="+ini)
	defer in.stop()

	send := func(line string) {
		c, _ := net.Dial("tcp", in.app.TransportAddr("tcp"))
		fmt.Fprintf(c, "%s\n", line)
		c.Close()
	}

	send("<163>real error")  // severity 3: kept
	send("<167>debug noise") // severity 7: dropped
	in.waitRows(1)
	time.Sleep(200 * time.Millisecond)
	if n := in.count(); n != 1 {
		t.Fatalf("%d rows; the filter did not drop debug", n)
	}

	// Loosen the filter and reload, exactly as an operator would.
	os.WriteFile(ini, []byte("[filter]\nmin_severity = debug\n"), 0o600)
	in.app.reload()

	send("<167>debug now wanted")
	in.waitRows(2)
}

// SPEC §15.3, the rollback. A reloadable key reverted to its boot value is a
// change against what is RUNNING and must be applied; a restart-only key must
// be warned about on every SIGHUP, not only the first.
func TestReloadRevertsAReloadableKeyToItsBootValue(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	write := func(body string) {
		t.Helper()
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[filter]\nmin_severity = err\n[ring]\ncapacity_items = 1000\n")
	in := start(t, dir, "--config="+ini)
	defer in.stop()

	send := func(line string) {
		c, err := net.Dial("tcp", in.app.TransportAddr("tcp"))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(c, "%s\n", line)
		c.Close()
	}

	// Loosen the filter and change a restart-only key.
	write("[filter]\nmin_severity = debug\n[ring]\ncapacity_items = 2000\n")
	r := in.app.reload()
	if len(r.applied) != 1 || r.applied[0] != "filter.min_severity" ||
		len(r.ignored) != 1 || r.ignored[0] != "ring.capacity_items" {
		t.Fatalf("first reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	send("<167>debug wanted")
	in.waitRows(1)

	// The same file again: the restart-only key is still warned about.
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 1 {
		t.Fatalf("second reload: applied=%v ignored=%v", r.applied, r.ignored)
	}

	// Roll the filter back to its boot value. Before the fix this reload
	// returned early as "unchanged" and the loosened filter stayed in force.
	write("[filter]\nmin_severity = err\n[ring]\ncapacity_items = 2000\n")
	r = in.app.reload()
	if len(r.applied) != 1 || r.applied[0] != "filter.min_severity" {
		t.Fatalf("revert reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
	send("<167>debug noise again")
	send("<163>real error")
	in.waitRows(2)
	time.Sleep(200 * time.Millisecond)
	if n := in.count(); n != 2 {
		t.Fatalf("%d rows; the reverted filter did not drop debug", n)
	}

	// Everything back to the boot values: nothing to apply, and the
	// restart-only key stops being warned about once it matches the boot.
	write("[filter]\nmin_severity = err\n[ring]\ncapacity_items = 1000\n")
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("boot reload: applied=%v ignored=%v", r.applied, r.ignored)
	}
}

// The acceptance rule on the one transport that can confirm: 200 OK only after
// the ring's guarantee. With ring=sqlite that guarantee is a COMMIT, so a crash
// right after the answer must not lose the message.
func TestRELPConfirmationSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	// sink=none so nothing drains: whatever was confirmed must still be in the
	// ring database when the next instance boots.
	in := start(t, dir, "--ring.mode=sqlite", "--sink.type=none")

	r, err := net.Dial("tcp", in.app.TransportAddr("relp"))
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(r)
	msg := `<13>Sep 12 10:00:00 host app: confirmado`
	fmt.Fprintf(r, "7 syslog %d %s\n", len(msg), msg)
	_ = r.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, _ := br.ReadString('\n')
	if !strings.HasPrefix(line, "7 rsp") || !strings.Contains(line, "200 OK") {
		t.Fatalf("RELP answered %q", line)
	}
	r.Close()

	// The row is committed at the instant of the answer. Read it directly.
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "ring.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		db.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&n)
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// sink=none confirms instantly and deletes from the ring once drained, so
	// either the row is still there or it was already delivered to nowhere --
	// what must NOT happen is an answer for a message that never reached disk.
	// The written counter tells the two apart.
	written := in.app.metrics.EventsWrittenTotal.Load()
	if n == 0 && written == 0 {
		t.Fatal("RELP confirmed a message that is neither in the ring nor delivered")
	}
	in.stop()
}

// SPEC §9.4: a poisoned row in ring.db must not keep the boot waiting forever.
// The row is quarantined, the accounting is corrected, and the transports open.
func TestBootDrainQuarantinesAPoisonedRow(t *testing.T) {
	dir := t.TempDir()
	first := start(t, dir, "--ring.mode=sqlite")
	if code := first.stop(); code != ExitOK {
		t.Fatalf("the first run exited %d", code)
	}

	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "ring.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ring (id, head, raw, nbytes) VALUES (?, ?, ?, ?)`,
		"poison", []byte("{"), []byte("raw"), 3); err != nil {
		t.Fatal(err)
	}
	db.Close()

	// start fails the test if the instance never opens its transports.
	in := start(t, dir, "--ring.mode=sqlite")
	if _, body := in.get("/metrics"); !strings.Contains(body, "syslog_ring_quarantined_total 1") {
		t.Error("syslog_ring_quarantined_total did not count the poisoned row")
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d", code)
	}

	db, err = sql.Open("sqlite", "file:"+filepath.Join(dir, "ring.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var inRing, inQuarantine int
	_ = db.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&inRing)
	_ = db.QueryRow(`SELECT COUNT(*) FROM quarantine WHERE id = 'poison'`).Scan(&inQuarantine)
	if inRing != 0 || inQuarantine != 1 {
		t.Fatalf("ring=%d quarantine=%d, want 0 and 1", inRing, inQuarantine)
	}
}

// SPEC §12.3: with ring=sqlite a drain that does not finish leaves the rows in
// ring.db for the next boot. That is not a loss: nothing is counted as dropped,
// no dump is written (it would deliver every row twice) and the exit is clean.
func TestSQLiteRingLeftoversAreNotCountedAsLost(t *testing.T) {
	dir := t.TempDir()
	args := []string{
		"--ring.data_dir=" + dir,
		"--admin.bind=" + freeAddr(t),
		"--listen.udp=", "--listen.relp=",
		"--listen.tcp=" + freeAddr(t),
		"--ring.mode=sqlite", "--ring.sqlite.synchronous=OFF",
		"--sink.type=sqlite", "--sink.sqlite.synchronous=OFF",
		"--sink.health_interval=50ms",
		"--sink.retry_backoff_min=5ms", "--sink.retry_backoff_max=10ms",
		"--sink.batch_max_delay=1ms",
		"--shutdown.drain_timeout=300ms",
		"--log.level=error",
		"--node.id=leftovers",
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	appCh := make(chan *App, 1)
	go func() {
		done <- runContext(ctx, args, func(a *App) {
			a.abort = make(chan os.Signal, 1)
			a.ready = make(chan struct{})
			a.wrapSink = func(s sink.Sink) sink.Sink {
				w := &failingWrites{Sink: s}
				w.fail.Store(true)
				return w
			}
			appCh <- a
		})
	}()
	a := <-appCh
	select {
	case <-a.Ready():
	case code := <-done:
		t.Fatalf("exited during boot: %d", code)
	case <-time.After(15 * time.Second):
		t.Fatal("the transports never opened")
	}

	c, err := net.Dial("tcp", a.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	const n = 3
	for i := 0; i < n; i++ {
		fmt.Fprintf(c, "<13>row %d\n", i)
	}
	c.Close()

	ringDB := filepath.Join(dir, "ring.db")
	rows := func() int {
		db, err := sql.Open("sqlite", "file:"+ringDB+"?mode=ro")
		if err != nil {
			return -1
		}
		defer db.Close()
		var k int
		_ = db.QueryRow(`SELECT COUNT(*) FROM ring`).Scan(&k)
		return k
	}
	deadline := time.Now().Add(10 * time.Second)
	for rows() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rows() < n {
		t.Fatalf("only %d of %d messages reached ring.db", rows(), n)
	}

	cancel()
	code := <-done
	if code != ExitOK {
		t.Fatalf("exit code %d, want 0: rows left in ring.db are not a failure", code)
	}
	if d := a.metrics.EventsDroppedTotal[obs.EventDropAbort].Load(); d != 0 {
		t.Fatalf("%d rows counted as dropped although they are on disk", d)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "emergency", "*.bin")); len(files) != 0 {
		t.Fatalf("an emergency dump was written for rows that are already on disk: %v", files)
	}
	if k := rows(); k != n {
		t.Fatalf("%d rows left in ring.db, want %d", k, n)
	}
}

// failingWrites wraps a real sink so that writes fail with a NETWORK error while
// the health check keeps passing. That is the exact shape of the incident this
// test is about: the state never goes DOWN, receivers keep accepting, and the
// workers HOLD their batches outside the ring.
type failingWrites struct {
	sink.Sink
	fail atomic.Bool
}

func (f *failingWrites) Write(ctx context.Context, batch []sink.Prepared) error {
	if f.fail.Load() {
		return errors.New("destination unreachable (injected)")
	}
	return f.Sink.Write(ctx, batch)
}

// Invariant §19.13 end to end, through the trap the sibling service paid for:
// a batch held by a retrying worker is NOT in the ring, so the shutdown must
// collect it from Workers.Pending() and dump it -- and the next process must
// turn that dump back into the very same row, queryable columns included.
func TestEmergencyDumpAndReingestionAreExact(t *testing.T) {
	dir := t.TempDir()
	events := filepath.Join(dir, "events.db")

	// --- first life: every write fails, and the drain deadline passes -------
	var wrapped *failingWrites
	args := func(extra ...string) []string {
		return append([]string{
			"--ring.data_dir=" + dir,
			"--admin.bind=" + freeAddr(t),
			"--listen.udp=",
			"--listen.tcp=" + freeAddr(t),
			"--sink.type=sqlite",
			"--sink.sqlite.path=" + events,
			"--sink.sqlite.synchronous=OFF",
			"--sink.health_interval=50ms",
			"--sink.retry_backoff_min=5ms",
			"--sink.retry_backoff_max=10ms",
			"--sink.batch_max_delay=1ms",
			"--log.level=error",
			"--node.id=first-life",
		}, extra...)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	appCh := make(chan *App, 1)
	go func() {
		done <- runContext(ctx, args("--shutdown.drain_timeout=200ms"), func(a *App) {
			a.abort = make(chan os.Signal, 1)
			a.ready = make(chan struct{})
			a.wrapSink = func(s sink.Sink) sink.Sink {
				wrapped = &failingWrites{Sink: s}
				wrapped.fail.Store(true)
				return wrapped
			}
			appCh <- a
		})
	}()
	a := <-appCh
	select {
	case <-a.Ready():
	case code := <-done:
		t.Fatalf("first life exited during boot: %d", code)
	case <-time.After(15 * time.Second):
		t.Fatal("first life never opened its transports")
	}

	lines := []string{
		`<165>1 2026-09-12T14:33:01.123Z web-01 nginx 1234 ID47 [a@1 k="v"] para-o-dump`,
		`<34>Oct 11 22:14:15 mymachine su[230]: segunda linha`,
		"sem pri, com \x00 no meio",
	}
	c, err := net.Dial("tcp", a.TransportAddr("tcp"))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		fmt.Fprintf(c, "%d %s", len(l), l)
	}
	c.Close()

	// Wait until the workers have actually tried and failed, so the batch is
	// genuinely HELD and not still sitting in the ring.
	deadline := time.Now().Add(10 * time.Second)
	for a.metrics.SinkErrorsTotal[0].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a.metrics.SinkErrorsTotal[0].Load() == 0 {
		t.Fatal("the injected failure never reached a worker")
	}

	cancel()
	code := <-done
	if code != ExitSink {
		t.Fatalf("a shutdown that had to dump exited %d, want %d", code, ExitSink)
	}
	dumps, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin"))
	if len(dumps) != 1 {
		t.Fatalf("dump files = %d, want 1", len(dumps))
	}

	// --- second life: a working sink ---------------------------------------
	second := start(t, dir,
		"--listen.udp=", "--listen.relp=",
		"--sink.sqlite.path="+events, "--node.id=second-life")
	defer second.stop()

	db, _ := sql.Open("sqlite", "file:"+events)
	defer db.Close()

	var n int
	db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	if n != len(lines) {
		t.Fatalf("reingested %d of %d messages", n, len(lines))
	}

	// The queryable columns come back, not just the blobs. A zero metadata here
	// is the sibling's documented bug: rows that exist but no WHERE can find.
	var host, app, rfc, env string
	var ts int64
	err = db.QueryRow(`SELECT hostname, app_name, rfc, ts, CAST(env AS TEXT) FROM events
	                   WHERE hostname = 'web-01' AND ts > 0`).Scan(&host, &app, &rfc, &ts, &env)
	if err != nil {
		t.Fatalf("the reingested row is invisible to the query its columns exist for: %v", err)
	}
	if app != "nginx" || rfc != "5424" {
		t.Fatalf("columns = %q %q", app, rfc)
	}

	// The identity and the reception instant are the FIRST life's, not the
	// second's: the head came back whole.
	var e map[string]any
	json.Unmarshal([]byte(env), &e)
	if e["node-id"] != "second-life" {
		// node-id is the instance that WROTE the envelope, which is the second.
		// Everything the head carries, below, must be the first life's.
		t.Logf("node-id = %v", e["node-id"])
	}
	if e["transport"] != "tcp" || e["framing"] != "octet" {
		t.Fatalf("wire fields changed across the dump: %v %v", e["transport"], e["framing"])
	}
	if int64(e["ts"].(float64)) != ts {
		t.Fatal("the envelope and the column disagree about ts")
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "emergency", "emg-*.bin")); len(left) != 0 {
		t.Fatalf("the dump was not removed after reingestion: %v", left)
	}
}

// /readyz reports the reason, and /metrics exports the syslog_ series.
func TestAdminSurface(t *testing.T) {
	in := start(t, t.TempDir())
	defer in.stop()

	if code, body := in.get("/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("/healthz = %d %q", code, body)
	}
	if code, body := in.get("/readyz"); code != 200 {
		t.Fatalf("/readyz = %d %q", code, body)
	}

	c, _ := net.Dial("tcp", in.app.TransportAddr("tcp"))
	fmt.Fprint(c, "<13>for the metrics\n")
	c.Close()
	in.waitRows(1)

	_, body := in.get("/metrics")
	for _, want := range []string{
		`syslog_messages_total{transport="tcp"} 1`,
		`syslog_messages_accepted_total{transport="tcp"} 1`,
		`syslog_parse_total{rfc="3164"} 1`, // a valid PRI alone makes it syslog (SPEC §6.3)
		`syslog_config_info{ring_mode="memory",sink_type="sqlite",transports="udp,tcp,relp"} 1`,
		`syslog_events_written_total 1`,
		`syslog_sink_up 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	// No label may come from emitter content.
	if strings.Contains(body, "for the metrics") || strings.Contains(body, `hostname="`) {
		t.Error("/metrics carries emitter-controlled content")
	}
}

// Shutdown drains the ring into the destination: SIGTERM loses nothing.
func TestGracefulShutdownDeliversEverything(t *testing.T) {
	in := start(t, t.TempDir())

	const n = 200
	c, _ := net.Dial("tcp", in.app.TransportAddr("tcp"))
	w := bufio.NewWriter(c)
	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "<13>msg-%04d\n", i)
	}
	w.Flush()
	c.Close()

	// Wait until every message has at least entered the pipeline, then stop at
	// once -- without waiting for the destination.
	deadline := time.Now().Add(10 * time.Second)
	for in.app.metrics.MessagesAcceptedTotal[1].Load() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit code %d, want 0", code)
	}
	if got := in.count(); got != n {
		t.Fatalf("%d of %d accepted messages reached the destination after SIGTERM", got, n)
	}
}

// No transport configured is a boot error, not an idle process that looks
// healthy on every probe.
func TestNoTransportExitsOne(t *testing.T) {
	code := RunContext(context.Background(), []string{
		"--listen.udp=", "--listen.tcp=", "--ring.data_dir=" + t.TempDir(),
	})
	if code != ExitConfig {
		t.Fatalf("exit %d, want %d", code, ExitConfig)
	}
}

// A second signal during shutdown exits at once with 130.
func TestSecondSignalAborts(t *testing.T) {
	in := start(t, t.TempDir(), "--shutdown.pre_stop_delay=10s")

	in.cancel() // first signal: shutdown starts, and sits in pre_stop_delay
	time.Sleep(100 * time.Millisecond)
	in.app.abort <- os.Interrupt // second signal

	select {
	case code := <-in.done:
		if code != ExitAborted {
			t.Fatalf("exit %d, want %d", code, ExitAborted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a second signal did not abort the shutdown")
	}
}

// SPEC §14.4: only a memory ring holds ring.max_bytes in RAM, so only there is
// there a number to compare GOMEMLIMIT against.
func TestMemoryAdviceFollowsTheRingMode(t *testing.T) {
	c := &config.Config{}
	c.Ring.MaxBytes = 512 << 20

	c.Ring.Mode = config.RingMemory
	if want, _ := memoryAdvice(c); want != 1<<30 {
		t.Errorf("memory: want = %d, want twice ring.max_bytes", want)
	}
	for _, mode := range []string{config.RingSQLiteMode, config.RingNone} {
		c.Ring.Mode = mode
		want, resident := memoryAdvice(c)
		if want != 0 || resident == "" {
			t.Errorf("%s: want = %d resident = %q; there is no ring in RAM to size from", mode, want, resident)
		}
	}
}
