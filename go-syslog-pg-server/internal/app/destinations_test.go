package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// Boot validation and availability of the real queue (SPEC §10.5, §13.1,
// §13.3, §12). Every test here needs SYSLOG_TEST_PG_DSN.

// refusedBoot runs an instance whose queue validation must refuse it: exit 3
// well before sink.boot_timeout (a permanent refusal is not retried), with no
// transport ever opened.
func refusedBoot(t *testing.T, args []string) {
	t.Helper()
	tcp := freePort(t)
	in := launch(t, testID, append(args, "--listen.udp=", "--listen.tcp="+tcp, "--sink.boot_timeout=120s"), nil)
	began := time.Now()
	code := in.waitExit(60 * time.Second)
	if code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if took := time.Since(began); took > 20*time.Second {
		t.Errorf("the refusal took %s: a permanent refusal must not wait for boot_timeout", took)
	}
	if c, err := net.DialTimeout("tcp", tcp, 200*time.Millisecond); err == nil {
		c.Close()
		t.Error("a transport is open after a refused boot")
	}
}

// SPEC §10.5, invariant §20.17: the queue is validated, never created. A
// missing table, a missing column or no unique index on (uuid) is exit 3 at
// once; the canonical queue of docker/postgres/inbox.sql boots.
func TestPostgresBootValidationExitsThree(t *testing.T) {
	pg := requirePG(t)
	argsFor := func(t *testing.T, table string) []string {
		return append(localArgs(t.TempDir()), pg.sinkArgs(table)...)
	}
	t.Run("missing table", func(t *testing.T) {
		schema := pg.queue(t, "")
		table := strings.TrimSuffix(schema, ".inbox") + fmt.Sprintf(".missing_%d", time.Now().UnixNano())
		refusedBoot(t, argsFor(t, table))
		var reg *string
		if err := pg.pool.QueryRow(context.Background(), "SELECT to_regclass($1)::text", table).Scan(&reg); err != nil || reg != nil {
			t.Errorf("the server created the table it validates (%v, %v)", show(reg), err)
		}
	})
	for name, alter := range map[string]string{
		"missing column":  "ALTER TABLE $T DROP COLUMN event_nsec",
		"no unique index": "ALTER TABLE $T DROP CONSTRAINT inbox_pk",
	} {
		t.Run(name, func(t *testing.T) {
			refusedBoot(t, argsFor(t, pg.queue(t, alter)))
		})
	}
	// The control: the same arguments against the canonical queue boot and
	// stop cleanly. Without it the cases above would also pass if every boot
	// were refused.
	t.Run("control", func(t *testing.T) {
		in := launch(t, testID, argsFor(t, pg.queue(t, "")), nil)
		in.waitReady()
		if code := in.stop(); code != ExitOK {
			t.Fatalf("exit %d", code)
		}
	})
}

// ------------------------------------------------------------ outage

// tcpProxy stands between the server and a destination, so a test can take
// the destination away and bring it back without touching the real server.
type tcpProxy struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	paused bool
	closed bool
	conns  map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		refuse := p.paused || p.closed
		p.mu.Unlock()
		if refuse {
			// A dead destination looks like a refused connection, not a stall.
			c.Close()
			continue
		}
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(client net.Conn) {
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		client.Close()
		return
	}
	p.mu.Lock()
	if p.paused || p.closed {
		p.mu.Unlock()
		client.Close()
		up.Close()
		return
	}
	p.conns[client], p.conns[up] = struct{}{}, struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { io.Copy(up, client); done <- struct{}{} }()
	go func() { io.Copy(client, up); done <- struct{}{} }()
	<-done
	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, up)
	p.mu.Unlock()
	client.Close()
	up.Close()
}

// pause cuts every live connection and refuses new ones.
func (p *tcpProxy) pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for c := range p.conns {
		c.Close()
	}
}

func (p *tcpProxy) resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *tcpProxy) close() {
	p.mu.Lock()
	p.closed = true
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.ln.Close()
}

// SPEC §10.5, §13.3: PostgreSQL goes away and comes back. While it is away the
// sink is DOWN and /readyz says so; TCP holds what it reads; the pool
// reconnects by itself, and nothing that was accepted is missing from the
// queue.
func TestPostgresDownThenBackLosesNothing(t *testing.T) {
	pg := requirePG(t)
	proxy := newTCPProxy(t, pg.addr())
	table := pg.queue(t, "")
	in := launch(t, testID, append(append(localArgs(t.TempDir()), pg.sinkArgs(table)...),
		"--sink.postgres.addr="+proxy.addr(),
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000",
		"--sink.op_timeout=1s", "--sink.health_timeout=500ms"), nil)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	lines := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - %s-%04d", prefix, i)
		}
		return out
	}

	sendLF(t, tcp, lines("before", 10)...)
	waitFor(t, 10*time.Second, "the first ten in the queue", func() bool { return in.count() == 10 })

	proxy.pause()
	waitFor(t, 20*time.Second, "/readyz to fail while PostgreSQL is away", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "sink is DOWN")
	})
	sendLF(t, tcp, lines("during", 10)...)
	waitFor(t, 10*time.Second, "the held line read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() >= 11 })

	proxy.resume()
	waitFor(t, 60*time.Second, "the recovery", func() bool { code, _ := in.get("/readyz"); return code == 200 })
	waitFor(t, 30*time.Second, "everything in the queue", func() bool { return in.count() == 20 })
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	got := map[string]bool{}
	for _, r := range in.rows() {
		got[checkRow(t, r).Payload.message()] = true
	}
	for _, prefix := range []string{"before", "during"} {
		for i := 0; i < 10; i++ {
			if k := fmt.Sprintf("%s-%04d", prefix, i); !got[k] {
				t.Errorf("%s is missing from the queue", k)
			}
		}
	}
	if d := m.MessagesDroppedTotal[obs.DropSinkDown].Load(); d != 0 {
		t.Errorf("TCP dropped %d messages while the sink was DOWN; it must hold them", d)
	}
}

// SPEC §13.3, §12, §18.8 with the real queue: PostgreSQL goes away for good
// while accepted messages are still in the memory ring. The fuses
// (fail_timeout, max_retries) turn DOWN into FATAL, the ring goes to an
// emergency dump of valid JLH lines, the process exits 3, and the next boot
// reingests the dump into the queue once PostgreSQL is back -- the same uuids,
// the same columns -- and removes the file.
func TestPostgresFatalDumpsAndTheNextBootReingests(t *testing.T) {
	pg := requirePG(t)
	proxy := newTCPProxy(t, pg.addr())
	dir := t.TempDir()
	args := append(append(localArgs(dir), pg.sinkArgs(pg.queue(t, ""))...),
		"--listen.udp=",
		"--sink.postgres.addr="+proxy.addr(),
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--sink.op_timeout=1s", "--sink.health_timeout=300ms")

	// Batches wait ten seconds to fill, so the accepted messages are still in
	// the ring when the destination goes away.
	first := launch(t, testID, append(args, "--sink.batch_max_delay=10s"), nil)
	first.waitReady()
	const n = 8
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - dumped-%04d", i)
	}
	sendLF(t, first.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	proxy.pause()
	if code := first.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if c := first.count(); c != 0 {
		t.Fatalf("%d messages reached the queue before the outage; the test needs them in the ring", c)
	}
	dumps := dumpFiles(t, first.emgDir(), testID.From)
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}

	proxy.resume()
	second := launch(t, testID, args, nil)
	second.waitReady()
	// Reingestion runs before the transports open (SPEC §13.1 step 9): at
	// Ready, every row is already there.
	checkReingested(t, dumped, second.rows())
	got := map[string]bool{}
	for _, r := range second.rows() {
		got[checkRow(t, r).Payload.message()] = true
	}
	for i := range lines {
		if k := fmt.Sprintf("dumped-%04d", i); !got[k] {
			t.Errorf("%s was lost across the FATAL", k)
		}
	}
	if left := dumpFiles(t, second.emgDir(), testID.From); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}
