package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/obs"
)

// Boot validation and availability of the real NATS (SPEC §10.7, §11, §13.1,
// §13.3, §12). Every test here needs RELPLOG_TEST_NATS_URL.

// refusedBoot runs an instance whose sink validation must refuse it: exit 3
// well before sink.boot_timeout (a permanent refusal is not retried), with no
// transport ever opened.
func refusedBoot(t *testing.T, id ident.Identity, args []string) {
	t.Helper()
	tcp := freePort(t)
	in := launch(t, id, append(args, "--listen.tcp="+tcp, "--sink.boot_timeout=120s"), nil)
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

// SPEC §10.7, §3.9, invariants §20.17, §20.20, §20.21: the stream is validated,
// never created. Missing, not covering loghub.<space>.syslog, capturing the
// control plane, refusing acknowledgements, or too small for the largest
// possible message -- each is exit 3 at once. In core mode there is no stream,
// but max_payload is still checked.
func TestNATSBootValidationExitsThree(t *testing.T) {
	// A control-plane namespace of this test alone: a stream capturing it can
	// be created without overlapping any other stream on the server.
	ns := "rlnatsctl" + unique()
	argsFor := func(t *testing.T, f *fixture, stream string, extra ...string) []string {
		return append(append(localArgs(t.TempDir()), "--sink.nats.urls="+f.nats.url,
			"--sink.nats.stream="+stream, "--loghub.ctl_namespace="+ns), extra...)
	}
	// bad is a space of the subtest's own -- two streams may not share
	// subjects -- with a stream over it that mut breaks.
	bad := func(t *testing.T, mut func(*jetstream.StreamConfig), spaces ...string) (*fixture, string) {
		f := newSpaces(t)
		if len(spaces) == 0 {
			spaces = []string{f.space}
		}
		return f, f.createStream("RLNATS_APP_BAD_"+unique(), mut, spaces...)
	}
	t.Run("missing stream", func(t *testing.T) {
		f := newSpaces(t)
		name := "RLNATS_MISSING_" + unique()
		refusedBoot(t, f.id, argsFor(t, f, name))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := f.nats.js.Stream(ctx, name); !errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Errorf("the server created the stream it validates (%v)", err)
		}
	})
	t.Run("stream of another space", func(t *testing.T) {
		f, stream := bad(t, nil, "x"+unique())
		refusedBoot(t, f.id, argsFor(t, f, stream))
	})
	for name, ctl := range map[string]string{
		"stream capturing the beacon subject": ns + ".beacon",
		"stream capturing the control plane":  ns + ".*",
	} {
		t.Run(name, func(t *testing.T) {
			f, stream := bad(t, func(c *jetstream.StreamConfig) { c.Subjects = append(c.Subjects, ctl) })
			refusedBoot(t, f.id, argsFor(t, f, stream))
		})
	}
	t.Run("stream that never acknowledges", func(t *testing.T) {
		f, stream := bad(t, func(c *jetstream.StreamConfig) { c.NoAck = true })
		refusedBoot(t, f.id, argsFor(t, f, stream))
	})
	t.Run("largest message above the stream's max_msg_size", func(t *testing.T) {
		f, stream := bad(t, func(c *jetstream.StreamConfig) { c.MaxMsgSize = 64 << 10 })
		refusedBoot(t, f.id, argsFor(t, f, stream))
	})
	t.Run("largest message above max_payload", func(t *testing.T) {
		// 12 x 16 MiB + 8 KiB against the server's max_payload (1 MiB by
		// default, 64 MiB at most; SPEC §3.9). The stream itself is canonical.
		f := newFixture(t)
		refusedBoot(t, f.id, argsFor(t, f, f.stream, "--syslog.max_message_bytes=16MiB"))
	})
	t.Run("core mode: largest message above max_payload", func(t *testing.T) {
		f := newCoreFixture(t)
		refusedBoot(t, f.id, append(localArgs(t.TempDir()), "--sink.nats.urls="+f.nats.url,
			"--sink.nats.mode=core", "--syslog.max_message_bytes=16MiB"))
	})
	// The control: the same arguments against the canonical stream boot and
	// stop cleanly. Without it the cases above would also pass if every boot
	// were refused.
	t.Run("control", func(t *testing.T) {
		f := newFixture(t)
		in := launch(t, f.id, argsFor(t, f, f.stream), nil)
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

// SPEC §10.7, §11, §13.3: NATS goes away and comes back. While it is away the
// sink is DOWN and /readyz says so; the RELP transactions are held -- neither
// answered nor refused (SPEC §5.7) -- and answered 200 once it is back; the
// client reconnects by itself, a beacon goes out on the reconnection, and
// nothing is missing from the stream.
func TestNATSDownThenBackLosesNothingAndBeaconsAgain(t *testing.T) {
	f := newFixture(t)
	proxy := newTCPProxy(t, strings.TrimPrefix(f.nats.url, "nats://"))
	sub := f.beacons("loghub._ctl")

	in := launch(t, f.id, f.args(t.TempDir(),
		"--sink.nats.urls=nats://"+proxy.addr(),
		"--loghub.beacon_interval=0", // only the connection beacons
		"--sink.fail_timeout=300s", "--sink.max_retries=1000000",
		"--sink.op_timeout=1s", "--sink.health_timeout=500ms",
		"--relp.backpressure_timeout=120s"), nil)
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

	if _, ok := nextBeacon(t, sub, f.id.From, 10*time.Second); !ok {
		t.Fatal("no beacon on the first connection")
	}

	sendRELP(t, tcp, lines("before", 10)...)
	waitFor(t, 10*time.Second, "the first ten in the stream", func() bool { return f.count() == 10 })

	proxy.pause()
	waitFor(t, 20*time.Second, "/readyz to fail while NATS is away", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "sink is DOWN")
	})
	rc := dialRELP(t, tcp)
	ids := rc.syslog(lines("during", 10)...)
	waitFor(t, 10*time.Second, "the held transactions read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() >= 20 })
	rc.expectNothing(300 * time.Millisecond) // held: no 200, and no 500

	proxy.resume()
	for i, body := range rc.responses(ids) {
		if body != relpRSPOK {
			t.Errorf("held transaction %d answered %q, want %q once the sink is back", i, body, relpRSPOK)
		}
	}
	rc.close()
	waitFor(t, 60*time.Second, "the recovery", func() bool { code, _ := in.get("/readyz"); return code == 200 })
	waitFor(t, 30*time.Second, "everything in the stream", func() bool { return f.count() == 20 })
	if _, ok := nextBeacon(t, sub, f.id.From, 10*time.Second); !ok {
		t.Error("no beacon on the reconnection (SPEC §11)")
	}
	if got := m.SinkNATSReconnectsTotal.Load(); got < 1 {
		t.Errorf("relplog_sink_nats_reconnects_total = %d after a reconnection", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	got := byMessage(t, f.msgs())
	for _, prefix := range []string{"before", "during"} {
		for i := 0; i < 10; i++ {
			if k := fmt.Sprintf("%s-%04d", prefix, i); !hasKey(got, k) {
				t.Errorf("%s is missing from the stream", k)
			}
		}
	}
	if n := m.RELPResponsesTotal[obs.RELPCode500].Load(); n != 0 {
		t.Errorf("%d answers of 500 while the sink was DOWN; it must hold them (SPEC §5.7)", n)
	}
	if n := m.RELPUnansweredTotal[obs.UnansweredBackpressure].Load(); n != 0 {
		t.Errorf("%d transactions handed back by backpressure inside the timeout", n)
	}
}

func hasKey[V any](m map[string]V, k string) bool { _, ok := m[k]; return ok }

// SPEC §13.3, §12, §18.8 with the real NATS: it goes away for good while
// accepted messages are still in the memory ring. The fuses (fail_timeout,
// max_retries) turn DOWN into FATAL, the ring goes to an emergency dump of valid
// JLH lines, the process exits 3, and the next boot reingests the dump into the
// stream once NATS is back -- the same uuids, the same bytes -- and removes the
// file.
func TestNATSFatalDumpsAndTheNextBootReingests(t *testing.T) {
	f := newFixture(t)
	proxy := newTCPProxy(t, strings.TrimPrefix(f.nats.url, "nats://"))
	dir := t.TempDir()
	args := f.args(dir,
		"--sink.nats.urls=nats://"+proxy.addr(),
		"--sink.fail_timeout=1s", "--sink.max_retries=3",
		"--sink.op_timeout=1s", "--sink.health_timeout=300ms")

	// Batches wait ten seconds to fill, so the accepted messages are still in
	// the ring when the destination goes away.
	first := launch(t, f.id, append(args, "--sink.batch_max_delay=10s"), nil)
	first.waitReady()
	const n = 8
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - dumped-%04d", i)
	}
	sendRELP(t, first.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "every message accepted", func() bool {
		return first.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	proxy.pause()
	if code := first.waitExit(60 * time.Second); code != ExitSink {
		t.Fatalf("exit %d, want %d", code, ExitSink)
	}
	if c := f.count(); c != 0 {
		t.Fatalf("%d messages reached the stream before the outage; the test needs them in the ring", c)
	}
	dumps := dumpFiles(t, first.emgDir(), f.id.From)
	if len(dumps) != 1 {
		t.Fatalf("dump files = %v, want 1", dumps)
	}
	dumped := dumpLines(t, dumps[0])
	if len(dumped) != n {
		t.Fatalf("the dump holds %d of %d accepted messages", len(dumped), n)
	}

	proxy.resume()
	second := launch(t, f.id, args, nil)
	second.waitReady()
	// Reingestion runs before the transports open (SPEC §13.1 step 9): at
	// Ready, every message is already in the stream.
	msgs := f.msgs()
	checkReingested(t, dumped, msgs)
	got := byMessage(t, msgs)
	for i := range lines {
		if k := fmt.Sprintf("dumped-%04d", i); !hasKey(got, k) {
			t.Errorf("%s was lost across the FATAL", k)
		}
	}
	if left := dumpFiles(t, second.emgDir(), f.id.From); len(left) != 0 {
		t.Errorf("the dump was not removed after reingestion: %v", left)
	}
	if code := second.stop(); code != ExitOK {
		t.Errorf("second life exited %d", code)
	}
}
