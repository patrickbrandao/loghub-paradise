package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helpers shared by the suites that run the assembled server against a real
// external destination (mysql_test.go, nats_test.go). Each of those skips
// without its SYSLOG_TEST_* variable; nothing here needs one.

// relpSend sends each line as one RELP transaction on a fresh connection and
// returns the lines the server answered "200 OK" for. That answer is the only
// confirmation syslog has (SPEC §2.3), so it is what every "nothing confirmed
// was lost" check reconciles against.
func relpSend(t *testing.T, addr string, lines []string) []string {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dialling RELP at %s: %v", addr, err)
	}
	defer c.Close()
	br := bufio.NewReader(c)

	var confirmed []string
	for i, l := range lines {
		txnr := i + 1
		if _, err := fmt.Fprintf(c, "%d syslog %d %s\n", txnr, len(l), l); err != nil {
			return confirmed
		}
		_ = c.SetReadDeadline(time.Now().Add(15 * time.Second))
		rsp, err := br.ReadString('\n')
		if err != nil {
			return confirmed
		}
		if strings.HasPrefix(rsp, fmt.Sprintf("%d rsp ", txnr)) && strings.Contains(rsp, "200 OK") {
			confirmed = append(confirmed, l)
		}
	}
	return confirmed
}

// markedLines builds n distinct RFC 5424 lines whose MSG is a marker a test can
// find in the destination's raw line.
func markedLines(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("<165>1 2026-09-15T12:00:00Z web-01 app 42 ID7 [m@1 n=\"%d\"] %s-%04d", i, prefix, i)
	}
	return out
}

// missing returns the entries of want that are not in got.
func missing(want, got []string) []string {
	have := make(map[string]bool, len(got))
	for _, g := range got {
		have[g] = true
	}
	var out []string
	for _, w := range want {
		if !have[w] {
			out = append(out, w)
		}
	}
	return out
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// waitExit waits for an instance that is expected to end on its own.
func (in *instance) waitExit(timeout time.Duration) int {
	in.t.Helper()
	select {
	case code := <-in.done:
		return code
	case <-time.After(timeout):
		in.cancel()
		in.t.Fatal("the instance did not exit")
		return -1
	}
}

// booting is an instance started without waiting for its transports: the boot
// is expected to fail, and the test needs the addresses it would have opened.
type booting struct {
	tcp, relp string
	done      chan int
	cancel    context.CancelFunc
}

func startExpectingExit(t *testing.T, dir string, extra ...string) *booting {
	t.Helper()
	b := &booting{tcp: freeAddr(t), relp: freeAddr(t), done: make(chan int, 1)}
	args := append([]string{
		"--ring.data_dir=" + dir,
		"--admin.bind=" + freeAddr(t),
		"--listen.udp=",
		"--listen.tcp=" + b.tcp,
		"--listen.relp=" + b.relp,
		"--sink.health_interval=50ms",
		"--log.level=error",
		"--node.id=test-node",
	}, extra...)
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go func() {
		b.done <- runContext(ctx, args, func(a *App) { a.abort = make(chan os.Signal, 1) })
	}()
	t.Cleanup(cancel)
	return b
}

// assertShut fails the test when any transport of a failed boot accepted a
// connection: the transports open last, and never in front of a sink that did
// not become usable (SPEC §12.1).
func (b *booting) assertShut(t *testing.T) {
	t.Helper()
	for _, addr := range []string{b.tcp, b.relp} {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			t.Errorf("a transport at %s accepted a connection despite the refused sink", addr)
		}
	}
}

// tcpProxy sits between the server and a destination so a test can take the
// destination away and bring it back without touching the real server, which
// other tests share.
type tcpProxy struct {
	t        *testing.T
	ln       net.Listener
	upstream string

	mu      sync.Mutex
	paused  bool
	closing bool
	conns   map[net.Conn]struct{}
}

func newTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{t: t, ln: ln, upstream: upstream, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		refuse := p.paused || p.closing
		p.mu.Unlock()
		if refuse {
			// Refusing rather than hanging is what a dead destination looks
			// like to a client: a connection error, not a stall.
			_ = c.Close()
			continue
		}
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(client net.Conn) {
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	p.track(client)
	p.track(up)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done

	p.untrack(client)
	p.untrack(up)
	_ = client.Close()
	_ = up.Close()
}

func (p *tcpProxy) track(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns[c] = struct{}{}
}

func (p *tcpProxy) untrack(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.conns, c)
}

// Pause cuts every live connection and refuses new ones.
func (p *tcpProxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

// Resume starts forwarding again.
func (p *tcpProxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
	p.closing = true
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	_ = p.ln.Close()
}

// emergencyFiles lists the dump files a run left behind.
func emergencyFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "emg-") && strings.HasSuffix(e.Name(), ".bin") {
			out = append(out, e.Name())
		}
	}
	return out
}
