package receiver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A minimal RELP client for the tests: it speaks the frames librelp speaks
// (open with offers, syslog, close) and reads the server's frames back. It
// never hides what the server sent: every frame is returned as it came.

// relpTestOffers is what librelp 1.11 sends in its open.
const relpTestOffers = "relp_version=0\nrelp_software=librelp,1.11.0,http://librelp.adiscon.com\ncommands=syslog"

type relpClient struct {
	t    *testing.T
	c    net.Conn
	br   *bufio.Reader
	txnr int64
}

// srvFrame is one frame read from the server.
type srvFrame struct {
	txnr int64
	cmd  string
	data string
}

func (f srvFrame) String() string { return fmt.Sprintf("%d %s %q", f.txnr, f.cmd, f.data) }

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func newRELPClient(t *testing.T, c net.Conn) *relpClient {
	t.Helper()
	return &relpClient{t: t, c: c, br: bufio.NewReader(c)}
}

// dialRELP connects and opens the session.
func dialRELP(t *testing.T, addr string) *relpClient {
	t.Helper()
	rc := newRELPClient(t, dial(t, addr))
	rc.open()
	return rc
}

// open sends the open command and requires a 200 with the protocol offers.
func (rc *relpClient) open() {
	rc.t.Helper()
	n := rc.send("open", relpTestOffers)
	f := rc.read()
	if f.txnr != n || f.cmd != "rsp" || !strings.HasPrefix(f.data, "200 OK\n") ||
		!strings.Contains(f.data, "relp_version=0") || !strings.Contains(f.data, "commands=syslog") {
		rc.t.Fatalf("open answered %v", f)
	}
}

// send writes one frame with the next txnr and returns it.
func (rc *relpClient) send(cmd, data string) int64 {
	rc.t.Helper()
	rc.txnr++
	rc.write(frameOf(rc.txnr, cmd, data))
	return rc.txnr
}

// syslog sends one syslog frame per message.
func (rc *relpClient) syslog(msgs ...string) []int64 {
	rc.t.Helper()
	var b strings.Builder
	out := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		rc.txnr++
		b.WriteString(frameOf(rc.txnr, "syslog", m))
		out = append(out, rc.txnr)
	}
	rc.write(b.String())
	return out
}

func (rc *relpClient) write(s string) {
	rc.t.Helper()
	_ = rc.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(rc.c, s); err != nil {
		rc.t.Fatalf("write: %v", err)
	}
}

func frameOf(txnr int64, cmd, data string) string {
	if data == "" {
		return fmt.Sprintf("%d %s 0\n", txnr, cmd)
	}
	return fmt.Sprintf("%d %s %d %s\n", txnr, cmd, len(data), data)
}

// read returns the next frame from the server, failing the test after 5 s.
func (rc *relpClient) read() srvFrame {
	rc.t.Helper()
	f, err := rc.tryRead(5 * time.Second)
	if err != nil {
		rc.t.Fatalf("reading a server frame: %v", err)
	}
	return f
}

// tryRead returns the next frame or the error that ended the read.
func (rc *relpClient) tryRead(d time.Duration) (srvFrame, error) {
	_ = rc.c.SetReadDeadline(time.Now().Add(d))
	return readServerFrame(rc.br)
}

// readServerFrame parses one frame the strict way librelp's client does:
// after DATALEN 0 the trailer must follow at once (relpframe.c).
func readServerFrame(br *bufio.Reader) (srvFrame, error) {
	var f srvFrame
	head, err := br.ReadString(' ')
	if err != nil {
		return f, err
	}
	f.txnr, err = strconv.ParseInt(strings.TrimSuffix(head, " "), 10, 64)
	if err != nil {
		return f, fmt.Errorf("bad txnr %q", head)
	}
	cmd, err := br.ReadString(' ')
	if err != nil {
		return f, err
	}
	f.cmd = strings.TrimSuffix(cmd, " ")
	var n int
	for {
		c, err := br.ReadByte()
		if err != nil {
			return f, err
		}
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
			continue
		}
		if n == 0 {
			if c != '\n' {
				return f, fmt.Errorf("DATALEN 0 followed by %q, not the trailer: librelp refuses that frame", c)
			}
			return f, nil
		}
		if c != ' ' {
			return f, fmt.Errorf("DATALEN followed by %q", c)
		}
		break
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(br, data); err != nil {
		return f, err
	}
	if c, err := br.ReadByte(); err != nil || c != '\n' {
		return f, fmt.Errorf("missing trailer (%v)", err)
	}
	f.data = string(data)
	return f, nil
}

// responses reads n rsp frames and returns their bodies by txnr.
func (rc *relpClient) responses(n int) map[int64]string {
	rc.t.Helper()
	out := map[int64]string{}
	for len(out) < n {
		f := rc.read()
		if f.cmd != "rsp" {
			rc.t.Fatalf("expected rsp, got %v (after %d of %d)", f, len(out), n)
		}
		out[f.txnr] = f.data
	}
	return out
}

// expect reads one frame and requires an rsp for txnr whose body starts with
// prefix.
func (rc *relpClient) expect(txnr int64, prefix string) {
	rc.t.Helper()
	f := rc.read()
	if f.cmd != "rsp" || f.txnr != txnr || !strings.HasPrefix(f.data, prefix) {
		rc.t.Fatalf("got %v, want rsp %d %q...", f, txnr, prefix)
	}
}

// expectNothing requires that no frame arrives for d.
func (rc *relpClient) expectNothing(d time.Duration) {
	rc.t.Helper()
	if f, err := rc.tryRead(d); err == nil {
		rc.t.Fatalf("unexpected frame %v", f)
	} else if !isTimeout(err) {
		rc.t.Fatalf("the connection ended: %v", err)
	}
	// A timed-out bufio read may leave a partial frame behind; the tests that
	// call this read nothing else from a half frame.
}

// expectServerClose requires the serverclose hint and then the end of the
// connection.
func (rc *relpClient) expectServerClose() {
	rc.t.Helper()
	f := rc.read()
	if f.txnr != 0 || f.cmd != "serverclose" || f.data != "" {
		rc.t.Fatalf("got %v, want 0 serverclose 0", f)
	}
	rc.expectEOF()
}

// expectEOF requires the server to close the connection with nothing more.
func (rc *relpClient) expectEOF() {
	rc.t.Helper()
	f, err := rc.tryRead(5 * time.Second)
	if err == nil {
		rc.t.Fatalf("got %v, want the connection closed", f)
	}
	if isTimeout(err) {
		rc.t.Fatal("the server did not close the connection")
	}
}

// expectClosed waits for the server to close a connection, ignoring anything
// it sends first.
func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 256)
	for {
		_, err := c.Read(buf)
		if err == nil {
			continue
		}
		if isTimeout(err) {
			t.Fatal("the server did not close the connection")
		}
		return
	}
}

// startTCP opens RELP in clear on a loopback port with short timeouts.
func startTCP(t *testing.T, h *harness, cfg TCPConfig) *Stream {
	t.Helper()
	tr, _ := startTCPCtx(t, h, cfg)
	return tr
}

// startTCPCtx is startTCP that also returns the cancel of the receive
// context, for the tests that drive a shutdown by hand.
func startTCPCtx(t *testing.T, h *harness, cfg TCPConfig) (*Stream, context.CancelFunc) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	if cfg.RELP.BackpressureTimeout == 0 {
		cfg.RELP.BackpressureTimeout = 2 * time.Second
	}
	tr, err := NewTCP(context.Background(), cfg, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); tr.Close() })
	tr.Start(ctx)
	return tr, cancel
}
