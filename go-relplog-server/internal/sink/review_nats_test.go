package sink

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
)

// The fixes of SPEC §22 pendências 13 to 20 in the NATS sink, ported from
// go-relplog-server (which took them, with adaptation, from
// go-syslog-nats-server and go-webhook-nats-server), and the findings of
// go-relplog-nats-server's review of 2026-10-13. Nothing here needs a server
// except the tests that say so; those against a server with limited
// permissions skip without RELPLOG_TEST_NATS_PERM_URL (see
// natsPermSetup).

// ------------------------------------------------------------- fake server

// natsFake is just enough of a NATS server to see what a client sends and to
// answer it the way a real one would: it greets every connection with its
// INFO, answers PING with PONG, records the subject of every PUB/HPUB, and
// optionally answers each publish with an -ERR (pubReply) or, when it carries
// a reply subject, with a JetStream PubAck (acks).
type natsFake struct {
	addr  string
	first chan []byte
	pubs  atomic.Int64

	mu       sync.Mutex
	info     string
	pubReply string
	acks     bool
	// silent leaves every PING unanswered: a flush then waits for a PONG
	// that never comes, until the connection drops.
	silent bool
	// replyFor, when set, answers a publish to subject with that -ERR line
	// (or nothing, for ""), in place of pubReply.
	replyFor func(subject string) string
	// ackErr, when set, turns the ack of the n-th acknowledged publish
	// (from 1) into that JetStream API error JSON (or a normal ack, for "").
	ackErr func(n int) string
	// noResponders answers every publish that has a reply subject with the
	// 503 status a real server sends when no stream listens.
	noResponders bool
	// tlsConf, when set, makes the fake a TLS server: right after the INFO
	// (which should then say tls_required) it runs the server side of the
	// handshake, as nats-server does, and speaks the protocol inside it.
	tlsConf *tls.Config
	// refuse hangs up every new connection before the INFO: a server that
	// is down, for a client that keeps dialing it.
	refuse   bool
	conns    []net.Conn
	subjects []string
}

func startNATSFake(t *testing.T, info string) *natsFake {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &natsFake{addr: ln.Addr().String(), first: make(chan []byte, 16), info: info}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func natsFakeINFO(extra string) string {
	return `{"server_id":"FAKE","server_name":"fake","version":"2.14.6","proto":1,"host":"127.0.0.1",` +
		`"port":4222,"headers":true,"max_payload":1048576` + extra + `}`
}

func (f *natsFake) set(fn func(f *natsFake)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// drop hangs up every connection, so a client reconnects.
func (f *natsFake) drop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.conns = nil
}

func (f *natsFake) published() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.subjects...)
}

func (f *natsFake) serve(c net.Conn) {
	defer c.Close()
	f.mu.Lock()
	if f.refuse {
		f.mu.Unlock()
		return
	}
	f.conns = append(f.conns, c)
	info, tlsConf := f.info, f.tlsConf
	f.mu.Unlock()
	if _, err := c.Write([]byte("INFO " + info + "\r\n")); err != nil {
		return
	}
	if tlsConf != nil {
		tc := tls.Server(c, tlsConf)
		if err := tc.Handshake(); err != nil {
			return
		}
		c = tc
	}
	rd := bufio.NewReader(c)
	subs := map[string]string{} // subject of each SUB -> its sid
	var seq int
	first := true
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		if first {
			select {
			case f.first <- []byte(line):
			default:
			}
			first = false
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "SUB":
			if len(fields) >= 3 {
				subs[fields[1]] = fields[len(fields)-1]
			}
		case "PING":
			f.mu.Lock()
			silent := f.silent
			f.mu.Unlock()
			if silent {
				continue
			}
			if _, err := c.Write([]byte("PONG\r\n")); err != nil {
				return
			}
		case "PUB", "HPUB":
			// PUB <subject> [reply] <size>, HPUB <subject> [reply] <hdr> <total>:
			// the payload is read whole, so its bytes are never taken for a
			// protocol line.
			size, _ := strconv.Atoi(fields[len(fields)-1])
			if _, err := io.ReadFull(rd, make([]byte, size+2)); err != nil {
				return
			}
			f.pubs.Add(1)
			reply := ""
			if (fields[0] == "PUB" && len(fields) == 4) || (fields[0] == "HPUB" && len(fields) == 5) {
				reply = fields[2]
			}
			f.mu.Lock()
			f.subjects = append(f.subjects, fields[1])
			pubReply, acks, ackErr, noResp := f.pubReply, f.acks, f.ackErr, f.noResponders
			if f.replyFor != nil {
				pubReply = f.replyFor(fields[1])
			}
			f.mu.Unlock()
			if pubReply != "" {
				// A real server's -ERR for a denied publish, and nothing else:
				// the message is never routed, so no ack follows.
				if _, err := c.Write([]byte(pubReply)); err != nil {
					return
				}
				continue
			}
			if noResp && reply != "" {
				sid := subs[reply]
				if i := strings.LastIndexByte(reply, '.'); sid == "" && i > 0 {
					sid = subs[reply[:i]+".*"]
				}
				if sid != "" {
					const status = "NATS/1.0 503\r\n\r\n"
					if _, err := fmt.Fprintf(c, "HMSG %s %s %d %d\r\n%s\r\n", reply, sid,
						len(status), len(status), status); err != nil {
						return
					}
				}
				continue
			}
			if acks && reply != "" {
				sid := subs[reply]
				if i := strings.LastIndexByte(reply, '.'); sid == "" && i > 0 {
					// The async publish mux listens on _INBOX.<nuid>.*.
					sid = subs[reply[:i]+".*"]
				}
				if sid != "" {
					seq++
					ack := fmt.Sprintf(`{"stream":"LOGHUB_SYSLOG","seq":%d}`, seq)
					if ackErr != nil {
						if e := ackErr(seq); e != "" {
							ack = e
						}
					}
					if _, err := fmt.Fprintf(c, "MSG %s %s %d\r\n%s\r\n", reply, sid, len(ack), ack); err != nil {
						return
					}
				}
			}
		}
	}
}

// natsLogBuf is a bytes.Buffer a logger and the test can share.
type natsLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *natsLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *natsLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeSink is a sink against srv in mode, logging into buf when given.
func fakeSink(t *testing.T, srv *natsFake, mode string, buf *natsLogBuf, mut func(*NATSConfig)) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: []string{"nats://" + srv.addr}, Mode: mode, Stream: "LOGHUB_SYSLOG",
		Workspace: "default", ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second}
	if mode == NATSModeCore {
		cfg.Stream = ""
	}
	if mut != nil {
		mut(&cfg)
	}
	log := discard()
	if buf != nil {
		log = slog.New(slog.NewTextHandler(buf, nil))
	}
	n, err := NewNATS(cfg, log, metrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// holdAsyncErrors delays the client's ErrorHandler, so only a synchronous read
// of LastError can produce the verdict in time: with the handler racing the
// round trip, a path that skipped the read passed now and then.
func holdAsyncErrors(n *NATS) {
	n.opts = append(n.opts, func(o *nats.Options) error {
		async := o.AsyncErrorCB
		o.AsyncErrorCB = func(c *nats.Conn, s *nats.Subscription, err error) {
			time.Sleep(2 * time.Second)
			async(c, s, err)
		}
		return nil
	})
}

// ------------------------------------------------------------- item 13

// §22 pendência 13: a token is the whole user part of a URL (LOGHUB_EB_URL accepts
// nats://<token>@host, SPEC §10.9), and nats.go's ConnectedUrlRedacted hides
// only a password: the token was printed in the "nats connected" INFO of every
// boot and the WARN of every reconnect. The password URL is the control that
// the log lines are there and still name the server.
func TestNATSConnectLogsNoCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, u := range []string{"nats://s3cr3tTok@", "nats://bob:s3cr3tPw@"} {
		srv := startNATSFake(t, natsFakeINFO(""))
		var buf natsLogBuf
		n := fakeSink(t, srv, NATSModeCore, &buf, func(c *NATSConfig) { c.URLs = []string{u + srv.addr} })
		if err := n.Health(ctx); err != nil {
			t.Fatalf("Health against the fake: %v", err)
		}
		srv.drop()
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(buf.String(), "nats reconnected") && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		n.Close()
		out := buf.String()
		for _, want := range []string{"nats connected", "nats reconnected", "url=nats://" + srv.addr} {
			if !strings.Contains(out, want) {
				t.Errorf("%s: the log lacks %q:\n%s", u, want, out)
			}
		}
		if strings.Contains(out, "s3cr3t") {
			t.Errorf("%s: a credential reached the log:\n%s", u, out)
		}
	}
}

// ------------------------------------------------------------- item 14

func permViolation(s string) error { return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, s) }

// §22 pendência 14: which denials are a verdict on the data path. The text is
// what nats.go hands the ErrorHandler for a server's -ERR.
func TestNATSDeniesTheDataPath(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{permViolation(`Permissions Violation for Publish to "loghub.default.syslog"`), true},
		{permViolation(`Permissions Violation for Publish to "loghub.acme.prod.syslog"`), true},
		{permViolation(`Permissions Violation for Publish to "loghub.default.syslog.authpriv.warning"`), true},
		{permViolation(`Permissions Violation for Publish to "loghub.a.b.syslog.kern.emerg"`), true},
		{permViolation(`Permissions Violation for Publish to "$JS.API.STREAM.INFO.LOGHUB_SYSLOG"`), true},
		{permViolation(`Permissions Violation for Subscription to "_INBOX.Ab12Cd.*"`), true},
		{permViolation(`Permissions Violation for Subscription to "_INBOX.x" using queue "q"`), true},
		// The beacon is the control plane: never a verdict on the data,
		// with the default namespace or one that ends like a data subject.
		{permViolation(`Permissions Violation for Publish to "loghub._ctl.beacon"`), false},
		{permViolation(`Permissions Violation for Publish to "loghub.ops.syslog.beacon"`), false},
		{permViolation(`Permissions Violation for Publish to "loghub.ops.syslog.kern.beacon"`), false},
		// Not the shape of a data subject.
		{permViolation(`Permissions Violation for Publish to "loghub.default.syslog.nope.info"`), false},
		{permViolation(`Permissions Violation for Publish to "loghub.syslog"`), false},
		{permViolation(`Permissions Violation for Publish to "other.thing"`), false},
		// A subscription that is not this sink's.
		{permViolation(`Permissions Violation for Subscription to "loghub.default.syslog"`), false},
		// The text without the sentinel, and other errors.
		{errors.New(`Permissions Violation for Publish to "loghub.default.syslog"`), false},
		{nats.ErrTimeout, false},
		{nil, false},
	} {
		if got := deniesTheDataPath(tc.err); got != tc.want {
			t.Errorf("deniesTheDataPath(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// §22 pendência 14: a denial on the data path reaches this sink only
// asynchronously, so nothing ever turned FATAL and the ring filled behind a
// sink that looked UP. The verdict is kept and returned by Health and by every
// Write, which then publishes nothing; a denial on the beacon's subject is
// not one.
func TestNATSVerdictIsReturnedByHealthAndWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	if err := n.Health(ctx); err != nil {
		t.Fatal(err)
	}
	n.asyncError(permViolation(`Permissions Violation for Publish to "loghub._ctl.beacon"`))
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health after a beacon denial = %v: the control plane took the data down", err)
	}
	if err := n.Write(ctx, batchN(t, 2)); err != nil {
		t.Fatalf("control: Write after a beacon denial = %v", err)
	}
	sent := srv.pubs.Load()

	n.asyncError(permViolation(`Permissions Violation for Subscription to "_INBOX.abc.*"`))
	if err := n.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "permissions violation") {
		t.Fatalf("Health after a data-path denial = %v, want fatal naming it", err)
	}
	if err := n.Write(ctx, batchN(t, 1)); !IsFatal(err) {
		t.Fatalf("Write after the verdict = %v, want fatal", err)
	}
	if got := srv.pubs.Load(); got != sent {
		t.Errorf("Write after the verdict published %d messages, want none", got-sent)
	}
}

// §22 pendência 14, core mode: a denied publish never fails PublishMsg. The -ERR
// comes before the flush's PONG, so the Write that met it reads it from
// LastError and is already fatal -- with the ErrorHandler held back, nothing
// else could make it so. Before, the chunk was confirmed, counted as written
// and released: lost, as was every chunk until a health check.
func TestNATSCoreDeniedPublishIsFatalAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.syslog\"'\r\n"
	})
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health before any publish: %v", err)
	}
	if err := n.Write(ctx, batchN(t, 3)); !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Write of a denied chunk = %v (class %s), want fatal naming the denial", err, ClassOf(err))
	}
	if got := srv.pubs.Load(); got != 3 {
		t.Fatalf("the fake saw %d publishes, want the 3 of the chunk", got)
	}
}

// §22 pendência 14, jetstream mode: a denied publish never gets an ack, so the
// futures time out -- network, retried forever. The -ERR already sits in
// LastError when they do, and the Write returns the verdict, fatal and still
// tainting; the ErrorHandler is held back so only that read can do it.
func TestNATSJetStreamDeniedPublishIsFatalAfterTheTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.syslog\"'\r\n"
	})
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.OpTimeout = 500 * time.Millisecond })
	holdAsyncErrors(n)
	err := n.Write(ctx, batchN(t, 2))
	if !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Write of a denied chunk = %v (class %s), want fatal naming the denial", err, ClassOf(err))
	}
	if !taints(err) {
		t.Error("the verdict lost the taint: futures of the chunk may still be pending")
	}
}

// ------------------------------------------------------------- item 15

// §22 pendência 15: a message the client refuses before it leaves (ErrMaxPayload:
// a reconnection to a node with a smaller max_payload, a dump line from a life
// with a larger syslog.max_message_bytes) is refused alone, like a 10054: the
// rest of the chunk is published and Delivered says which. It ended the chunk:
// the later envelopes were never published yet confirmed, and the earlier
// ones, stored, were counted lost.
func TestNATSMaxPayloadMidChunkIsRefusedAlone(t *testing.T) {
	cc := convert.DefaultConfig()
	cc.MaxMessageBytes = 64 << 10
	for _, mode := range []string{NATSModeJetStream, NATSModeCore} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			srv := startNATSFake(t, strings.Replace(natsFakeINFO(""), "1048576", "8192", 1))
			srv.set(func(f *natsFake) { f.acks = true })
			n := fakeSink(t, srv, mode, nil, nil)
			b := batchWith(t, cc, newItem("<13>small zero"), newItem("<13>"+strings.Repeat("x", 10000)),
				newItem("<13>small two"))
			err := n.Write(ctx, b)
			if ClassOf(err) != ClassLogic || !errors.Is(err, nats.ErrMaxPayload) {
				t.Fatalf("Write = %v (class %s), want logic for the oversized message", err, ClassOf(err))
			}
			var e *Error
			if !errors.As(err, &e) || fmt.Sprint(e.Delivered) != "[0 2]" {
				t.Fatalf("Delivered = %v, want [0 2]", e.Delivered)
			}
			if stored, lost := settleLogic(err, b.Envelopes); stored != 2 || len(lost) != 1 ||
				lost[0] != b.Envelopes[1].UUID() {
				t.Fatalf("settled as %d stored, lost %v", stored, lost)
			}
			// The message after the refused one did leave.
			if got := srv.pubs.Load(); got != 2 {
				t.Fatalf("the fake saw %d publishes, want 2", got)
			}
		})
	}
}

// ------------------------------------------------------------- item 16

// §22 pendência 16, core mode: max_payload is checked on every health check, not
// once. A reconnection may land on a server with a smaller one, and every
// larger message would then be refused by the client, chunk after chunk, with
// the sink UP.
func TestNATSCoreMaxPayloadIsCheckedOnEveryCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, func(c *NATSConfig) { c.MaxEventBytes = MaxEventBytes(64 << 10) })
	if err := n.Health(ctx); err != nil {
		t.Fatalf("Health with a max_payload that fits: %v", err)
	}
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: a second check against the same server: %v", err)
	}
	back := make(chan struct{}, 4)
	n.OnConnect(func() { back <- struct{}{} })
	<-back // registered while connected: it runs once at once
	srv.set(func(f *natsFake) { f.info = strings.Replace(f.info, "1048576", "65536", 1) })
	srv.drop()
	select {
	case <-back:
	case <-ctx.Done():
		t.Fatal("the client never reconnected")
	}
	if err := n.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "max_payload=65536") {
		t.Fatalf("Health after reconnecting to a smaller max_payload = %v, want fatal naming it", err)
	}
}

// ------------------------------------------------------------- item 17

// §22 pendência 17 (decided 2026-10-11): retention=interest acknowledges and keeps
// nothing while no consumer is interested, so a confirmed chunk could be lost:
// refused, fatal. workqueue and limits are the controls. max_msgs_per_subject
// is only warned about, once per value, though validate runs on every check.
func TestNATSValidateRetentionAndPerSubjectCap(t *testing.T) {
	ok := jetstream.StreamConfig{Name: "LOGHUB_SYSLOG", Subjects: []string{"loghub.default.syslog"}}
	n := newTestNATS(t, nil)
	for _, r := range []jetstream.RetentionPolicy{jetstream.LimitsPolicy, jetstream.WorkQueuePolicy} {
		cfg := ok
		cfg.Retention = r
		if err := n.validate(1<<20, &jetstream.StreamInfo{Config: cfg}); err != nil {
			t.Errorf("retention=%s refused: %v", r, err)
		}
	}
	interest := ok
	interest.Retention = jetstream.InterestPolicy
	if err := n.validate(1<<20, &jetstream.StreamInfo{Config: interest}); !IsFatal(err) ||
		!strings.Contains(err.Error(), "retention=interest") {
		t.Errorf("retention=interest = %v, want fatal naming it", err)
	}

	var buf natsLogBuf
	w, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_SYSLOG", Workspace: "default"},
		slog.New(slog.NewTextHandler(&buf, nil)), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	check := func(perSubject int64) {
		t.Helper()
		cfg := ok
		cfg.MaxMsgsPerSubject = perSubject
		if err := w.validate(1<<20, &jetstream.StreamInfo{Config: cfg}); err != nil {
			t.Fatalf("max_msgs_per_subject=%d refused: %v", perSubject, err)
		}
	}
	warnings := func() int { return strings.Count(buf.String(), "max_msgs_per_subject=") }
	check(-1)
	check(0)
	if warnings() != 0 {
		t.Fatalf("a warning without a per-subject cap:\n%s", buf.String())
	}
	check(10)
	check(10)
	check(10)
	if warnings() != 1 {
		t.Fatalf("%d warnings for one value over three checks, want 1:\n%s", warnings(), buf.String())
	}
	check(20)
	check(-1)
	check(20)
	if warnings() != 3 {
		t.Fatalf("%d warnings, want 3 (10, 20, and 20 again after it was lifted):\n%s", warnings(), buf.String())
	}
}

// ------------------------------------------------------------- item 20

// §22 pendência 20: core mode publishes a dump of another space with no stream to
// ask whether anyone listens: lost unless someone subscribes there, and the
// dump removed as delivered. It is said once per foreign space, never per
// message, and never for this instance's own space.
func TestNATSCoreWarnsOnceAboutAForeignSpace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	var buf natsLogBuf
	n := fakeSink(t, srv, NATSModeCore, &buf, nil)
	if err := n.Write(ctx, batchN(t, 2)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "another space") {
		t.Fatalf("a warning for the own space:\n%s", buf.String())
	}
	for range 3 {
		if err := n.Write(ctx, batchFrom(t, newItemIn("acme", line5424), newItemIn("acme", line3164))); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(buf.String(), "subject=loghub.acme.syslog"); got != 1 {
		t.Errorf("%d warnings for the foreign space, want 1:\n%s", got, buf.String())
	}
	// Control: the foreign envelopes did go out, on their own subject.
	if got := strings.Count(strings.Join(srv.published(), " "), "loghub.acme.syslog"); got != 6 {
		t.Errorf("%d publishes on the foreign subject, want 6", got)
	}
}

// §22 pendência 20: the own space is the hot path, and finding its subject --
// with the comparison that tells a foreign space apart -- must not cost an
// allocation per envelope: the subject strings are built once.
func TestNATSOwnSubjectDoesNotAllocate(t *testing.T) {
	for _, mode := range []string{NATSSubjectsExact, NATSSubjectsFacilitySeverity} {
		n := newTestNATS(t, func(c *NATSConfig) { c.Subjects = mode })
		b := batchOf(t, line5424)
		e := &b.Envelopes[0]
		if allocs := testing.AllocsPerRun(1000, func() { _ = n.subject(e) }); allocs != 0 {
			t.Errorf("%s: %v allocations per subject of the own space, want 0", mode, allocs)
		}
	}
}

// -------------------------------------------- against a real server (14, 15, 16, 17, 19)

// §22 pendência 16: the stream contract holds on every health check, not only at
// the boot. A stream edited underneath answered every publish with a timeout
// or a refusal -- network, retried forever, or logic, dropped -- while Health,
// which saw the stream still there, kept the sink UP.
func TestNATSStreamEditedAtRuntimeIsFatal(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(f *natsFixture, c *jetstream.StreamConfig)
		want string
	}{
		"subjects narrowed": {func(_ *natsFixture, c *jetstream.StreamConfig) { c.Subjects = c.Subjects[1:] },
			"does not cover"},
		"no_ack turned on":     {func(_ *natsFixture, c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
		"max_msg_size lowered": {func(_ *natsFixture, c *jetstream.StreamConfig) { c.MaxMsgSize = 1024 }, "max_msg_size"},
		"control plane captured": {func(f *natsFixture, c *jetstream.StreamConfig) {
			c.Subjects = append(c.Subjects, f.ns+".>")
		}, "control plane"},
	} {
		t.Run(name, func(t *testing.T) {
			f := natsSetup(t)
			cfg := f.cfg
			cfg.MaxEventBytes = 4096
			p := f.sink(cfg)
			f.ready(p)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Control: a second check of the untouched stream still passes.
			if err := p.Health(ctx); err != nil {
				t.Fatalf("second Health of the untouched stream: %v", err)
			}
			sc := natsStreamConfigFor(t, f.stream, f.space)
			tc.mut(f, &sc)
			if _, err := f.js.UpdateStream(ctx, sc); err != nil {
				t.Fatalf("update: %v", err)
			}
			if err := p.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Health after the edit = %v, want fatal naming %q", err, tc.want)
			}
		})
	}
}

// §22 pendência 16, the other half: checking the contract on every health check
// must not start to require the subject of another space a reingested dump
// published on. Once that dump drained, narrowing the stream back to the own
// space -- what the operator may do -- is not a FATAL.
func TestNATSForeignSubjectIsNotPartOfTheContract(t *testing.T) {
	f := natsSetup(t)
	other := fmt.Sprintf("o%d", time.Now().UnixNano())
	space, stream := f.newStream(func(c *jetstream.StreamConfig) {
		c.Subjects = append(c.Subjects, "loghub."+other+".syslog")
	})
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, space
	p := f.sink(cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, batchFrom(t, f.items(other, 2)...)); err != nil {
		t.Fatalf("Write of the foreign space: %v", err)
	}
	if n := f.count(stream); n != 2 {
		t.Fatalf("%d messages, want 2", n)
	}
	sc := natsStreamConfigFor(t, stream, space)
	if _, err := f.js.UpdateStream(ctx, sc); err != nil {
		t.Fatalf("narrowing the stream: %v", err)
	}
	if err := p.Health(ctx); err != nil {
		t.Fatalf("Health after the foreign subject left the stream = %v, want it healthy", err)
	}
	if err := p.Write(ctx, batchFrom(t, f.items(space, 1)...)); err != nil {
		t.Errorf("control: Write of the own space after the narrowing: %v", err)
	}
}

// §22 pendência 15 against a real server: a message above its max_payload is
// refused by the client before it leaves, alone, in both modes, and
// Delivered says exactly which were published.
func TestNATSOversizedMessageIsRefusedAlone(t *testing.T) {
	f := natsSetup(t)
	if f.nc.MaxPayload() > 4<<20 {
		t.Skipf("max_payload %d: the test needs a server below 4 MiB", f.nc.MaxPayload())
	}
	cc := convert.DefaultConfig()
	cc.MaxMessageBytes = 8 << 20
	big := "<13>" + strings.Repeat("x", int(f.nc.MaxPayload())+1024)
	for _, mode := range []string{NATSModeJetStream, NATSModeCore} {
		t.Run(mode, func(t *testing.T) {
			space, stream := f.newStream(nil)
			cfg := f.cfg
			cfg.Stream, cfg.Workspace, cfg.Mode = stream, space, mode
			p := f.sink(cfg)
			var sub *nats.Subscription
			if mode == NATSModeCore {
				var err error
				if sub, err = f.nc.SubscribeSync("loghub." + space + ".syslog"); err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				if err := f.nc.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			b := batchWith(t, cc, newItemIn(space, "<13>small zero"), newItemIn(space, big),
				newItemIn(space, "<13>small two"))
			err := p.Write(context.Background(), b)
			if ClassOf(err) != ClassLogic {
				t.Fatalf("Write = %v (class %s), want logic", err, ClassOf(err))
			}
			var e *Error
			if !errors.As(err, &e) || fmt.Sprint(e.Delivered) != "[0 2]" {
				t.Fatalf("Delivered = %v, want [0 2]", e.Delivered)
			}
			if mode == NATSModeJetStream {
				if n := f.count(stream); n != 2 {
					t.Fatalf("%d messages stored, want 2", n)
				}
				return
			}
			for i := 0; i < 2; i++ {
				if _, err := sub.NextMsg(5 * time.Second); err != nil {
					t.Fatalf("core subscriber got %d of 2 messages: %v", i, err)
				}
			}
		})
	}
}

// §22 pendência 17 against a real server: a stream with retention=interest and no
// consumer acknowledges a publish and keeps nothing -- shown here, so the
// refusal is about a real loss -- and Health refuses it, fatal.
func TestNATSInterestStreamIsRefused(t *testing.T) {
	f := natsSetup(t)
	space, stream := f.newStream(func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.Publish(ctx, "loghub."+space+".syslog", []byte("{}")); err != nil {
		t.Fatalf("control: the interest stream refused a publish: %v", err)
	}
	if n := f.count(stream); n != 0 {
		t.Fatalf("control: the interest stream kept %d messages with no consumer", n)
	}
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, space
	if err := f.sink(cfg).Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "retention=interest") {
		t.Fatalf("Health of an interest stream = %v, want fatal naming it", err)
	}
}

// natsPermSetup addresses a JetStream server whose users have limited
// permissions, by RELPLOG_TEST_NATS_PERM_URL (no user part), and creates a
// stream for a fresh space as its admin. The server's configuration:
//
//	listen: 127.0.0.1:14274
//	jetstream { store_dir: "/tmp/nats-perm" }
//	authorization { users = [
//	  { user: admin, password: admin }
//	  { user: nopub, password: nopub, permissions: {
//	      publish: { allow: [">"], deny: ["loghub.*.syslog", "loghub.*.syslog.>"] },
//	      subscribe: { allow: [">"] } } }
//	  { user: noinbox, password: noinbox, permissions: {
//	      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
//	  { user: nobeacon, password: nobeacon, permissions: {
//	      publish: { allow: [">"], deny: ["*.beacon"] }, subscribe: { allow: [">"] } } }
//	] }
func natsPermSetup(t *testing.T) (url, space, stream string) {
	t.Helper()
	url = strings.TrimSpace(os.Getenv("RELPLOG_TEST_NATS_PERM_URL"))
	if url == "" {
		t.Skip("set RELPLOG_TEST_NATS_PERM_URL to a JetStream server with the users of natsPermSetup")
	}
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin"), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("RELPLOG_TEST_NATS_PERM_URL is not reachable as admin: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	n := time.Now().UnixNano()
	space, stream = fmt.Sprintf("p%d", n), fmt.Sprintf("SLS_PERM_%d", n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, natsStreamConfigFor(t, stream, space)); err != nil {
		t.Fatalf("creating stream %s: %v", stream, err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), stream) })
	return url, space, stream
}

func permSink(t *testing.T, url, user, space, stream, mode string) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: []string{url}, Username: user, Password: user, Mode: mode, Stream: stream,
		Workspace: space, CtlNamespace: "sls_perm_ctl", ConnectTimeout: 5 * time.Second, OpTimeout: time.Second}
	if mode == NATSModeCore {
		cfg.Stream = ""
	}
	p, err := NewNATS(cfg, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// §22 pendência 14 against a real server: a role denied the data subject, or the
// _INBOX the acks and the API's answers come back on, is a permanent verdict
// -- fatal from Write and Health -- in both modes; a role denied only the
// beacon's subject keeps the data path healthy.
func TestNATSPermissionDeniedIsFatal(t *testing.T) {
	url, space, stream := natsPermSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("publish denied, jetstream", func(t *testing.T) {
		p := permSink(t, url, "nopub", space, stream, NATSModeJetStream)
		if err := p.Health(ctx); err != nil {
			t.Fatalf("control: Health before any publish: %v", err)
		}
		if err := p.Write(ctx, batchFrom(t, newItemIn(space, line5424))); !IsFatal(err) {
			t.Fatalf("Write of a denied publish = %v (class %s), want fatal", err, ClassOf(err))
		}
		if err := p.Health(ctx); !IsFatal(err) {
			t.Fatalf("Health after the denial = %v, want fatal", err)
		}
	})
	t.Run("publish denied, core", func(t *testing.T) {
		p := permSink(t, url, "nopub", space, stream, NATSModeCore)
		if err := p.Health(ctx); err != nil {
			t.Fatalf("control: Health before any publish: %v", err)
		}
		if err := p.Write(ctx, batchFrom(t, newItemIn(space, line5424))); !IsFatal(err) {
			t.Fatalf("Write of a denied publish = %v (class %s), want fatal at once", err, ClassOf(err))
		}
	})
	t.Run("inbox subscription denied", func(t *testing.T) {
		p := permSink(t, url, "noinbox", space, stream, NATSModeJetStream)
		hctx, hcancel := context.WithTimeout(ctx, 3*time.Second)
		defer hcancel()
		if err := p.Health(hctx); !IsFatal(err) || !strings.Contains(err.Error(), "_INBOX") {
			t.Fatalf("Health with the ack inbox denied = %v (class %s), want fatal naming _INBOX", err, ClassOf(err))
		}
	})
	t.Run("beacon denied", func(t *testing.T) {
		p := permSink(t, url, "nobeacon", space, stream, NATSModeJetStream)
		if err := p.Health(ctx); err != nil {
			t.Fatal(err)
		}
		if err := p.PublishCore("sls_perm_ctl.beacon", []byte("{}")); err != nil {
			t.Fatalf("PublishCore: %v", err)
		}
		// The -ERR arrives asynchronously; the next round trips follow it.
		if err := p.Write(ctx, batchFrom(t, newItemIn(space, line5424))); err != nil {
			t.Fatalf("Write after a beacon denial = %v: the control plane took the data down", err)
		}
		time.Sleep(200 * time.Millisecond)
		if err := p.Health(ctx); err != nil {
			t.Fatalf("Health after a beacon denial = %v", err)
		}
		p.mu.Lock()
		last := p.nc.LastError()
		p.mu.Unlock()
		if !errors.Is(last, nats.ErrPermissionViolation) {
			t.Errorf("control: the beacon was not denied (last error %v); the case proves nothing", last)
		}
	})
}

// §22 pendência 19 against a real server: credentials refused at the first
// CONNECT end the boot at once, fatal, instead of waiting out
// sink.boot_timeout with the network's message.
func TestNATSRefusedCredentialsAreFatal(t *testing.T) {
	url, space, stream := natsPermSetup(t)
	cfg := NATSConfig{URLs: []string{url}, Username: "admin", Password: "wrong", Stream: stream,
		Workspace: space, ConnectTimeout: 5 * time.Second}
	p, err := NewNATS(cfg, discard(), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	h := NewHealth(p, HealthConfig{Timeout: 5 * time.Second}, discard(), metrics())
	start := time.Now()
	err = h.WaitReady(context.Background(), 30*time.Second, 10*time.Millisecond, 100*time.Millisecond)
	if !IsFatal(err) {
		t.Fatalf("WaitReady with a wrong password = %v (class %s), want fatal", err, ClassOf(err))
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the verdict took %v; it must not wait out the boot timeout", d)
	}
}

// A discard=new stream whose max_bytes is below one record refuses the first
// large message for good, and every reingestion of the dump the same way: the
// boot refuses it (ported from SN/N; independent review, 2026-10-11). With
// discard=old, or room for a record, the stream is accepted.
func TestNATSValidateDiscardNewBelowARecord(t *testing.T) {
	w, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_SYSLOG", Workspace: "default",
		MaxEventBytes: 100 << 10}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	base := jetstream.StreamConfig{Name: "LOGHUB_SYSLOG", Subjects: []string{"loghub.default.syslog"}}
	small := base
	small.Discard, small.MaxBytes = jetstream.DiscardNew, 4096
	if err := w.validateSize(1<<20, &small); !IsFatal(err) || !strings.Contains(err.Error(), "discard=new") {
		t.Fatalf("discard=new max_bytes=4096 = %v, want fatal naming it", err)
	}
	for name, cfg := range map[string]jetstream.StreamConfig{
		"discard=old": func() jetstream.StreamConfig { c := small; c.Discard = jetstream.DiscardOld; return c }(),
		"room":        func() jetstream.StreamConfig { c := small; c.MaxBytes = 1 << 30; return c }(),
		"no cap":      func() jetstream.StreamConfig { c := small; c.MaxBytes = 0; return c }(),
	} {
		if err := w.validateSize(1<<20, &cfg); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
}

// nats.go keeps only the last -ERR: a beacon denial arriving during a core
// chunk may hide a data-path denial before it, so the chunk is not confirmed.
// An old denial, a data-path one (the verdict handles it) or no error at all
// change nothing (independent review, 2026-10-11).
func TestMaskedDenial(t *testing.T) {
	beacon := fmt.Errorf("%w: Permissions Violation for Publish to \"loghub._ctl.beacon\"", nats.ErrPermissionViolation)
	data := fmt.Errorf("%w: Permissions Violation for Publish to \"loghub.default.syslog\"", nats.ErrPermissionViolation)
	other := errors.New("nats: stale connection")
	for _, c := range []struct {
		name         string
		before, last error
		want         bool
	}{
		{"new beacon denial", nil, beacon, true},
		{"new beacon denial after another error", other, beacon, true},
		{"the same old beacon denial", beacon, beacon, false},
		{"a data-path denial is the verdict's", nil, data, false},
		{"not a denial", nil, other, false},
		{"no error", nil, nil, false},
	} {
		if got := maskedDenial(c.before, c.last); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The flush of a core chunk -- or of the core health check -- that a dropped
// connection interrupts returns ErrConnectionClosed from nats.go (the pending
// PING's channel is closed) although the client is only RECONNECTING. Classed
// as fatal by the table, a NATS blip in the middle of a flush was FATAL, a dump
// and exit 3 instead of a retry (go-syslog-nats-server TESTING §8 item 1, the
// same in go-webhook-nats-server and go-relplog-server; found in
// go-relplog-nats-server, 2026-10-13). It is network now; a
// connection that is closed for good stays fatal (the control).
func TestNATSCoreFlushCutByAReconnectionIsNetwork(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	for _, op := range []string{"write", "health"} {
		srv.set(func(f *natsFake) { f.silent = true })
		done := make(chan error, 1)
		go func() {
			if op == "write" {
				done <- n.Write(ctx, batchOf(t, line5424))
			} else {
				done <- n.Health(ctx)
			}
		}()
		time.Sleep(200 * time.Millisecond) // the flush is waiting for its PONG
		srv.set(func(f *natsFake) { f.silent = false })
		srv.drop()
		err := <-done
		if err == nil || ClassOf(err) != ClassNetwork {
			t.Fatalf("%s: a flush cut by a reconnection = %v (class %s), want network", op, err, ClassOf(err))
		}
		waitConnected(t, n)
	}
	// Control: once the connection is closed for good, the same error is fatal.
	n.Close()
	if err := n.flushErr(n.nc, "flush", nats.ErrConnectionClosed); !IsFatal(err) {
		t.Fatalf("a flush on a closed connection = %v, want fatal", err)
	}
}

func waitConnected(t *testing.T, n *NATS) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		nc := n.nc
		n.mu.Unlock()
		if nc != nil && nc.IsConnected() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the client did not reconnect")
}
