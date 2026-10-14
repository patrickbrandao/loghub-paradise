package sink

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
)

// Porte das correções da família syslog/RELP (2026-10-13), the NATS sink: the
// sink of go-syslog-nats-server after its port from go-relplog-nats-server
// (fb92edd: TLS verdict through LastError, the core flush across a
// reconnection, the masked denial, 10077 of a full or closing stream, 10158,
// 10167 and 429, retention=interest, the _INBOX denial), the "store is closed"
// text of go-syslog-server's review (X3) and its rule of the pool (C1). The
// fake server and its helpers come from go-syslog-nats-server. Each test
// failed on the code before the port. The ones against a real server skip
// without it:
//
//	WHS_TEST_NATS_URL       JetStream (the suite's own, nats_integration_test.go)
//	WHS_TEST_NATS_PERM_URL  JetStream with the users of natsPermSetup
//	WHS_TEST_NATS_TLS_URL   a listener with tls {}
//	WHS_TEST_NATS_SERVER    the nats-server binary, for the servers a test
//	                        starts, stops and restarts on its own port
//
// (docs/TESTING.md §15.3).

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
	conns        []net.Conn
	subjects     []string
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
	f.conns = append(f.conns, c)
	info := f.info
	f.mu.Unlock()
	if _, err := c.Write([]byte("INFO " + info + "\r\n")); err != nil {
		return
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
					ack := fmt.Sprintf(`{"stream":"LOGHUB_WEBHOOK","seq":%d}`, seq)
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
	cfg := NATSConfig{URLs: []string{"nats://" + srv.addr}, Mode: mode, Stream: "LOGHUB_WEBHOOK",
		Workspace: "default", ConnectTimeout: 5 * time.Second, OpTimeout: 5 * time.Second}
	if mode == NATSModeCore {
		cfg.Stream = ""
	}
	if mut != nil {
		mut(&cfg)
	}
	log := quietLog()
	if buf != nil {
		log = slog.New(slog.NewTextHandler(buf, nil))
	}
	n, err := NewNATS(cfg, log, testMetrics())
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

func permViolation(s string) error { return fmt.Errorf("%w: %s", nats.ErrPermissionViolation, s) }

// ------------------------------------------------- 10077 of a full stream

// A stream with discard=new that is full answers 10077 with the store's own
// words, "maximum messages exceeded": backpressure that clears when a
// consumer acknowledges or the limits move, not a store that can never take
// the message. It was fatal -- a dump and exit 3, and the stored part of the
// batch duplicated at reingestion. Against the real server, so the text the
// classification matches is the one nats-server 2.14.6 sends.
func TestNATSFullDiscardNewStreamIsNetwork(t *testing.T) {
	f := natsSetup(t)
	space, stream := f.newStream(func(c *jetstream.StreamConfig) {
		c.MaxMsgs, c.Discard = 1, jetstream.DiscardNew
	})
	cfg := f.cfg
	cfg.Stream, cfg.Workspace = stream, space
	p := f.sink(cfg)
	f.ready(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Write(ctx, f.items(space, 1)); err != nil {
		t.Fatalf("control: the first message into an empty stream: %v", err)
	}
	err := p.Write(ctx, f.items(space, 1))
	if err == nil {
		t.Fatal("a message past max_msgs with discard=new was confirmed")
	}
	if ClassOf(err) != ClassNetwork || !strings.Contains(err.Error(), "maximum messages exceeded") {
		t.Fatalf("Write into a full discard=new stream = %v (class %s), want network naming the store's refusal",
			err, ClassOf(err))
	}
}

// ------------------------------------------- retention and per-subject cap

// SPEC §18.1 item 7 (closed): retention=interest acknowledges and keeps
// nothing while no consumer is interested, so a confirmed event could be
// lost: refused, fatal (decided in go-syslog-server, docs/DECISOES.md).
// workqueue and limits are the controls. max_msgs_per_subject is only warned
// about, once per value, though validate runs on every check.
func TestNATSValidateRetentionAndPerSubjectCap(t *testing.T) {
	ok := jetstream.StreamConfig{Name: "LOGHUB_WEBHOOK", Subjects: []string{subjectOf("default")}}
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
	w, err := NewNATS(NATSConfig{URLs: []string{"nats://127.0.0.1:1"}, Stream: "LOGHUB_WEBHOOK", Workspace: "default"},
		slog.New(slog.NewTextHandler(&buf, nil)), testMetrics())
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

// Against a real server: a stream with retention=interest and no consumer
// acknowledges a publish and keeps nothing -- shown here, so the refusal is
// about a real loss -- and Health refuses it, fatal.
func TestNATSInterestStreamIsRefused(t *testing.T) {
	f := natsSetup(t)
	space, stream := f.newStream(func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := f.js.Publish(ctx, subjectOf(space), []byte("{}")); err != nil {
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

// ------------------------------------------------------------ permissions

// SPEC §18.1 item 9 (closed): a denial on the data path reaches this sink
// only asynchronously; the _INBOX one too. Kept as the verdict, Health and
// Write return it, and Write then publishes nothing; a denial on the
// beacon's subject is not one.
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
	if err := n.Write(ctx, mkBatch(2)); err != nil {
		t.Fatalf("control: Write after a beacon denial = %v", err)
	}
	sent := srv.pubs.Load()

	n.asyncError(permViolation(`Permissions Violation for Subscription to "_INBOX.abc.*"`))
	if err := n.Health(ctx); !IsFatal(err) || !strings.Contains(err.Error(), "permissions violation") {
		t.Fatalf("Health after a data-path denial = %v, want fatal naming it", err)
	}
	if err := n.Write(ctx, mkBatch(1)); !IsFatal(err) {
		t.Fatalf("Write after the verdict = %v, want fatal", err)
	}
	if got := srv.pubs.Load(); got != sent {
		t.Errorf("Write after the verdict published %d messages, want none", got-sent)
	}
}

// Jetstream mode: a denied publish never gets an ack, so the futures time out
// -- network, retried forever. The -ERR already sits in LastError when they
// do, and the Write returns the verdict, fatal (the batch tainted before it,
// as for any failure after a publish); the ErrorHandler is held back so only
// that read can do it.
func TestNATSJetStreamDeniedPublishIsFatalAfterTheTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srv := startNATSFake(t, natsFakeINFO(""))
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.webhook\"'\r\n"
	})
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.OpTimeout = 500 * time.Millisecond })
	holdAsyncErrors(n)
	err := n.Write(ctx, mkBatch(2))
	if !IsFatal(err) || !strings.Contains(err.Error(), "ermissions") {
		t.Fatalf("Write of a denied batch = %v (class %s), want fatal naming the denial", err, ClassOf(err))
	}
}

// natsPermSetup addresses a JetStream server whose users have limited
// permissions, by WHS_TEST_NATS_PERM_URL (no user part), and creates a stream
// for a fresh space as its admin. The server's configuration:
//
//	jetstream { store_dir: "/tmp/nats-perm" }
//	authorization { users = [
//	  { user: admin, password: admin }
//	  { user: nopub, password: nopub, permissions: {
//	      publish: { allow: [">"], deny: ["loghub.*.webhook"] },
//	      subscribe: { allow: [">"] } } }
//	  { user: noinbox, password: noinbox, permissions: {
//	      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
//	  { user: nobeacon, password: nobeacon, permissions: {
//	      publish: { allow: [">"], deny: ["*.beacon"] }, subscribe: { allow: [">"] } } }
//	] }
func natsPermSetup(t *testing.T) (url, space, stream string) {
	t.Helper()
	url = strings.TrimSpace(os.Getenv("WHS_TEST_NATS_PERM_URL"))
	if url == "" {
		t.Skip("set WHS_TEST_NATS_PERM_URL to a JetStream server with the users of natsPermSetup")
	}
	nc, err := nats.Connect(url, nats.UserInfo("admin", "admin"), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("WHS_TEST_NATS_PERM_URL is not reachable as admin: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	n := time.Now().UnixNano()
	space, stream = fmt.Sprintf("p%d", n), fmt.Sprintf("WHS_PERM_%d", n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, natsCanonicalStream(stream, space)); err != nil {
		t.Fatalf("creating stream %s: %v", stream, err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), stream) })
	return url, space, stream
}

func permSink(t *testing.T, url, user, space, stream, mode string, opTimeout time.Duration) *NATS {
	t.Helper()
	cfg := NATSConfig{URLs: []string{url}, Username: user, Password: user, Mode: mode, Stream: stream,
		Workspace: space, CtlNamespace: "whs_perm_ctl", ConnectTimeout: 5 * time.Second, OpTimeout: opTimeout}
	if mode == NATSModeCore {
		cfg.Stream = ""
	}
	p, err := NewNATS(cfg, quietLog(), testMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// Against a real server: a role denied the data subject, or the _INBOX the
// acks and the API's answers come back on, is a permanent verdict -- fatal
// from the very Write and Health that met it -- in both modes; a role denied
// only the beacon's subject keeps the data path healthy.
func TestNATSPermissionDeniedIsFatal(t *testing.T) {
	url, space, stream := natsPermSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The ack's own timeout (sink.op_timeout) ends the wait, or the worker's
	// deadline does first: the verdict either way.
	for name, op := range map[string]time.Duration{"publish denied, jetstream": time.Second,
		"publish denied, jetstream, the deadline first": time.Minute} {
		t.Run(name, func(t *testing.T) {
			p := permSink(t, url, "nopub", space, stream, NATSModeJetStream, op)
			if err := p.Health(ctx); err != nil {
				t.Fatalf("control: Health before any publish: %v", err)
			}
			wctx, wcancel := context.WithTimeout(ctx, 3*time.Second)
			defer wcancel()
			if err := p.Write(wctx, []*envelope.Item{itemIn(space, testRequest("/p", []byte("p")))}); !IsFatal(err) {
				t.Fatalf("Write of a denied publish = %v (class %s), want fatal", err, ClassOf(err))
			}
			if err := p.Health(ctx); !IsFatal(err) {
				t.Fatalf("Health after the denial = %v, want fatal", err)
			}
		})
	}
	t.Run("publish denied, core", func(t *testing.T) {
		p := permSink(t, url, "nopub", space, stream, NATSModeCore, time.Second)
		if err := p.Health(ctx); err != nil {
			t.Fatalf("control: Health before any publish: %v", err)
		}
		if err := p.Write(ctx, []*envelope.Item{itemIn(space, testRequest("/p", []byte("p")))}); !IsFatal(err) {
			t.Fatalf("Write of a denied publish = %v (class %s), want fatal at once", err, ClassOf(err))
		}
	})
	t.Run("inbox subscription denied", func(t *testing.T) {
		p := permSink(t, url, "noinbox", space, stream, NATSModeJetStream, time.Second)
		hctx, hcancel := context.WithTimeout(ctx, 3*time.Second)
		defer hcancel()
		if err := p.Health(hctx); !IsFatal(err) || !strings.Contains(err.Error(), "_INBOX") {
			t.Fatalf("Health with the ack inbox denied = %v (class %s), want fatal naming _INBOX", err, ClassOf(err))
		}
	})
	t.Run("beacon denied", func(t *testing.T) {
		p := permSink(t, url, "nobeacon", space, stream, NATSModeJetStream, time.Second)
		if err := p.Health(ctx); err != nil {
			t.Fatal(err)
		}
		if err := p.PublishCore("whs_perm_ctl.beacon", []byte("{}")); err != nil {
			t.Fatalf("PublishCore: %v", err)
		}
		// The -ERR arrives asynchronously; the next round trips follow it.
		if err := p.Write(ctx, []*envelope.Item{itemIn(space, testRequest("/b", []byte("b")))}); err != nil {
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

// nats.go keeps only the last -ERR: a beacon denial arriving during a core
// batch may hide a data-path denial before it, so the batch is not confirmed.
// An old denial, a data-path one (the verdict handles it) or no error at all
// change nothing.
func TestMaskedDenial(t *testing.T) {
	beacon := permViolation(`Permissions Violation for Publish to "loghub._ctl.beacon"`)
	data := permViolation(`Permissions Violation for Publish to "loghub.default.webhook"`)
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

// Core: a denial of the beacon subject that lands between the batch's PUBs
// and the flush hides an earlier denial of the data path (nats.go keeps only
// the last -ERR). The ErrorHandler is held back, so only writeCore's own
// maskedDenial check can refuse to confirm the batch.
func TestNATSCoreMaskedDenialIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.replyFor = func(subject string) string {
			return "-ERR 'Permissions Violation for Publish to \"" + subject + "\"'\r\n"
		}
	})
	beforeCoreFlush = func() { _ = n.nc.Publish("loghub._ctl.beacon", []byte("{}")) }
	t.Cleanup(func() { beforeCoreFlush = nil })
	if err := n.Write(ctx, mkBatch(1)); err == nil {
		t.Fatal("a batch the server denied was confirmed: the beacon's -ERR masked the data path's")
	}
}

// Core Health reads LastError after its flush: a denial of the data path
// becomes the verdict there even with the ErrorHandler late.
func TestNATSCoreHealthReadsTheDenialItsFlushFollows(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.webhook\"'\r\n"
	})
	if err := n.nc.Publish("loghub.default.webhook", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := n.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health after a denied publish = %v, want the fatal verdict", err)
	}
}

// ------------------------------------------------- core and reconnections

// The flush of a core batch -- or of the core health check -- that a dropped
// connection interrupts returns ErrConnectionClosed from nats.go (the pending
// PING's channel is closed) although the client is only RECONNECTING. Classed
// as fatal by the table, a NATS blip in the middle of a flush was FATAL, a dump
// and exit 3 instead of a retry. It is network now; a connection that is
// closed for good stays fatal (the control).
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
				done <- n.Write(ctx, mkBatch(1))
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

// A core flush that STARTS while the client reconnects is resolved by the new
// server's PONG, but the batch's PUBs went down the old socket: it was
// confirmed although nothing says the server received it.
func TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	beforeCoreFlush = func() {
		srv.drop()
		deadline := time.Now().Add(5 * time.Second)
		for n.nc.IsConnected() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	t.Cleanup(func() { beforeCoreFlush = nil })
	err := n.Write(ctx, mkBatch(1))
	if err == nil || ClassOf(err) != ClassNetwork {
		t.Fatalf("a batch whose flush crossed a reconnection = %v (class %s), want network", err, ClassOf(err))
	}
	// Control: with the connection steady, the same write is confirmed.
	beforeCoreFlush = nil
	waitConnected(t, n)
	if err := n.Write(ctx, mkBatch(1)); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// -------------------------------------------------------------------- TLS

// A server that starts demanding TLS after the boot: nats.go never reports it
// through ReconnectErrCB (that is for a failed TCP dial), so Health answered
// "connection is RECONNECTING", network, until the fuses -- or forever. Read
// from LastError, with no other server to go to, it is the verdict.
func TestNATSReconnectToATLSOnlyServerIsAVerdict(t *testing.T) {
	for _, mode := range []string{NATSModeCore, NATSModeJetStream} {
		t.Run(mode, func(t *testing.T) {
			srv := startNATSFake(t, natsFakeINFO(""))
			n := fakeSink(t, srv, mode, nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if _, _, err := n.connect(); err != nil {
				t.Fatal(err)
			}
			srv.set(func(f *natsFake) { f.info = natsFakeINFO(`,"tls_required":true`) })
			srv.drop()
			deadline := time.Now().Add(8 * time.Second)
			var err error
			for time.Now().Before(deadline) {
				// The fake answers no JetStream API: a check that still finds
				// the old connection up waits for its stream info no longer
				// than this.
				hctx, hcancel := context.WithTimeout(ctx, 500*time.Millisecond)
				err = n.Health(hctx)
				hcancel()
				if IsFatal(err) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !IsFatal(err) || !errors.Is(err, errNATSServerRequiresTLS) {
				t.Fatalf("Health after reconnecting to a TLS-only server = %v (class %s), want the fatal TLS verdict",
					err, ClassOf(err))
			}
		})
	}
}

// natsProc is a nats-server this test owns, by WHS_TEST_NATS_SERVER, on a port
// of its own that survives a stop and a start: a client reconnects to the
// same address, and finds there whatever configuration the test gave the
// next life.
type natsProc struct {
	t    *testing.T
	bin  string
	port int
	cmd  *exec.Cmd
}

func natsServerBin(t *testing.T) string {
	t.Helper()
	bin := strings.TrimSpace(os.Getenv("WHS_TEST_NATS_SERVER"))
	if bin == "" {
		t.Skip("set WHS_TEST_NATS_SERVER to a nats-server binary to run the tests that start their own servers")
	}
	return bin
}

// freePort is a port nobody listens on right now.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newNATSProc(t *testing.T, port int) *natsProc {
	t.Helper()
	p := &natsProc{t: t, bin: natsServerBin(t), port: port}
	t.Cleanup(p.stop)
	return p
}

func (p *natsProc) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", p.port) }

// start runs a life of the server with conf after its listen line, and waits
// until the port accepts.
func (p *natsProc) start(conf string) {
	p.t.Helper()
	p.stop()
	file := filepath.Join(p.t.TempDir(), "nats.conf")
	body := fmt.Sprintf("listen: 127.0.0.1:%d\n%s\n", p.port, conf)
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		p.t.Fatal(err)
	}
	p.cmd = exec.Command(p.bin, "-c", file)
	if err := p.cmd.Start(); err != nil {
		p.t.Fatalf("starting nats-server: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p.port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			p.t.Fatalf("nats-server on %d never accepted: %v", p.port, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *natsProc) stop() {
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	p.cmd = nil
}

// SPEC §8.7.6, the decision this server keeps: a node of the pool that
// demands TLS is the verdict even with another node that still takes
// plaintext -- a cluster moving to TLS is no longer a destination of this
// server. go-syslog-server, which has TLS, made it network instead (its
// review, 2026-10-13, C1); here the guard is that the verdict still comes, now
// through LastError: connected to the plaintext node, that node goes down,
// and the reconnection meets the TLS one (the shared listener of
// WHS_TEST_NATS_TLS_URL).
func TestNATSATLSServerOfAPoolIsAVerdict(t *testing.T) {
	tlsURL := os.Getenv("WHS_TEST_NATS_TLS_URL")
	if tlsURL == "" {
		t.Skip("set WHS_TEST_NATS_TLS_URL to a nats-server listener with tls {}")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := newNATSProc(t, freePort(t))
	a.start("")
	n := newTestNATS(t, func(c *NATSConfig) {
		c.URLs, c.Mode, c.Stream, c.ConnectTimeout = []string{a.url(), tlsURL}, NATSModeCore, "", 2*time.Second
	})
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health with the plaintext node up = %v", err)
	}
	a.stop()
	deadline := time.Now().Add(10 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		hctx, hcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err = n.Health(hctx)
		hcancel()
		if IsFatal(err) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !IsFatal(err) || !errors.Is(err, errNATSServerRequiresTLS) {
		t.Fatalf("Health with the plaintext node down and a TLS node in the pool = %v (class %s), want the TLS verdict",
			err, ClassOf(err))
	}
}
