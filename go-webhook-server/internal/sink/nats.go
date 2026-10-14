package sink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
)

// NATSConfig mirrors [sink.nats].
type NATSConfig struct {
	URLs        []string
	Username    string
	Password    string
	Token       string
	Credentials string // path to a .creds file
	TLS         bool
	TLSCA       string
	TLSInsecure bool
	Stream      string
	// Subject is sink.nats.subject, default "loghub.{space}.{type}", expanded
	// per envelope (SPEC §3.8). Workspace is this instance's space: the
	// subject the boot validates against the stream.
	Subject    string
	Workspace  string
	ClientName string

	ConnectTimeout time.Duration
	// OpTimeout bounds how long the client keeps an unanswered publish
	// pending, so a message -- and the buffer it points at -- is never held
	// past the worker's own deadline (SPEC §8.7.2).
	OpTimeout time.Duration
	// MaxEventBytes is the largest envelope one accepted request can produce
	// (SPEC §3.2), checked against max_payload and the stream's max_msg_size
	// at boot (SPEC §8.7.5). Zero skips the check.
	MaxEventBytes int64
	// MaxPending is the ceiling of async publishes in flight per process:
	// 2 × (sink.workers × sink.batch + ring.max_inflight) (SPEC §8.7.2).
	MaxPending int
}

// Header names of SPEC §3.8. Nats-Msg-Id is the library's.
const (
	natsHeaderType  = "X-LogHub-Type"
	natsHeaderUUID  = "X-LogHub-UUID"
	natsHeaderSpace = "X-LogHub-Space"

	// natsHeaderOverhead is what the header block costs: the names, the
	// uuid twice, a space and a type of at most 64 each, the content type and
	// the framing. The envelope itself is in Data, never in a header (the
	// JetStream ceiling on headers is 64 KiB).
	natsHeaderOverhead = 512

	// natsCtlBeacon is the LogHub control plane's beacon subject, under the
	// default loghub._ctl namespace. The data stream must never cover it
	// (SPEC §3.8, §8.7.5): it would store every beacon of the ecosystem and
	// replay old ones to every new consumer.
	natsCtlBeacon = "loghub._ctl.beacon"

	// jsFileStoreMaxRecord is the largest record a file-backed stream stores,
	// whatever max_payload says: nats-server refuses a record above its
	// rlBadThresh of 32 MiB with a store failure, 10077, which is fatal here
	// (SPEC §8.7.6).
	jsFileStoreMaxRecord = 32 << 20
)

// NATS is the JetStream destination (SPEC §8.7). Message shape in §3.8.
//
// It is the destination that most resembles Redis -- remote, schemaless, a
// consumer on the other side -- with one difference that decides everything:
// JetStream acknowledges every publish, and that acknowledgement is what
// gives the 200 of ring=none its meaning.
type NATS struct {
	cfg     NATSConfig
	subject topic
	log     *slog.Logger
	m       *obs.Metrics

	opts   []nats.Option
	jsOpts []jetstream.JetStreamOpt

	// mu guards the lazy connect; nc and js are set once and never replaced.
	mu sync.Mutex
	nc *nats.Conn
	js jetstream.JetStream

	// covered holds the subjects other than the instance's own that the
	// configured stream was found to listen on (cover). Only a backlog from
	// another Workspace produces them, so the set stays small; natsCoveredMax
	// bounds it anyway, and a subject past the bound is just asked again.
	coverMu sync.Mutex
	covered map[string]struct{}

	// asyncErrLog samples the client's asynchronous errors. A publish
	// permission denied produces one per message, and SPEC §11.3 allows no
	// per-message log line on the default path.
	asyncErrLog *obs.Sampler

	validated atomic.Bool
	closed    atomic.Bool

	// perSubjectWarned is the max_msgs_per_subject last warned about, so the
	// validation that runs on every health check says it once per value, not
	// once per check.
	perSubjectWarned atomic.Int64

	// verdict is a permanent refusal the client reported asynchronously -- a
	// permissions violation, a reconnection whose TLS can never succeed
	// (isTLSRefusal) -- which no synchronous call returns: a denied publish
	// only times out, and a denied stream info only times out. Health and
	// every Write turn it into FATAL (SPEC §8.7.2).
	verdict atomic.Pointer[error]
}

// natsCoveredMax bounds the set of foreign subjects known to be covered.
const natsCoveredMax = 1024

// NewNATS prepares the client without connecting: the connection is opened
// by the first health check, and the boot's WaitReady owns the retry loop
// (SPEC §8.7.1). The errors here are configuration errors -- a CA file that
// does not load, a credentials file that does not exist -- never a network
// condition.
func NewNATS(cfg NATSConfig, log *slog.Logger, m *obs.Metrics) (*NATS, error) {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	if cfg.MaxPending < 1 {
		cfg.MaxPending = 4096
	}
	if len(cfg.URLs) == 0 {
		return nil, errors.New("sink nats: no server URL")
	}
	if cfg.Stream == "" || cfg.Subject == "" {
		return nil, errors.New("sink nats: stream and subject are required")
	}

	n := &NATS{cfg: cfg, log: log, m: m, subject: newTopic(cfg.Subject, cfg.Workspace),
		asyncErrLog: obs.NewSampler(30 * time.Second)}
	opts := []nats.Option{
		nats.Name(cfg.ClientName),
		nats.Timeout(cfg.ConnectTimeout),
		// The health machine has the fuses that decide when to give up; a
		// client that closed itself after N attempts would turn a long outage
		// into an unrecoverable state with no emergency dump (SPEC §8.7.1).
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		// No reconnect buffer: the ring is the only buffer of this design. A
		// publish during a disconnect fails at once with the network class
		// instead of queueing inside the client, invisible to the ring's
		// accounting, to the 429 and to the dump.
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectErrHandler(func(nc *nats.Conn, err error) {
			// nats.go calls this only for a reconnection whose TCP dial
			// failed; a TLS refusal comes later, in the handshake, and is
			// caught by noteLastError from Health. Kept for a client version
			// that reports it here. Only a verdict is logged: every failed
			// dial of a long outage as an ERROR would repeat what /readyz and
			// whs_sink_up already say. The verdict only with no other server
			// to go to (soleServer).
			if isTLSRefusal(err) && soleServer(nc) {
				n.setVerdict(err)
				n.asyncError(err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			m.SinkNATSReconnectsTotal.Inc()
			log.Warn("nats reconnected", "url", connectedURL(nc))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			// Our own Close arrives here too; only a close the client decided
			// on -- a repeated authentication refusal -- is an error.
			if n.closed.Load() {
				log.Info("nats connection closed")
				return
			}
			log.Error("nats connection closed by the client; it will not reconnect", "error", nc.LastError())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			n.asyncError(err)
		}),
	}
	switch {
	case cfg.Credentials != "":
		// The library reads the file at connect time; a missing file would
		// surface as a boot retry loop instead of the exit 1 a wrong path
		// deserves.
		if _, err := os.Stat(cfg.Credentials); err != nil {
			return nil, fmt.Errorf("sink nats: credentials: %w", err)
		}
		opts = append(opts, nats.UserCredentials(cfg.Credentials))
	case cfg.Token != "":
		opts = append(opts, nats.Token(cfg.Token))
	case cfg.Username != "":
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.TLS {
		// ServerName is left empty: nats.go fills it from the host it dials,
		// which is the right name for every server of a cluster.
		tc, err := loadTLS("sink nats", cfg.TLSCA, cfg.TLSInsecure, "")
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Secure(tc))
	}

	jsOpts := []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncMaxPending(cfg.MaxPending),
		// Without a timeout a future the server never answers stays pending
		// forever, holding the message and the buffer it points at.
		jetstream.WithPublishAsyncTimeout(cfg.OpTimeout),
	}

	n.opts, n.jsOpts = opts, jsOpts
	return n, nil
}

func (n *NATS) Type() string { return "nats" }

// asyncError logs an asynchronous client error, at most once per interval,
// with the count of what was suppressed in between, and keeps a permanent
// refusal for Health and Write (see verdict).
func (n *NATS) asyncError(err error) {
	if deniesTheDataPath(err) {
		n.setVerdict(err)
	}
	if ok, suppressed := n.asyncErrLog.Allow(); ok {
		n.log.Error("nats async error", "error", err, "suppressed", suppressed)
	}
}

func (n *NATS) setVerdict(err error) { n.verdict.CompareAndSwap(nil, &err) }

// verdictErr is the verdict as the fatal error every operation returns once
// it is set.
func (n *NATS) verdictErr(op string) error {
	if v := n.verdict.Load(); v != nil {
		return fatalErr(op, fmt.Errorf("the NATS server refused this server permanently: %w", *v))
	}
	return nil
}

// noteLastError keeps the connection's last error as the verdict when it is a
// permanent refusal. The ErrorHandler runs on the client's callback goroutine,
// after the reader has already stored the -ERR in LastError; a caller that has
// just seen its own round trip fail reads it here instead of racing the
// handler.
func (n *NATS) noteLastError(nc *nats.Conn) {
	last := nc.LastError()
	// A TLS that can never succeed -- a server whose certificate this client
	// does not trust, or one that stopped offering the TLS sink.nats.tls
	// demands -- refuses this client for good. Seen only here after the boot:
	// nats.go calls ReconnectErrCB only when the TCP dial of a reconnection
	// fails, while the TLS refusal happens later, in the connect handshake,
	// and lands in LastError with the status RECONNECTING. Without this the
	// FATAL came from the fuses, ~20 s later, reading "connection is
	// RECONNECTING" -- or never, with both fuses off (go-relplog-nats-server,
	// independent review, 2026-10-13, through go-syslog-server; SPEC §8.7.9).
	//
	// Only when no other server can serve (soleServer). LastError is the last
	// server TRIED: with a pool -- several sink.nats.urls, or servers the
	// cluster announced -- the client cycles through them while the one it
	// trusts is down, and the TLS error of one untrusted node read here made a
	// permanent FATAL, a dump and exit 3, though the trusted one came back a
	// moment later (go-syslog-server review, 2026-10-13). With a pool it stays
	// network, and Health names the TLS refusal as the reason, so the FATAL of
	// the fuses, if the pool never recovers, says why.
	if deniesTheDataPath(last) || (isTLSRefusal(last) && soleServer(nc)) {
		n.setVerdict(last)
	}
}

// soleServer reports that the client has no server to go to but the one it
// is using: its pool -- the configured URLs plus every server the cluster
// announced since (nats.go adds them on its own) -- has one entry. Only then
// is a TLS refusal a verdict on this client (SPEC §8.7.9).
func soleServer(nc *nats.Conn) bool { return nc == nil || len(nc.Servers()) <= 1 }

// escalate turns a network failure into the verdict once one is set. A publish
// the server denied never gets an ack, so the future ends in a timeout --
// network, retried forever with the sink UP -- and a denied ack subscription
// or stream info ends the same way. With the verdict in hand that timeout is
// known to be permanent.
func (n *NATS) escalate(nc *nats.Conn, op string, err error) error {
	if ClassOf(err) != ClassNetwork {
		return err
	}
	if nc != nil {
		n.noteLastError(nc)
	}
	if v := n.verdictErr(op); v != nil {
		return v
	}
	return err
}

// deniesTheDataPath reports a permissions violation. This server publishes
// nothing but data on its connection -- the envelopes, the JetStream API
// calls of Health and cover, and the subscription to the inbox the
// acknowledgements come back on -- so every violation is one the data path
// cannot live with. The server reports it only through the asynchronous
// error callback: the denied publish itself just times out, network, retried
// forever with the sink UP (the review of go-syslog-nats-server).
func deniesTheDataPath(err error) bool {
	return errors.Is(err, nats.ErrPermissionViolation)
}

// connectedURL is the server this connection reached, scheme and host only.
// nats.go's ConnectedUrl is the URL as configured, password included, and
// ConnectedUrlRedacted hides a password and nothing else: a URL whose user
// part is a token (nats://token@host) came out whole, in the INFO line of
// every boot and the WARN of every reconnect (SPEC §8.7.1).
func connectedURL(nc *nats.Conn) string {
	u, err := url.Parse(nc.ConnectedUrl())
	if err != nil || u.Host == "" {
		return nc.ConnectedAddr()
	}
	return u.Scheme + "://" + u.Host
}

// connect opens the connection on first use.
func (n *NATS) connect() (*nats.Conn, jetstream.JetStream, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nc != nil {
		return n.nc, n.js, nil
	}
	nc, err := nats.Connect(strings.Join(n.cfg.URLs, ","), n.opts...)
	if err != nil {
		if isTLSRefusal(err) && len(n.cfg.URLs) > 1 {
			// nats.Connect tries every URL and returns the error of the last
			// one: the TLS refusal of one server does not say the others
			// cannot serve -- one may be down for a moment. Network, waited
			// out to sink.boot_timeout, with the refusal named (SPEC §8.7.9).
			return nil, nil, networkErr("connect", fmt.Errorf(
				"a server of sink.nats.urls refused TLS, and no other answered: %w", err))
		}
		return nil, nil, classifyNATS("connect", err)
	}
	js, err := jetstream.New(nc, n.jsOpts...)
	if err != nil {
		nc.Close()
		return nil, nil, classifyNATS("jetstream", err)
	}
	n.nc, n.js = nc, js
	n.log.Info("nats connected", "url", connectedURL(nc), "max_payload", nc.MaxPayload())
	return nc, js, nil
}

// message builds the wire shape of SPEC §3.8 from the envelope's own header.
//
// Data is the item's JLH slice, not a copy: the library serialises it to the
// socket inside the publish call and, with retries disabled, never reads it
// again. The headers are short strings of their own.
func (n *NATS) message(it *envelope.Item) (*nats.Msg, error) {
	head, _, err := envelope.ParseHead(it.Env)
	if err != nil {
		return nil, err
	}
	h := nats.Header{}
	// <uuid>.<step>, never the bare uuid: a processor forwarding into the
	// same stream (same uuid, step+1) would have its legitimate hop dropped as
	// a duplicate by the JetStream window.
	h.Set(jetstream.MsgIDHeader, it.ID+"."+strconv.Itoa(head.Step))
	h.Set("Content-Type", "application/json")
	h.Set(natsHeaderType, head.Type)
	h.Set(natsHeaderUUID, it.ID)
	h.Set(natsHeaderSpace, head.Space)
	return &nats.Msg{Subject: n.subject.For(head.Space, head.Type), Header: h, Data: it.Env}, nil
}

// Write publishes the batch asynchronously and then waits for every
// acknowledgement (SPEC §8.7.2).
//
// A message refused for good -- an envelope that does not map, one the client
// refuses before it leaves (ErrMaxPayload), one the stream refuses in its
// acknowledgement (10054) -- is refused alone: the rest of the batch is still
// published and confirmed, and the logic error that comes back names what the
// stream stored (Error.Delivered, SPEC §8.1). The first such refusal used to
// end the batch, with the earlier messages in the stream counted as lost and
// the later ones never published yet confirmed (the review of
// go-syslog-nats-server).
//
// Every error after the first publish taints the batch: futures may still be
// pending, and the buffers must be left to the GC (SPEC §8.7.2, §12.3).
func (n *NATS) Write(ctx context.Context, items []*envelope.Item) error {
	if len(items) == 0 {
		return nil
	}
	if n.closed.Load() {
		return ErrClosed
	}
	if err := n.verdictErr("publish"); err != nil {
		return err
	}
	nc, js, err := n.connect()
	if err != nil {
		return err
	}

	// Every message is built, and every foreign subject checked, before the
	// first is published.
	b := newBatchResult(items)
	msgs := make([]*nats.Msg, len(items))
	for i, it := range items {
		msg, err := n.message(it)
		if err != nil {
			b.reject(i, "publish", err)
			continue
		}
		msgs[i] = msg
	}
	// The own subject is validated by every health check; any other is
	// checked once, before anything of the batch is published.
	for _, msg := range msgs {
		if msg != nil && msg.Subject != n.subject.own {
			if err := n.cover(ctx, js, msg.Subject, msg.Header.Get(natsHeaderSpace)); err != nil {
				return err
			}
		}
	}

	futs := make([]jetstream.PubAckFuture, len(msgs))
	published := 0
	for i, msg := range msgs {
		if msg == nil {
			continue
		}
		f, err := js.PublishMsgAsync(msg,
			// The server rejects the message if the subject belongs to
			// another stream: a configuration mistake becomes a refusal, not
			// rows in the wrong place.
			jetstream.WithExpectStream(n.cfg.Stream),
			// No retries inside the library: they would re-read msg.Data
			// after this call returned, which is the buffer race SPEC §12.3
			// forbids. The worker retries, with the item still owning the
			// buffer (SPEC §10.4).
			jetstream.WithRetryAttempts(0))
		if err != nil {
			if classifyNATSError(err) == ClassLogic {
				// Refused by the client before it left (ErrMaxPayload): this
				// message alone.
				b.reject(i, "publish", err)
				continue
			}
			if published > 0 {
				b.taint()
			}
			return n.escalate(nc, "publish", classifyNATS("publish", err))
		}
		futs[i] = f
		published++
	}

	var dups uint64
	for i, f := range futs {
		if f == nil {
			continue
		}
		select {
		case ack := <-f.Ok():
			if ack != nil && ack.Duplicate {
				dups++
			}
		case err := <-f.Err():
			c := classifyNATSError(err)
			if c != ClassLogic {
				// Retried (network) or the end (fatal) as a whole batch: which
				// futures resolved does not matter, and some are still pending.
				b.taint()
				op := "ack"
				if c != ClassNetwork {
					op += " id=" + b.items[i].ID
				}
				// A denied publish never gets an ack: its timeout is the
				// verdict once one is set (escalate).
				return n.escalate(nc, op, classifyNATS(op, err))
			}
			// One message refused by the stream (10054): the other futures say
			// what the stream stored.
			b.reject(i, "ack", err)
		case <-ctx.Done():
			b.taint()
			return n.escalate(nc, "ack", networkErr("ack", ctx.Err()))
		}
	}
	if dups > 0 {
		n.m.SinkNATSDuplicatesTotal.Add(dups)
	}
	if err := b.err(); err != nil {
		// Every future has resolved, but a refused write leaves its buffers to
		// the GC like any other failure, and this path is rare.
		b.taint()
		return err
	}
	return nil
}

// batchResult tracks which messages of a batch were refused for good, for
// Error.Delivered.
type batchResult struct {
	items    []*envelope.Item
	refused  *Error
	rejected []bool
}

func newBatchResult(items []*envelope.Item) *batchResult {
	return &batchResult{items: items, rejected: make([]bool, len(items))}
}

// reject marks message i as refused for good: logic, about that message only.
func (b *batchResult) reject(i int, op string, err error) {
	if b.refused == nil {
		b.refused = &Error{Class: ClassLogic, Op: op + " id=" + b.items[i].ID, Err: err}
	}
	b.rejected[i] = true
}

// err is nil when nothing was refused, or the logic error with the indexes the
// destination did take.
func (b *batchResult) err() error {
	if b.refused == nil {
		return nil
	}
	b.refused.Delivered = make([]int, 0, len(b.rejected))
	for i, r := range b.rejected {
		if !r {
			b.refused.Delivered = append(b.refused.Delivered, i)
		}
	}
	return b.refused
}

// taint hands every item of the batch to the GC instead of the pool (SPEC
// §12.3): a future may still be pending and pointing at Env.
func (b *batchResult) taint() {
	for _, it := range b.items {
		it.Taint()
	}
}

// cover checks, once per subject, that the configured stream listens on the
// subject of an envelope from another space (SPEC §8.7.5). The boot validates
// only this instance's own subject, and the subject is expanded per envelope
// (SPEC §3.8): a backlog that ring.db or an emergency dump kept from a life
// with another Workspace lands on a subject no stream may cover. Published
// blind, it got no answer -- "no response from stream", network -- and the
// boot drain gave up with exit 3, restart after restart, with an error that
// never named the cause. Asked first, it is fatal and names the subject.
//
// Only a covered subject is remembered: a refusal stops the process, and the
// next life asks again, after the operator has widened the stream.
func (n *NATS) cover(ctx context.Context, js jetstream.JetStream, subject, space string) error {
	n.coverMu.Lock()
	_, ok := n.covered[subject]
	n.coverMu.Unlock()
	if ok {
		return nil
	}
	owner, err := js.StreamNameBySubject(ctx, subject)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fatalErr("publish", fmt.Errorf(
			"no stream listens on subject %q, the subject of an envelope of space %q (a backlog from "+
				"another Workspace, in ring.db or an emergency dump): stream %q must also cover that "+
				"subject until the backlog drains (SPEC §8.7.5)", subject, space, n.cfg.Stream))
	}
	if err != nil {
		return classifyNATS("publish", err)
	}
	if owner != n.cfg.Stream {
		// The expected-stream header would refuse it too (10060); asked
		// first, the error says which stream took the subject.
		return fatalErr("publish", fmt.Errorf(
			"subject %q, of an envelope of space %q, belongs to stream %q, not %q: stream %q must "+
				"cover that subject until the backlog drains (SPEC §8.7.5)",
			subject, space, owner, n.cfg.Stream, n.cfg.Stream))
	}
	n.coverMu.Lock()
	if n.covered == nil {
		n.covered = map[string]struct{}{}
	}
	if len(n.covered) < natsCoveredMax {
		n.covered[subject] = struct{}{}
	}
	n.coverMu.Unlock()
	return nil
}

// Health checks the connection state and asks the stream for its info
// (SPEC §8.7.7). A PING would answer happily with JetStream disabled, and the
// state would never leave UP.
//
// Every check also runs the contract of SPEC §8.7.5 against the info it has
// just fetched.
func (n *NATS) Health(ctx context.Context) error {
	if n.closed.Load() {
		return ErrClosed
	}
	if err := n.verdictErr("health"); err != nil {
		return err
	}
	nc, js, err := n.connect()
	if err != nil {
		return err
	}
	switch st := nc.Status(); st {
	case nats.CONNECTED:
	case nats.CLOSED:
		// Never reconnects from here: only this process closes the
		// connection, or the server refused the credentials twice.
		last := nc.LastError()
		if last == nil {
			last = errors.New("connection closed")
		}
		return fatalErr("health", fmt.Errorf("connection closed for good: %w", last))
	default:
		// escalate reads LastError: what keeps the client RECONNECTING may be
		// permanent (a TLS that can never succeed, see noteLastError). With a
		// pool it is not a verdict, but it is the reason, and it goes in the
		// error: the FATAL of the fuses keeps the error of the last check.
		down := networkErr("health", fmt.Errorf("connection is %s", st))
		if last := nc.LastError(); isTLSRefusal(last) {
			down = networkErr("health", fmt.Errorf("connection is %s; a server of the pool refused TLS: %w", st, last))
		}
		return n.escalate(nc, "health", down)
	}

	stream, err := js.Stream(ctx, n.cfg.Stream)
	if err != nil {
		// A denied stream info, or a denied _INBOX subscription its answer
		// needed, only times out; the verdict says it is permanent.
		return n.escalate(nc, "health", n.streamErr(err))
	}
	// The whole contract on every check, against the info this call has
	// already fetched (no extra round trip). Checked once, a stream edited
	// underneath -- its subjects narrowed, no_ack turned on -- answered every
	// publish with a timeout, network, retried forever while this check kept
	// the sink UP and the ring filled (SPEC §8.7.5).
	info := stream.CachedInfo()
	if err := n.validate(nc.MaxPayload(), info); err != nil {
		if IsFatal(err) && !n.validated.Load() {
			return n.nameTheOwner(ctx, js, err)
		}
		return err
	}
	if !n.validated.Swap(true) {
		cfg := info.Config
		n.log.Info("stream validated",
			"stream", n.cfg.Stream, "subjects", cfg.Subjects, "covers", n.subject.own,
			"storage", cfg.Storage.String(), "replicas", cfg.Replicas,
			"duplicate_window", cfg.Duplicates.String(), "max_payload", nc.MaxPayload())
	}
	return nil
}

// streamErr classifies a failure to fetch the stream's info.
func (n *NATS) streamErr(err error) error {
	if !n.validated.Load() && errors.Is(err, nats.ErrNoResponders) {
		// Nobody answers the JetStream API at the first check: JetStream
		// is off on this server (an account without it answers 10039,
		// fatal on its own), or no JetStream server is reachable from it
		// -- a leaf whose hub is down. The client cannot tell the two
		// apart: jetstream's AccountInfo maps no responders to "not
		// enabled" itself, and probing $JS.API.INFO turned a role without
		// that permission into a permanent verdict. Either way nothing can
		// be confirmed now, and as network the boot waited out
		// sink.boot_timeout to exit with the wrong reason (SPEC §8.7.5).
		// After a validated check, the same answer is a blip: network.
		return fatalErr("validate", fmt.Errorf(
			"nobody answers the JetStream API: JetStream is disabled on this server, or no "+
				"JetStream server is reachable from it; sink=nats needs JetStream (docs/SPEC.md "+
				"§8.7.5): %w", err))
	}
	return classifyNATS("stream", err)
}

// nameTheOwner turns the boot's "does not cover" into the error that says
// where the subject goes instead: another stream, or none. It asks the server
// only on that failure, so a valid stream costs no extra round trip.
func (n *NATS) nameTheOwner(ctx context.Context, js jetstream.JetStream, err error) error {
	var uncovered *errUncovered
	if !errors.As(err, &uncovered) {
		return err
	}
	subject := n.subject.own
	owner, oerr := js.StreamNameBySubject(ctx, subject)
	switch {
	case errors.Is(oerr, jetstream.ErrStreamNotFound):
		return fatalErr("validate", fmt.Errorf(
			"no stream listens on subject %q; apply docker/nats/stream.json", subject))
	case oerr == nil && owner != n.cfg.Stream:
		return fatalErr("validate", fmt.Errorf(
			"subject %q belongs to stream %q, not %q", subject, owner, n.cfg.Stream))
	}
	return err
}

// errUncovered is the stream not covering this instance's own subject.
type errUncovered struct{ msg string }

func (e *errUncovered) Error() string { return e.msg }

// validate is the contract of SPEC §8.7.5, checked by every Health call,
// against the stream's info and the connected server's max_payload.
// Everything it refuses is ClassFatal: the stream is created by whoever runs
// NATS, from docker/nats/stream.json, and this server never creates or alters
// one.
func (n *NATS) validate(maxPayload int64, info *jetstream.StreamInfo) error {
	if info == nil {
		return networkErr("validate", errors.New("stream info not available"))
	}
	cfg := info.Config
	if cfg.Sealed {
		return fatalErr("validate", fmt.Errorf("stream %q is sealed and accepts no publishes", n.cfg.Stream))
	}
	if cfg.Retention == jetstream.InterestPolicy {
		// With interest retention a message nobody is interested in is
		// acknowledged with an ordinary PubAck and not kept: with no consumer
		// bound -- before the first one, or while every one is deleted -- each
		// batch is confirmed, released from the ring and lost, with no error
		// and no counter, after the 200 of ring=none (SPEC §16.1). The PubAck
		// is what gives that 200 and the ring's release their meaning, so a
		// stream that may discard on acceptance cannot be this sink's (decided
		// in go-syslog-server, 2026-10-11; ported 2026-10-13, docs/DECISOES.md).
		return fatalErr("validate", fmt.Errorf(
			"stream %q has retention=interest: it acknowledges and keeps nothing while no consumer is "+
				"interested, so a confirmed event could be lost (docs/SPEC.md §8.7.5); use "+
				"retention=limits (or workqueue)", n.cfg.Stream))
	}
	// Every event of a space shares one subject: a per-subject cap keeps only
	// the last few of a whole space and silently discards the rest as they
	// arrive, each one acknowledged. Legitimate for a "latest only" stream,
	// which is why it is a warning and not a refusal -- said once per value,
	// not once per check.
	if v := max(cfg.MaxMsgsPerSubject, 0); n.perSubjectWarned.Swap(v) != v && v > 0 {
		n.log.Warn("stream keeps at most max_msgs_per_subject messages per subject; every event of "+
			"a space shares one subject, so older events are discarded as new ones are acknowledged "+
			"(docs/SPEC.md §8.7.5)", "stream", n.cfg.Stream, "max_msgs_per_subject", v)
	}
	if cfg.NoAck {
		// A stream that never acknowledges turns every PublishMsgAsync future
		// into an ErrAsyncPublishTimeout: network, retried forever, the ring
		// filling behind it while the health check -- which only asks for the
		// stream's info -- keeps the sink UP. Nothing would ever be confirmed.
		return fatalErr("validate", fmt.Errorf(
			"stream %q has no_ack=true: it never acknowledges a publish, so no message could ever be "+
				"confirmed (docs/SPEC.md §8.7.5); set no_ack=false", n.cfg.Stream))
	}

	// Local pattern matching, not a server lookup by subject: no extra round
	// trip on every check. Two streams of one account cannot overlap, so a
	// covering pattern here means no other stream owns the subject.
	if own := n.subject.own; !coveredBy(cfg.Subjects, own) {
		return fatalErr("validate", &errUncovered{fmt.Sprintf(
			"stream %q (subjects %v) does not cover %q; apply docker/nats/stream.json",
			n.cfg.Stream, cfg.Subjects, own)})
	}

	// A stream that captures the control plane would store every beacon and
	// replay old ones to every new consumer (SPEC §3.8). loghub.> is the
	// classic way to get there.
	for _, p := range cfg.Subjects {
		if subjectCovers(p, natsCtlBeacon) {
			return fatalErr("validate", fmt.Errorf(
				"stream %q subject %q captures the control plane subject %q; the data stream must "+
					"never cover it (list the subject of each space, never loghub.>)",
				n.cfg.Stream, p, natsCtlBeacon))
		}
	}

	// A subject of another space found covered (cover) and no longer
	// covered is forgotten, not refused: only a boot-time backlog publishes
	// there, and an operator narrowing the stream once it drained -- what
	// cover's own error tells them they may do -- must not take a healthy
	// process down. The next publish there, if any, asks again and is refused
	// then, naming the subject (SPEC §8.7.5).
	n.coverMu.Lock()
	for subject := range n.covered {
		if !coveredBy(cfg.Subjects, subject) {
			delete(n.covered, subject)
		}
	}
	n.coverMu.Unlock()

	return n.validateSize(maxPayload, &cfg)
}

func coveredBy(patterns []string, subject string) bool {
	for _, p := range patterns {
		if subjectCovers(p, subject) {
			return true
		}
	}
	return false
}

// validateSize compares the largest message with the server's max_payload,
// the stream's max_msg_size, the file store's record ceiling and, for a
// stream that refuses new messages when full, its max_bytes (SPEC §8.7.5). A
// message accepted with 200 and then refused for its size would be logic:
// lost.
func (n *NATS) validateSize(maxPayload int64, cfg *jetstream.StreamConfig) error {
	if n.cfg.MaxEventBytes <= 0 {
		return nil
	}
	// The stream name travels too, in Nats-Expected-Stream; past ~230
	// characters it outgrows the slack of natsHeaderOverhead.
	need := n.cfg.MaxEventBytes + natsHeaderOverhead + int64(len(n.cfg.Stream))
	if maxPayload < need {
		return fatalErr("validate", fmt.Errorf(
			"nats-server max_payload=%d is below the largest event this server can accept (%d bytes); "+
				"raise max_payload or lower server.max_body_bytes", maxPayload, need))
	}
	if ms := int64(cfg.MaxMsgSize); ms > 0 && ms < need {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_msg_size=%d is below the largest event this server can accept (%d bytes); "+
				"raise it or lower server.max_body_bytes", n.cfg.Stream, ms, need))
	}
	// What the store keeps: the message, its subject and the record's own
	// framing.
	record := need + int64(len(n.subject.own)) + 64
	// A stream that refuses new messages when full, smaller than one message:
	// the first large event is refused for good, and every boot after the dump
	// validates and then fails its reingestion the same way. With discard old
	// the stream makes room instead. And the 10077 of a full stream is network
	// now (storeFailedIsTransient), so here it would hold the ring forever.
	if cfg.Discard == jetstream.DiscardNew && cfg.MaxBytes > 0 && cfg.MaxBytes < record {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_bytes=%d with discard=new is below the largest message this server can "+
				"publish (%d bytes; docs/SPEC.md §8.7.5); raise max_bytes or lower server.max_body_bytes",
			n.cfg.Stream, cfg.MaxBytes, record))
	}
	// A file store has a record ceiling of its own, below max_payload's upper
	// range, and past it the refusal is 10077, fatal: sizing max_payload up to
	// 64 MB would otherwise turn one large body into the end of the process.
	if cfg.Storage == jetstream.FileStorage && record > jsFileStoreMaxRecord {
		return fatalErr("validate", fmt.Errorf(
			"stream %q uses file storage, which stores records of at most %d bytes, below the "+
				"largest message this server can publish (%d bytes; docs/SPEC.md §8.7.5); lower "+
				"server.max_body_bytes", n.cfg.Stream, jsFileStoreMaxRecord, record))
	}
	return nil
}

// subjectCovers reports whether every subject matching subject (which may hold
// wildcards) also matches pattern -- NATS token rules: "*" is exactly one
// token, ">" one or more, and only at the end.
func subjectCovers(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, pt := range p {
		if pt == ">" {
			return i < len(s)
		}
		if i >= len(s) {
			return false
		}
		switch {
		case s[i] == ">":
			// The subject runs on for any number of tokens; only a ">" in
			// the pattern could follow it, and that case returned above.
			return false
		case pt == "*":
			// One token, literal or "*", either way.
		case pt != s[i]:
			// Different literals, or a literal against a "*" it cannot cover.
			return false
		}
	}
	return len(p) == len(s)
}

// Close releases the connection. Close, not Drain: the workers are stopped
// before the sink is closed, so there is nothing left to flush.
func (n *NATS) Close() error {
	if n.closed.Swap(true) {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nc != nil {
		n.nc.Close()
	}
	return nil
}

// JetStream API error codes this sink classifies by number (SPEC §8.7.6). The
// client library names some of them; the rest come from the server's error
// table, and the numbers are stable across versions.
const (
	jsErrInsufficientResources = 10023
	jsErrJetStreamNotEnabledFA = 10039
	jsErrMessageExceedsMaximum = 10054
	jsErrStreamNotFound        = 10059
	jsErrStreamNotMatch        = 10060
	jsErrJetStreamNotEnabled   = 10076
	jsErrStreamStoreFailed     = 10077 // see storeFailedIsTransient
	jsErrHeaderExceedsMaximum  = 10097
	jsErrStreamSealed          = 10109
	jsErrDuplicateInProcess    = 10158
	jsErrStreamTooManyRequests = 10167
)

// isAuthRefusal recognises a refusal of the credentials at CONNECT, which
// nats.go returns with the server's own words: its errors.Is compares the
// whole text, so "User Authentication Expired" never matched ErrAuthExpired
// ("authentication expired") and the boot waited out sink.boot_timeout.
func isAuthRefusal(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "authorization violation") ||
		strings.Contains(msg, "authentication expired") ||
		strings.Contains(msg, "authentication revoked")
}

// isTLSRefusal recognises a TLS failure that no retry can change: the server's
// certificate fails verification (an unknown authority, a name that does not
// match, an expired certificate -- nats.go wraps the handshake's
// tls.CertificateVerificationError in ErrTLS), or sink.nats.tls is on and the
// server offers no TLS (ErrSecureConnWanted). Both need the operator: a CA, a
// certificate, the configuration. A server that starts to DEMAND TLS is
// upgraded to by nats.go on its own, with the system roots when sink.nats.tls
// is off, and lands here only if that certificate does not verify. Other
// failures of the handshake -- a reset, a timeout -- stay network. As network,
// the boot waited out sink.boot_timeout to exit with the network's message, and
// after the boot the client stayed RECONNECTING with the sink DOWN until the
// fuses (go-relplog-nats-server, independent review, 2026-10-13, adapted by
// go-syslog-server to a client with TLS; SPEC §8.7.9).
func isTLSRefusal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrSecureConnWanted) {
		return true
	}
	var cv *tls.CertificateVerificationError
	return errors.As(err, &cv)
}

// classifyNATS maps a client or API error onto the class the state machine
// acts on. The table is SPEC §8.7.6; keep the two in step.
func classifyNATS(op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Class: classifyNATSError(err), Op: op, Err: err}
}

func classifyNATSError(err error) Class {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return ClassNetwork

	// Credentials refused, a connection closed for good, or a TLS that can
	// never succeed: nothing a retry can change.
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired),
		errors.Is(err, nats.ErrConnectionClosed), isAuthRefusal(err), isTLSRefusal(err):
		return ClassFatal

	// The stream is gone or JetStream is off: every batch would be refused.
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrJetStreamNotEnabled),
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return ClassFatal

	// One event does not fit. Unreachable after the boot check of SPEC
	// §8.7.5 unless the server changed underneath.
	case errors.Is(err, nats.ErrMaxPayload), errors.Is(err, jetstream.ErrMaxBytesExceeded):
		return ClassLogic

	// No answer, or a client between servers.
	case errors.Is(err, nats.ErrTimeout), errors.Is(err, nats.ErrNoResponders),
		errors.Is(err, nats.ErrDisconnected), errors.Is(err, nats.ErrConnectionReconnecting),
		errors.Is(err, nats.ErrReconnectBufExceeded), errors.Is(err, nats.ErrNoServers),
		errors.Is(err, jetstream.ErrNoStreamResponse), errors.Is(err, jetstream.ErrAsyncPublishTimeout),
		errors.Is(err, jetstream.ErrTooManyStalledMsgs):
		return ClassNetwork
	}

	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		if api := jsErr.APIError(); api != nil {
			if api.ErrorCode == jsErrStreamStoreFailed && storeFailedIsTransient(api.Description) {
				return ClassNetwork
			}
			switch api.ErrorCode {
			case jsErrStreamNotFound, jsErrJetStreamNotEnabled, jsErrJetStreamNotEnabledFA,
				jsErrStreamNotMatch, jsErrStreamSealed:
				return ClassFatal
			case jsErrInsufficientResources, jsErrStreamStoreFailed:
				return ClassFatal
			case jsErrMessageExceedsMaximum, jsErrHeaderExceedsMaximum:
				// One message does not fit. 10097 is unreachable from this
				// sink, whose header block is fixed and small, and it is logic
				// rather than the fatal of an unknown 4xx: were it ever
				// reached, it would be about that message, not the stream.
				return ClassLogic
			case jsErrDuplicateInProcess, jsErrStreamTooManyRequests:
				// Transient, though 4xx. 10158 (409): the same Nats-Msg-Id is
				// still in a Raft proposal -- the retry, after a deadline, of a
				// publish a clustered stream has not finished storing; it
				// passes on its own, and the retry then is a duplicate. 10167
				// (429): the stream's ingest queue is full. As the fatal of an
				// unknown 4xx either was a dump and exit 3, maybe on the whole
				// fleet at once.
				return ClassNetwork
			}
			// Any 429 is backpressure, never the request's form: the batch
			// publish inflight ceilings (10210, 10211) pass once the server
			// catches up, like 10167.
			if api.Code == 429 {
				return ClassNetwork
			}
			// Any other 4xx is the server refusing the request's form, which
			// repeats on every attempt; a 5xx is the server's own trouble.
			if api.Code >= 400 && api.Code < 500 {
				return ClassFatal
			}
			return ClassNetwork
		}
	}
	// Unknown is retried rather than dropped: a filling ring is visible, a
	// discarded batch is not.
	return ClassNetwork
}

// storeFailedIsTransient tells the 10077 of a stream that is merely full, or
// closing, from a store that cannot take the message at all. nats-server
// answers every store refusal with 10077 (processJetStreamMsg), and the
// description is the store's own error: "maximum messages exceeded", "maximum
// bytes exceeded" and "maximum messages per subject exceeded" are a stream
// with discard=new that is full -- backpressure, which clears as soon as a
// consumer acknowledges -- and "store is closed" a stream or server shutting
// down (a rolling restart of NATS). As fatal they were a dump and exit 3, and
// the stored part of the batch duplicated at reingestion (go-relplog-server,
// independent review, 2026-10-11). A record too large for the store stays
// fatal: retrying cannot make it fit, and validateSize refuses the
// configurations that produce it.
//
// The text is nats-server's own (server/store.go, ErrStoreClosed, 2.14.6:
// "store is closed"). The family matched "store closed" alone, a text the
// server never sends, so a stream closing was still FATAL (go-syslog-server
// review, 2026-10-13); "store closed" stays for an older wording.
func storeFailedIsTransient(desc string) bool {
	d := strings.ToLower(desc)
	return strings.Contains(d, "maximum messages exceeded") ||
		strings.Contains(d, "maximum bytes exceeded") ||
		strings.Contains(d, "maximum messages per subject exceeded") ||
		strings.Contains(d, "store is closed") ||
		strings.Contains(d, "store closed")
}
