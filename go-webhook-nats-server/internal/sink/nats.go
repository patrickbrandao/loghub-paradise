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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
)

// sink.nats.mode values (SPEC §8.7.2).
const (
	NATSModeJetStream = "jetstream"
	NATSModeCore      = "core"

	// DefaultCtlNamespace is loghub.ctl_namespace (SPEC §8.7.10).
	DefaultCtlNamespace = "loghub._ctl"
)

// NATSConfig mirrors [sink.nats].
type NATSConfig struct {
	URLs        []string
	Username    string
	Password    string
	Token       string
	Credentials string // path to a .creds file

	// Mode is jetstream (default) or core (best effort, SPEC §8.7.2).
	Mode string
	// Stream is validated at boot and never created; ignored in core.
	Stream string

	// Workspace is this instance's own space: the subject the boot validation
	// proves the stream covers. Envelopes are published on their OWN space,
	// read from the JLH (SPEC §3.8).
	Workspace string
	// CtlNamespace is loghub.ctl_namespace: the boot refuses a stream whose
	// subjects would capture <ctl_namespace>.beacon (SPEC §8.7.5, invariant
	// §17.21).
	CtlNamespace string

	ClientName     string
	ConnectTimeout time.Duration
	// OpTimeout bounds how long the client keeps an unanswered publish
	// pending, so a message -- and the item buffer it points at -- is never
	// held past the worker's own deadline (SPEC §8.7.2).
	OpTimeout time.Duration
	// MaxPending is the ceiling of async publishes in flight per process:
	// sink.workers × sink.batch (SPEC §8.7.2).
	MaxPending int
	// MaxEventBytes is the largest envelope one accepted request can produce
	// (SPEC §3.2). The boot compares it plus the header block with
	// max_payload, the stream's max_msg_size and the file store's record
	// ceiling (SPEC §8.7.5). Zero skips the size checks.
	MaxEventBytes int64
}

// Header names of SPEC §3.8. Nats-Msg-Id is the library's constant.
const (
	natsHeaderContentType = "Content-Type"
	natsHeaderType        = "X-LogHub-Type"
	natsHeaderUUID        = "X-LogHub-UUID"
	natsHeaderSpace       = "X-LogHub-Space"

	// natsHeaderMax bounds the header block this sink writes (SPEC §3.8):
	// NATS/1.0 (10), Nats-Msg-Id with "<uuid>.<step>" and a step of up to two
	// digits (54), Content-Type (32), X-LogHub-Type: webhook (24),
	// X-LogHub-UUID (53), X-LogHub-Space with a 64-byte space (82) and the
	// closing CRLF (2) come to 257. The block is small and fixed by
	// construction, which is the whole reason the JLH is not in it: JetStream
	// stores a header block's length in sixteen bits and refuses anything
	// above 64 KiB with 10097.
	natsHeaderMax = 257

	// natsExpectedStreamHeader is what WithExpectStream adds, the stream name
	// aside.
	natsExpectedStreamHeader = len("Nats-Expected-Stream: \r\n")

	// jsFileStoreMaxRecord is the largest record a file-backed stream stores,
	// whatever max_payload says: nats-server refuses a record above its
	// rlBadThresh of 32 MiB with a store failure, 10077, which is fatal here
	// (SPEC §8.7.6).
	jsFileStoreMaxRecord = 32 << 20

	// natsCoveredMax bounds the set of foreign subjects known to be covered
	// (cover).
	natsCoveredMax = 1024
)

// NATS is the only destination (SPEC §8.7). Message shape in §3.8.
//
// One connection per process, multiplexed by the workers and by the beacon of
// SPEC §8.7.10, opened lazily by the first health check. It is always
// plaintext: this server has no TLS (SPEC §1.2). In jetstream mode every
// publish is acknowledged by the stream; in core mode success only means the
// server received the messages.
//
// The shape is go-syslog-nats-server's sink, with the corrections of its
// independent review; the headers read from the JLH and the cover check of a
// foreign subject come from go-webhook-fast-server's (SPEC §8.7).
type NATS struct {
	cfg NATSConfig
	log *slog.Logger
	m   *obs.Metrics

	opts   []nats.Option
	jsOpts []jetstream.JetStreamOpt

	// mu guards the lazy connect; nc and js are set once and never replaced.
	mu sync.Mutex
	nc *nats.Conn
	js jetstream.JetStream

	hookMu sync.Mutex
	hooks  []func()

	// own is the subject of this instance's space, precomputed so the hot
	// path never allocates for it. subjects caches the subject of every other
	// space seen: those come only from emergency dumps an operator put in the
	// directory, so the map stays small.
	own      string
	subjects sync.Map // string -> string

	// covered holds the foreign subjects the configured stream was found to
	// listen on (cover). A subject past natsCoveredMax is just asked again.
	coverMu sync.Mutex
	covered map[string]struct{}

	// blind holds the foreign subjects core mode has warned about: published
	// with no stream to ask (SPEC §8.7.5).
	blind sync.Map // string -> struct{}

	// asyncErrLog samples the client's asynchronous errors. A publish
	// permission denied produces one per message, and SPEC §11.3 allows no
	// per-message log line on the default path.
	asyncErrLog *obs.Sampler

	validated atomic.Bool
	closed    atomic.Bool

	// verdict is a permanent refusal the client reported asynchronously -- a
	// permissions violation on the data subjects or the JetStream API, a
	// reconnection to a server that demands TLS -- which no synchronous call
	// returns: a denied publish only times out (jetstream) or vanishes (core),
	// and a denied stream info only times out. Health turns it into FATAL.
	verdict atomic.Pointer[error]
}

// NewNATS prepares the client without connecting: the connection is opened by
// the first health check, and the boot's WaitReady owns the retry loop
// (SPEC §8.7.1). The errors here are configuration errors -- a credentials
// file that does not exist, two authentication methods -- never a network
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
	if cfg.Mode == "" {
		cfg.Mode = NATSModeJetStream
	}
	if cfg.CtlNamespace == "" {
		cfg.CtlNamespace = DefaultCtlNamespace
	}
	if len(cfg.URLs) == 0 {
		return nil, errors.New("sink nats: no server URL")
	}
	// nats.go dials tls:// and wss:// with TLS, ws:// as a websocket, and a
	// bare host:port as nats://. The configuration admits only nats://
	// (SPEC §1.2, §8.7.1); the sink holds the same line on its own, so no path
	// into it -- a test, a future caller -- reaches a TLS dial. The URL itself
	// is never echoed: it may carry credentials.
	for _, list := range cfg.URLs {
		for _, u := range strings.Split(list, ",") {
			scheme, _, ok := strings.Cut(strings.TrimSpace(u), "://")
			if !ok || !strings.EqualFold(scheme, "nats") {
				return nil, errors.New("sink nats: every server URL must be nats:// " +
					"(tls://, ws://, wss:// and a bare host:port are refused: this server has no TLS)")
			}
		}
	}
	switch cfg.Mode {
	case NATSModeJetStream:
		if cfg.Stream == "" {
			return nil, errors.New("sink nats: stream is required in jetstream mode")
		}
	case NATSModeCore:
	default:
		return nil, fmt.Errorf("sink nats: mode %q is neither jetstream nor core", cfg.Mode)
	}
	if cfg.Workspace == "" {
		return nil, errors.New("sink nats: workspace is required to derive the subject")
	}
	methods := 0
	if cfg.Username != "" || cfg.Password != "" {
		if cfg.Username == "" {
			return nil, errors.New("sink nats: password without username")
		}
		methods++
	}
	if cfg.Token != "" {
		methods++
	}
	if cfg.Credentials != "" {
		methods++
	}
	if methods > 1 {
		return nil, errors.New("sink nats: more than one authentication method (username/password, token, credentials); use exactly one")
	}

	n := &NATS{cfg: cfg, log: log, m: m, asyncErrLog: obs.NewSampler(30 * time.Second),
		own: subjectOf(cfg.Workspace)}
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
		// accounting, to the 429 and to the dump (invariant §17.19).
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			// Our own Close disconnects too, with no error. A WARN on every
			// clean shutdown would teach an operator to ignore the one that
			// means an outage.
			if n.closed.Load() {
				return
			}
			log.Warn("nats disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			m.SinkNATSReconnectsTotal.Inc()
			log.Warn("nats reconnected", "url", connectedURL(nc))
			// SPEC §8.7.10: a beacon on every reconnection.
			n.fireHooks()
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
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
			// Without this the eventual FATAL of a server that started to
			// demand TLS after the boot read "connection is RECONNECTING".
			if errors.Is(err, errNATSServerRequiresTLS) {
				n.setVerdict(err)
			}
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
	// No nats.Secure and no TLS handshake first: this server has no TLS
	// (SPEC §1.2), and a tls:// URL is refused above. That alone is not
	// enough: nats.go UPGRADES to TLS on its own when the server's INFO says
	// tls_required, with a default tls.Config and the system roots. The one
	// hook it consults before the handshake is TLSCertCB, so the sink sets it
	// -- and nothing else of TLS, which would turn Secure on -- to a refusal:
	// the connection ends before a ClientHello is written, with a fatal
	// verdict that names the cause (invariant §17.22).
	opts = append(opts, func(o *nats.Options) error {
		o.TLSCertCB = func() (tls.Certificate, error) { return tls.Certificate{}, errNATSServerRequiresTLS }
		return nil
	})

	n.opts = opts
	n.jsOpts = []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncMaxPending(cfg.MaxPending),
		// Without a timeout a future the server never answers stays pending
		// forever, holding the message and the item buffer it points at.
		jetstream.WithPublishAsyncTimeout(cfg.OpTimeout),
	}
	return n, nil
}

func (n *NATS) Type() string { return "nats" }

// subjectOf is the subject of one space: loghub.<space>.webhook (SPEC §3.8).
// It is derived, never configured: the rule loghub.{space}.{type} is the
// JLH's.
func subjectOf(space string) string { return "loghub." + space + "." + envelope.Type }

// asyncError logs an asynchronous client error, at most once per interval,
// with the count of what was suppressed in between, and keeps a permanent
// refusal for Health (see verdict).
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

// deniesTheDataPath reports a permissions violation on what the data path
// needs: a publish to a loghub.<space>.webhook subject, or anything on the
// JetStream API ($JS.API.>, the stream info of Health). The replies -- the
// publish acks and the API's answers -- arrive on the client's _INBOX, and a
// subscription denied there is not a verdict yet (SPEC §18.1). A denial on the
// beacon's subject is NOT one: the control plane failing must not take the
// data down with it (SPEC §8.7.10).
func deniesTheDataPath(err error) bool {
	if !errors.Is(err, nats.ErrPermissionViolation) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "$JS.API") {
		return true
	}
	i := strings.Index(msg, `"loghub.`)
	return i >= 0 && strings.Contains(msg[i:], "."+envelope.Type+`"`)
}

// connect opens the connection on first use and then runs the connect hooks
// (SPEC §8.7.10: a beacon on the first connection).
func (n *NATS) connect() (*nats.Conn, jetstream.JetStream, error) {
	n.mu.Lock()
	if n.nc != nil {
		nc, js := n.nc, n.js
		n.mu.Unlock()
		return nc, js, nil
	}
	nc, err := nats.Connect(strings.Join(n.cfg.URLs, ","), n.opts...)
	if err != nil {
		n.mu.Unlock()
		return nil, nil, classifyNATS("connect", err)
	}
	var js jetstream.JetStream
	if n.cfg.Mode == NATSModeJetStream {
		js, err = jetstream.New(nc, n.jsOpts...)
		if err != nil {
			nc.Close()
			n.mu.Unlock()
			return nil, nil, classifyNATS("jetstream", err)
		}
	}
	n.nc, n.js = nc, js
	n.mu.Unlock()

	n.log.Info("nats connected", "url", connectedURL(nc), "mode", n.cfg.Mode,
		"max_payload", nc.MaxPayload())
	// Outside the lock: a hook that called back into this sink must not
	// deadlock against the connect that triggered it.
	n.fireHooks()
	return nc, js, nil
}

// OnConnect registers fn to run after the first connection and after every
// reconnection: what the beacon of SPEC §8.7.10 needs. fn runs on the
// connecting goroutine or on the client's callback goroutine, so it must not
// block; the beacon's Kick only signals its own goroutine. When the connection
// is already up, fn also runs once right away, so registering late does not
// lose the beacon of the first connection.
func (n *NATS) OnConnect(fn func()) {
	n.hookMu.Lock()
	n.hooks = append(n.hooks, fn)
	n.hookMu.Unlock()

	n.mu.Lock()
	nc := n.nc
	n.mu.Unlock()
	if nc != nil && nc.IsConnected() {
		fn()
	}
}

func (n *NATS) fireHooks() {
	n.hookMu.Lock()
	hooks := append([]func(){}, n.hooks...)
	n.hookMu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

// PublishCore publishes data on subject by core NATS, on the sink's own
// connection: the beacon's path (SPEC §8.7.10), fanout and never JetStream,
// because the data stream must not cover the control plane. It does not
// connect: before the first health check there is no connection, and the
// beacon only starts after the boot has one.
func (n *NATS) PublishCore(subject string, data []byte) error {
	n.mu.Lock()
	nc := n.nc
	n.mu.Unlock()
	if nc == nil || n.closed.Load() {
		return networkErr("publish", errors.New("not connected"))
	}
	if err := nc.Publish(subject, data); err != nil {
		return classifyNATS("publish", err)
	}
	return nil
}

// connectedURL is the server this connection reached, scheme and host only.
// nats.go's ConnectedUrlRedacted hides a password and nothing else: a URL
// whose user part is a token (nats://token@host, SPEC §8.7.9) came out whole,
// in the INFO line of every boot and the WARN of every reconnect.
func connectedURL(nc *nats.Conn) string {
	u, err := url.Parse(nc.ConnectedUrl())
	if err != nil || u.Host == "" {
		return nc.ConnectedAddr()
	}
	return u.Scheme + "://" + u.Host
}

// ServerAddr returns host:port of the server the connection is on, or "" when
// it is not connected. The beacon dials it by UDP, without traffic, to learn
// which local address routes there (SPEC §8.7.10).
func (n *NATS) ServerAddr() string {
	n.mu.Lock()
	nc := n.nc
	n.mu.Unlock()
	if nc == nil {
		return ""
	}
	return nc.ConnectedAddr()
}

// subject is where an envelope of space goes. The own space is the hot path
// and never allocates; any other comes from a dump and is cached.
func (n *NATS) subject(space string) string {
	if space == n.cfg.Workspace {
		return n.own
	}
	if v, ok := n.subjects.Load(space); ok {
		return v.(string)
	}
	v, _ := n.subjects.LoadOrStore(space, subjectOf(space))
	return v.(string)
}

// message builds the wire shape of SPEC §3.8 from the envelope's own header.
//
// Data is the item's JLH slice, not a copy: the library serialises it to the
// socket inside the publish call and, with retries disabled, never reads it
// again. The headers are short strings of their own, in a literal map:
// nats.Header keeps the names exactly as written.
//
// Nats-Msg-Id is <uuid>.<step>, NEVER the bare uuid: a processor forwarding to
// the same stream (same uuid, step + 1) would otherwise have its legitimate
// hop discarded as a duplicate by the JetStream window.
func (n *NATS) message(it *envelope.Item) (*nats.Msg, error) {
	head, _, err := envelope.ParseHead(it.Env)
	if err != nil {
		return nil, err
	}
	if head.Type != envelope.Type {
		// The handler writes only webhook envelopes and the dump gate admits
		// only those (SPEC §9.2); anything else would land on a subject of
		// another type with this server's headers.
		return nil, fmt.Errorf("envelope type %q is not %q", head.Type, envelope.Type)
	}
	h := nats.Header{
		jetstream.MsgIDHeader: {it.ID + "." + strconv.Itoa(head.Step)},
		natsHeaderContentType: {"application/json"},
		natsHeaderType:        {head.Type},
		natsHeaderUUID:        {it.ID},
		natsHeaderSpace:       {head.Space},
	}
	return &nats.Msg{Subject: n.subject(head.Space), Header: h, Data: it.Env}, nil
}

// Write publishes the batch (SPEC §8.7.2).
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

	// Every message is built before the first is published. One that does not
	// map is refused alone (rejected), like a message too large: the rest of
	// the batch is still worth delivering (SPEC §8.7.2).
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
	if n.cfg.Mode == NATSModeCore {
		return n.writeCore(ctx, nc, msgs, b)
	}
	return n.writeJetStream(ctx, js, msgs, b)
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

// writeJetStream publishes one PublishMsgAsync per envelope, and only once the
// N futures are in the air waits for the N acknowledgements, under the
// sink.op_timeout of the batch.
//
// Every error after the first publish taints the batch: futures may still be
// pending, and the buffers must be left to the GC (SPEC §8.7.2, §12.3).
func (n *NATS) writeJetStream(ctx context.Context, js jetstream.JetStream, msgs []*nats.Msg, b *batchResult) error {
	// The own subject was validated at boot; any other is checked once,
	// before anything of the batch is published (SPEC §8.7.5).
	for _, msg := range msgs {
		if msg != nil && msg.Subject != n.own {
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
			// The server rejects the message if the subject belongs to another
			// stream: a configuration mistake becomes a refusal, not messages
			// in the wrong place.
			jetstream.WithExpectStream(n.cfg.Stream),
			// No retries inside the library: they would re-read msg.Data after
			// Write returned, which is the buffer race SPEC §12.3 forbids. The
			// worker retries, still owning the buffer (SPEC §10.4).
			jetstream.WithRetryAttempts(0))
		if err != nil {
			if classifyNATSError(err) == ClassLogic {
				// Refused by the client before it left (ErrMaxPayload): this
				// message alone. Stopping here left every later envelope
				// unpublished yet confirmed, and counted the earlier ones lost
				// (independent review of go-syslog-nats-server).
				b.reject(i, "publish", err)
				continue
			}
			if published > 0 {
				b.taint()
			}
			return classifyNATS("publish", err)
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
				return classifyNATS(op, err)
			}
			// One message refused by the stream (10054): the other futures say
			// what the stream stored.
			b.reject(i, "ack", err)
		case <-ctx.Done():
			b.taint()
			return networkErr("ack", ctx.Err())
		}
	}
	if dups > 0 {
		// Inside the stream's duplicate_window a redelivery is acknowledged as
		// a duplicate and stored once; outside it, it duplicates
		// (at-least-once, SPEC §8.7.3).
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

// writeCore publishes by core NATS and flushes once per batch (SPEC §8.7.2).
// The client copies Data into its write buffer inside PublishMsg, so nothing
// reads the item buffers after this returns. Success means the server
// RECEIVED the messages, not that anyone stored them.
//
// A message refused before it leaves (ErrMaxPayload: larger than this
// server's max_payload) is refused alone, as in jetstream mode: the rest of
// the batch is still published, and only the refused ones are lost.
func (n *NATS) writeCore(ctx context.Context, nc *nats.Conn, msgs []*nats.Msg, b *batchResult) error {
	for i, msg := range msgs {
		if msg == nil {
			continue
		}
		if msg.Subject != n.own {
			// A dump of another space. Core mode has no stream to ask whether
			// anyone listens (cover is jetstream's), and the message is lost
			// unless something subscribes to another tenant's subject. Said
			// once per subject, so the loss is at least not silent.
			if _, seen := n.blind.LoadOrStore(msg.Subject, struct{}{}); !seen {
				n.log.Warn("nats core mode: publishing an emergency dump of another space; "+
					"with no stream behind it, it is lost unless something subscribes to its "+
					"subject (docs/SPEC.md §8.7.5)", "subject", msg.Subject, "own", n.own)
			}
		}
		if err := nc.PublishMsg(msg); err != nil {
			if classifyNATSError(err) == ClassLogic {
				b.reject(i, "publish", err)
				continue
			}
			return classifyNATS("publish", err)
		}
	}
	if err := n.flush(ctx, nc); err != nil {
		return classifyNATS("flush", err)
	}
	// A denied publish never fails PublishMsg: the server answers with an
	// -ERR, and it comes before the PONG of the flush, so LastError holds it
	// by now, without a race. Without this, a batch the server refused was
	// confirmed, counted as written and released -- lost -- and so was every
	// batch until the next health check turned the verdict into FATAL.
	if last := nc.LastError(); deniesTheDataPath(last) {
		n.setVerdict(last)
	}
	if err := n.verdictErr("publish"); err != nil {
		return err
	}
	return b.err()
}

// flush bounds the round trip by the caller's deadline, or by sink.op_timeout
// when the context has none.
func (n *NATS) flush(ctx context.Context, nc *nats.Conn) error {
	if _, ok := ctx.Deadline(); ok {
		return nc.FlushWithContext(ctx)
	}
	return nc.FlushTimeout(n.cfg.OpTimeout)
}

// cover checks, once per subject, that the configured stream listens on the
// subject of an envelope from another space (SPEC §8.7.5). The boot validates
// only this instance's own subject, and the subject is derived per envelope
// (SPEC §3.8): an emergency dump from a life with another Workspace lands on a
// subject no stream may cover. Published blind, it got no answer -- "no
// responders", network -- and the boot gave up with exit 3 after
// sink.boot_timeout, restart after restart, with an error that never named the
// cause. Asked first, it is fatal and names the subject.
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
			"no stream listens on subject %q, the subject of an envelope of space %q (an emergency "+
				"dump from another Workspace): stream %q must also cover that subject until the dump "+
				"drains (SPEC §8.7.5)", subject, space, n.cfg.Stream))
	}
	if err != nil {
		return classifyNATS("publish", err)
	}
	if owner != n.cfg.Stream {
		// The expected-stream header would refuse it too (10060); asked
		// first, the error says which stream took the subject.
		return fatalErr("publish", fmt.Errorf(
			"subject %q, of an envelope of space %q, belongs to stream %q, not %q: stream %q must "+
				"cover that subject until the dump drains (SPEC §8.7.5)",
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

// Health checks the connection state and, in jetstream mode, asks the stream
// for its info; in core mode it flushes (SPEC §8.7.7). A PING would answer
// happily with JetStream disabled, and the state would never leave UP.
//
// Every check also runs the contract of SPEC §8.7.5 against what it has just
// fetched.
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
		return networkErr("health", fmt.Errorf("connection is %s", st))
	}

	if n.cfg.Mode == NATSModeCore {
		if err := n.flush(ctx, nc); err != nil {
			return classifyNATS("flush", err)
		}
		// On every check, not once: a reconnection may land on a server
		// with a smaller max_payload (SPEC §8.7.5).
		if err := n.validateSize(nc.MaxPayload(), nil); err != nil {
			return err
		}
		if !n.validated.Swap(true) {
			n.log.Info("nats core mode: best effort, a message nobody subscribes to is lost",
				"largest_message", n.maxMessageBytes(), "max_payload", nc.MaxPayload())
		}
		return nil
	}

	stream, err := js.Stream(ctx, n.cfg.Stream)
	if err != nil {
		return n.streamErr(err)
	}
	// The whole contract on every check, against the info this call has
	// already fetched (no extra round trip). Checked once, a stream edited
	// underneath -- its subjects narrowed, no_ack turned on -- answered every
	// publish with a timeout, network, retried forever while this check kept
	// the sink UP and the ring filled (SPEC §8.7.5). validated only gates the
	// log line and the no-responders verdict of streamErr.
	info := stream.CachedInfo()
	if err := n.validate(nc.MaxPayload(), info); err != nil {
		return err
	}
	if !n.validated.Swap(true) {
		cfg := info.Config
		n.log.Info("stream validated",
			"stream", n.cfg.Stream, "subjects", cfg.Subjects, "covers", n.own,
			"storage", cfg.Storage.String(), "replicas", cfg.Replicas,
			"duplicate_window", cfg.Duplicates.String(),
			"largest_message", n.maxMessageBytes(), "max_payload", nc.MaxPayload())
	}
	return nil
}

// streamErr classifies a failure to fetch the stream's info.
func (n *NATS) streamErr(err error) error {
	if !n.validated.Load() && errors.Is(err, nats.ErrNoResponders) {
		// Nobody answers the JetStream API at the first check: JetStream is
		// off on this server (an account without it answers 10039, fatal on
		// its own), or no JetStream server is reachable from it -- a leaf
		// whose hub is down. The client cannot tell the two apart:
		// jetstream's AccountInfo, asked here until 2026-10-11, maps no
		// responders to "not enabled" itself, and its request to
		// $JS.API.INFO, denied to a minimal role, became a permanent
		// permission verdict (the review of the port into
		// go-webhook-fast-server). Either way nothing can be confirmed now,
		// and as network the boot waited out sink.boot_timeout to exit with
		// the wrong reason (SPEC §8.7.5). After a validated check, the same
		// answer is a blip: network.
		return fatalErr("validate", fmt.Errorf(
			"nobody answers the JetStream API: JetStream is disabled on this server, or no "+
				"JetStream server is reachable from it; sink.nats.mode = jetstream needs it "+
				"(docs/SPEC.md §8.7.5): %w", err))
	}
	return classifyNATS("stream", err)
}

// maxMessageBytes is what the largest message this server can publish counts
// against max_payload: the largest envelope in Data, plus the header block,
// whose Nats-Expected-Stream carries the stream name in jetstream mode.
func (n *NATS) maxMessageBytes() int64 {
	need := n.cfg.MaxEventBytes + natsHeaderMax
	if n.cfg.Mode == NATSModeJetStream {
		need += int64(natsExpectedStreamHeader + len(n.cfg.Stream))
	}
	return need
}

// validate is the contract of SPEC §8.7.5 in jetstream mode, checked by every
// Health call, against the
// stream's info and the connected server's max_payload. Everything it refuses
// is ClassFatal: the stream is created by whoever runs NATS, from
// docker/nats/stream.json, and this server never creates or alters one
// (invariant §17.17).
func (n *NATS) validate(maxPayload int64, info *jetstream.StreamInfo) error {
	if info == nil {
		return networkErr("validate", errors.New("stream info not available"))
	}
	cfg := info.Config
	if cfg.Sealed {
		return fatalErr("validate", fmt.Errorf("stream %q is sealed and accepts no publishes", n.cfg.Stream))
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

	// Local pattern matching, not a server lookup by subject: the same rule as
	// the control-plane check below, with no extra round trip. Two streams of
	// one account cannot overlap, so a covering pattern here means no other
	// stream owns the subject.
	if !coveredBy(cfg.Subjects, n.own) {
		return fatalErr("validate", fmt.Errorf(
			"stream %q (subjects %v) does not cover %q; apply docker/nats/stream.json for this space",
			n.cfg.Stream, cfg.Subjects, n.own))
	}

	// A stream that captures the control plane would store every beacon and
	// replay old ones to every new consumer (invariant §17.21). loghub.> is the
	// classic way to get there.
	beacon := n.cfg.CtlNamespace + ".beacon"
	for _, p := range cfg.Subjects {
		if subjectCovers(p, beacon) {
			return fatalErr("validate", fmt.Errorf(
				"stream %q subject %q captures the control plane subject %q; the data stream must "+
					"never cover it (use loghub.<space>.webhook, never loghub.>)",
				n.cfg.Stream, p, beacon))
		}
	}

	// A subject of another space found covered (cover) and no longer
	// covered is forgotten, not refused: only the boot's reingestion of a
	// dump from another Workspace publishes there, and an operator narrowing
	// the stream once that dump drained -- what cover's own error tells them
	// they may do -- must not take a healthy process down with a FATAL and a
	// dump of the ring. The next publish there, if any, asks again and is
	// refused then, naming the subject (SPEC §8.7.5).
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

// validateSize compares the largest message with the server's max_payload and,
// with a stream, its max_msg_size and the file store's record ceiling (SPEC
// §8.7.5, invariant §17.20). A message accepted with 200 and then refused for
// its size would be logic: lost.
func (n *NATS) validateSize(maxPayload int64, cfg *jetstream.StreamConfig) error {
	if n.cfg.MaxEventBytes <= 0 {
		return nil
	}
	need := n.maxMessageBytes()
	if mp := maxPayload; mp < need {
		return fatalErr("validate", fmt.Errorf(
			"nats-server max_payload=%d is below the largest message this server can publish "+
				"(%d bytes: the largest envelope of %d and the headers; docs/SPEC.md §8.7.5); raise "+
				"max_payload or lower server.max_body_bytes", mp, need, n.cfg.MaxEventBytes))
	}
	if cfg == nil {
		return nil
	}
	if ms := int64(cfg.MaxMsgSize); ms > 0 && ms < need {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_msg_size=%d is below the largest message this server can publish "+
				"(%d bytes; docs/SPEC.md §8.7.5); raise it or lower server.max_body_bytes",
			n.cfg.Stream, ms, need))
	}
	// A stream that refuses new messages when full, smaller than one message:
	// the first large event is 10077, fatal, and every boot after the dump
	// validates and then fails its reingestion the same way.
	if record := need + int64(len(n.own)) + 64; cfg.Discard == jetstream.DiscardNew &&
		cfg.MaxBytes > 0 && cfg.MaxBytes < record {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_bytes=%d with discard=new is below the largest message this server can "+
				"publish (%d bytes; docs/SPEC.md §8.7.5); raise max_bytes or lower server.max_body_bytes",
			n.cfg.Stream, cfg.MaxBytes, record))
	}
	// A file store has a record ceiling of its own, below max_payload's upper
	// range, and past it the refusal is 10077, fatal: sizing max_payload up to
	// 64 MB would otherwise turn one large body into the end of the process.
	if record := need + int64(len(n.own)) + 64; cfg.Storage == jetstream.FileStorage &&
		record > jsFileStoreMaxRecord {
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
	jsErrJetStreamNotEnabled   = 10076
	jsErrJetStreamNotEnabledFA = 10039
	jsErrMessageExceedsMaximum = 10054
	jsErrStreamNotFound        = 10059
	jsErrStreamNotMatch        = 10060
	jsErrStreamStoreFailed     = 10077
	jsErrHeaderExceedsMaximum  = 10097
	jsErrStreamSealed          = 10109
)

// errNATSServerRequiresTLS is the verdict on a server whose INFO says
// tls_required. This server has no TLS (SPEC §1.2), and such a server will
// never take its plaintext, so it is fatal: the boot stops with this reason
// instead of retrying until sink.boot_timeout.
var errNATSServerRequiresTLS = errors.New("the NATS server requires TLS, and this server has no TLS " +
	"(docs/SPEC.md §8.7.9); point sink.nats.urls at a listener without TLS")

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

	// Credentials refused, a connection closed for good, or a server that
	// demands the TLS this server does not have: nothing a retry can change.
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired),
		errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, errNATSServerRequiresTLS),
		isAuthRefusal(err):
		return ClassFatal

	// The stream is gone or JetStream is off: every batch would be refused.
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrJetStreamNotEnabled),
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return ClassFatal

	// One message does not fit. Unreachable after the boot check of SPEC
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
			switch api.ErrorCode {
			case jsErrStreamNotFound, jsErrJetStreamNotEnabled, jsErrJetStreamNotEnabledFA,
				jsErrStreamNotMatch, jsErrStreamSealed,
				jsErrInsufficientResources, jsErrStreamStoreFailed:
				return ClassFatal
			case jsErrMessageExceedsMaximum, jsErrHeaderExceedsMaximum:
				// One message does not fit. 10097 is unreachable from this
				// sink, whose header block is fixed and small, and it is logic
				// rather than the fatal of an unknown 4xx: were it ever
				// reached, it would be about that message, not the stream.
				return ClassLogic
			}
			// A 4xx is the server refusing the request's form, which repeats on
			// every attempt; a 5xx is the server's own trouble.
			if api.Code >= 400 && api.Code < 500 {
				return ClassFatal
			}
			return ClassNetwork
		}
	}
	// Unknown is retried rather than dropped: a filling ring is visible, a
	// discarded batch is not (invariant §17.18).
	return ClassNetwork
}
