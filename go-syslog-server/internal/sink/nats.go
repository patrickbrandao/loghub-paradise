package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// sink.nats.mode and sink.nats.subjects values (SPEC §10.7).
const (
	NATSModeJetStream = "jetstream"
	NATSModeCore      = "core"

	NATSSubjectsExact            = "exact"
	NATSSubjectsFacilitySeverity = "facility-severity"

	// DefaultCtlNamespace is loghub.ctl_namespace (SPEC §11).
	DefaultCtlNamespace = "loghub._ctl"
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

	// Mode is jetstream (default) or core (best effort, SPEC §10.7).
	Mode string
	// Stream is validated at boot and never created; ignored in core.
	Stream string
	// Subjects is exact (default) or facility-severity (SPEC §3.8).
	Subjects string

	// Workspace is this instance's own space: the subject the boot validation
	// proves the stream covers. Envelopes are published on their OWN space,
	// from the item head.
	Workspace string
	// CtlNamespace is loghub.ctl_namespace: the boot refuses a stream whose
	// subjects would capture <ctl_namespace>.beacon (SPEC §10.7, invariant
	// §20.21).
	CtlNamespace string

	ClientName     string
	ConnectTimeout time.Duration
	// OpTimeout bounds how long the client keeps an unanswered publish pending,
	// so a message -- and the chunk buffer it points at -- is never held past
	// the worker's own deadline.
	OpTimeout time.Duration
	// MaxPending is the ceiling of async publishes in flight per process:
	// sink.workers × sink.batch + ring.max_inflight (SPEC §10.7).
	MaxPending int
	// MaxEventBytes is the worst JLH (MaxEventBytes(L), SPEC §3.9), computed by
	// the app. The boot compares it plus the header block with max_payload, the
	// stream's max_msg_size and the file store's record ceiling. Zero skips the
	// size checks.
	MaxEventBytes int64
}

// Header names of SPEC §3.8. Nats-Msg-Id is the library's constant.
const (
	natsHeaderContentType = "Content-Type"
	natsHeaderType        = "X-LogHub-Type"
	natsHeaderUUID        = "X-LogHub-UUID"
	natsHeaderSpace       = "X-LogHub-Space"

	// natsHeaderMax bounds the header block this sink writes (SPEC §3.8):
	// NATS/1.0 (10), Nats-Msg-Id with "<uuid>.<step>" (54), Content-Type (32),
	// X-LogHub-Type (23), X-LogHub-UUID (53), X-LogHub-Space with a 64-byte
	// space (82) and the closing CRLF (2) come to 256. The block is small and
	// fixed by construction, which is the whole reason the JLH is not in it:
	// JetStream stores a header block's length in sixteen bits and refuses
	// anything above 64 KiB with 10097.
	natsHeaderMax = 256

	// natsExpectedStreamHeader is what WithExpectStream adds, the stream name
	// aside.
	natsExpectedStreamHeader = len("Nats-Expected-Stream: \r\n")

	// natsGranularSuffixMax is the longest ".<facility>.<severity>" of the
	// facility-severity subjects: ".authpriv.warning".
	natsGranularSuffixMax = len(".authpriv.warning")

	// jsFileStoreMaxRecord is the largest record a file-backed stream stores,
	// whatever max_payload says: nats-server refuses a record above its
	// rlBadThresh of 32 MiB with a store failure, 10077, which is fatal here
	// (SPEC §10.7).
	jsFileStoreMaxRecord = 32 << 20
)

// NATS is the default destination (SPEC §10.7). Message shape in §3.8.
//
// One connection per process, multiplexed by the workers and by the beacon of
// SPEC §11, opened lazily by the first health check. In jetstream mode every
// publish is acknowledged by the stream, and that acknowledgement is what gives
// the confirmation of ring=none its meaning; in core mode success only means
// the server received the messages.
//
// The JLH travels in Data, alone, never in a header: an envelope with one long
// hostname or a few kilobytes of structured data would pass the 64 KiB header
// ceiling and turn one emitter into the end of the process (the standard
// server learned it against a real server).
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

	// subjects caches the subject set of each space seen. The space comes from
	// the item head, written by the identity of the instance that RECEIVED the
	// message -- never by an emitter -- so the set of keys is the set of
	// Workspaces sharing a ring, which an operator controls.
	subjects sync.Map // string -> *natsSubjects

	// asyncErrLog samples the client's asynchronous errors. A publish
	// permission denied produces one per message, and SPEC §14.2 allows no
	// per-message log line on the default path.
	asyncErrLog *obs.Sampler

	validated atomic.Bool
	closed    atomic.Bool
}

// NewNATS prepares the client without connecting: the connection is opened by
// the first health check, and the boot's WaitReady owns the retry loop
// (SPEC §10.7). The errors here are configuration errors -- a CA file that does
// not load, a credentials file that does not exist, two authentication methods
// -- never a network condition.
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
	if cfg.Subjects == "" {
		cfg.Subjects = NATSSubjectsExact
	}
	if cfg.CtlNamespace == "" {
		cfg.CtlNamespace = DefaultCtlNamespace
	}
	if len(cfg.URLs) == 0 {
		return nil, errors.New("sink nats: no server URL")
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
	switch cfg.Subjects {
	case NATSSubjectsExact, NATSSubjectsFacilitySeverity:
	default:
		return nil, fmt.Errorf("sink nats: subjects %q is neither exact nor facility-severity", cfg.Subjects)
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

	n := &NATS{cfg: cfg, log: log, m: m, asyncErrLog: obs.NewSampler(30 * time.Second)}
	opts := []nats.Option{
		nats.Name(cfg.ClientName),
		nats.Timeout(cfg.ConnectTimeout),
		// The health machine has the fuses that decide when to give up; a
		// client that closed itself after N attempts would turn a long outage
		// into an unrecoverable state with no emergency dump (SPEC §10.7).
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		// No reconnect buffer: the ring is the only buffer of this design. A
		// publish during a disconnect fails at once with the network class
		// instead of queueing inside the client, invisible to the ring's
		// accounting, to the transports' backpressure and to the dump
		// (invariant §20.19).
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
			log.Warn("nats reconnected", "url", nc.ConnectedUrlRedacted())
			// SPEC §11: a beacon on every reconnection.
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

	n.opts = opts
	n.jsOpts = []jetstream.JetStreamOpt{
		jetstream.WithPublishAsyncMaxPending(cfg.MaxPending),
		// Without a timeout a future the server never answers stays pending
		// forever, holding the message and the chunk buffer it points at.
		jetstream.WithPublishAsyncTimeout(cfg.OpTimeout),
	}
	return n, nil
}

func (n *NATS) Type() string { return "nats" }

// asyncError logs an asynchronous client error, at most once per interval,
// with the count of what was suppressed in between.
func (n *NATS) asyncError(err error) {
	if ok, suppressed := n.asyncErrLog.Allow(); ok {
		n.log.Error("nats async error", "error", err, "suppressed", suppressed)
	}
}

// connect opens the connection on first use and then runs the connect hooks
// (SPEC §11: a beacon on the first connection).
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

	n.log.Info("nats connected", "url", nc.ConnectedUrlRedacted(), "mode", n.cfg.Mode,
		"max_payload", nc.MaxPayload())
	// Outside the lock: a hook that called back into this sink must not
	// deadlock against the connect that triggered it.
	n.fireHooks()
	return nc, js, nil
}

// OnConnect registers fn to run after the first connection and after every
// reconnection: what the beacon of SPEC §11 needs. fn runs on the connecting
// goroutine or on the client's callback goroutine, so it must not block; the
// beacon's Kick only signals its own goroutine. When the connection is already
// up, fn also runs once right away, so registering late does not lose the
// beacon of the first connection.
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
// connection: the beacon's path (SPEC §11), fanout and never JetStream, because
// the data stream must not cover the control plane. It does not connect: before
// the first health check there is no connection, and the beacon only starts
// after the boot has one.
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

// ServerAddr returns host:port of the server the connection is on, or "" when
// it is not connected. The beacon dials it by UDP, without traffic, to learn
// which local address routes there (SPEC §11).
func (n *NATS) ServerAddr() string {
	n.mu.Lock()
	nc := n.nc
	n.mu.Unlock()
	if nc == nil {
		return ""
	}
	return nc.ConnectedAddr()
}

// natsSubjects is the subject set of one space (SPEC §3.8).
type natsSubjects struct {
	exact    string
	granular [192]string // indexed by priority: facility × 8 + severity
}

func (n *NATS) subjectsFor(space string) *natsSubjects {
	if v, ok := n.subjects.Load(space); ok {
		return v.(*natsSubjects)
	}
	s := &natsSubjects{exact: "loghub." + space + ".syslog"}
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		for pri := range s.granular {
			s.granular[pri] = s.exact + "." + convert.FacilityNames[pri/8] + "." + convert.SeverityNames[pri%8]
		}
	}
	v, _ := n.subjects.LoadOrStore(space, s)
	return v.(*natsSubjects)
}

// subject is where one envelope goes: loghub.<space>.syslog, or with
// facility-severity loghub.<space>.syslog.<facility>.<severity> with the
// RFC 5427 names (SPEC §3.8). It is derived, never configured: the rule
// loghub.{space}.{type} is the JLH's.
func (n *NATS) subject(e *Envelope) string {
	s := n.subjectsFor(e.Item.Head.Space)
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		if pri := e.Res.Priority; pri >= 0 && pri < len(s.granular) {
			return s.granular[pri]
		}
	}
	return s.exact
}

// message builds the wire shape of SPEC §3.8 around the envelope's JLH.
//
// Nats-Msg-Id is <uuid>.<step>, NEVER the bare uuid: a processor forwarding to
// the same stream (same uuid, step + 1) would otherwise have its legitimate
// hop discarded as a duplicate by the JetStream window.
func (n *NATS) message(e *Envelope) *nats.Msg {
	uuid := e.UUID()
	space := e.Item.Head.Space
	h := nats.Header{
		jetstream.MsgIDHeader: {uuid + ".0"},
		natsHeaderContentType: {"application/json"},
		natsHeaderType:        {"syslog"},
		natsHeaderUUID:        {uuid},
		natsHeaderSpace:       {space},
	}
	return &nats.Msg{Subject: n.subject(e), Header: h, Data: e.JLH}
}

// Write publishes the chunk (SPEC §10.7).
func (n *NATS) Write(ctx context.Context, b *Batch) error {
	if len(b.Envelopes) == 0 {
		return nil
	}
	if n.closed.Load() {
		return ErrClosed
	}
	nc, js, err := n.connect()
	if err != nil {
		return err
	}
	if n.cfg.Mode == NATSModeCore {
		return n.writeCore(ctx, nc, b)
	}
	return n.writeJetStream(ctx, js, b)
}

// writeJetStream publishes one PublishMsgAsync per envelope, and only once the
// N futures are in the air waits for the N acknowledgements, under the
// sink.op_timeout of the chunk.
//
// Data points into the chunk buffer, which goes back to the pool only after
// every future resolved (a nil return). Every error after the first publish is
// marked as tainting: futures may still be pending, and the buffer must be left
// to the GC (SPEC §10.7, §15.2).
func (n *NATS) writeJetStream(ctx context.Context, js jetstream.JetStream, b *Batch) error {
	futs := make([]jetstream.PubAckFuture, 0, len(b.Envelopes))
	for i := range b.Envelopes {
		f, err := js.PublishMsgAsync(n.message(&b.Envelopes[i]),
			// The server rejects the message if the subject belongs to another
			// stream: a configuration mistake becomes a refusal, not messages
			// in the wrong place.
			jetstream.WithExpectStream(n.cfg.Stream),
			// No retries inside the library: they would re-read msg.Data after
			// Write returned, which is the buffer race SPEC §15.2 forbids. The
			// worker retries, still owning the buffer (SPEC §10.2).
			jetstream.WithRetryAttempts(0))
		if err != nil {
			err = classifyNATS("publish", err)
			if len(futs) > 0 {
				return tainting(err)
			}
			return err
		}
		futs = append(futs, f)
	}

	var dups uint64
	// refused is the first message the stream refused for good (logic: too
	// large for it, 10054) and refusedAt every such index. One such refusal
	// does not stop the wait: the other futures say which messages the stream
	// STORED, and those are not lost (SPEC §10.2, Error.Delivered).
	var refused *Error
	var refusedAt []int
	for i, f := range futs {
		select {
		case ack := <-f.Ok():
			if ack != nil && ack.Duplicate {
				dups++
			}
		case err := <-f.Err():
			c := classifyNATSError(err)
			op := "ack"
			if c != ClassNetwork {
				op += " uuid=" + b.Envelopes[i].UUID()
			}
			if c != ClassLogic {
				// Retried (network) or the end (fatal) as a whole chunk: which
				// futures resolved does not matter, and some are still pending.
				return tainting(classifyNATS(op, err))
			}
			if refused == nil {
				refused = &Error{Class: ClassLogic, Op: op, Err: err}
			}
			refusedAt = append(refusedAt, i)
		case <-ctx.Done():
			return tainting(networkErr("ack", ctx.Err()))
		}
	}
	if dups > 0 {
		// Inside the stream's duplicate_window a redelivery is acknowledged as
		// a duplicate and stored once; outside it, it duplicates
		// (at-least-once, SPEC §10.2).
		n.m.SinkNATSDuplicatesTotal.Add(dups)
	}
	if refused != nil {
		refused.Delivered = make([]int, 0, len(futs)-len(refusedAt))
		for i, k := 0, 0; i < len(futs); i++ {
			if k < len(refusedAt) && refusedAt[k] == i {
				k++
				continue
			}
			refused.Delivered = append(refused.Delivered, i)
		}
		// Every future has resolved, but SPEC §10.7 leaves the buffer of any
		// failed write to the GC, and this path is rare.
		return tainting(refused)
	}
	return nil
}

// writeCore publishes by core NATS and flushes once per chunk (SPEC §10.7). The
// client copies Data into its write buffer inside PublishMsg, so nothing reads
// the chunk buffer after this returns. Success means the server RECEIVED the
// messages, not that anyone stored them.
func (n *NATS) writeCore(ctx context.Context, nc *nats.Conn, b *Batch) error {
	for i := range b.Envelopes {
		if err := nc.PublishMsg(n.message(&b.Envelopes[i])); err != nil {
			return classifyNATS("publish", err)
		}
	}
	if err := n.flush(ctx, nc); err != nil {
		return classifyNATS("flush", err)
	}
	return nil
}

// flush bounds the round trip by the caller's deadline, or by sink.op_timeout
// when the context has none.
func (n *NATS) flush(ctx context.Context, nc *nats.Conn) error {
	if _, ok := ctx.Deadline(); ok {
		return nc.FlushWithContext(ctx)
	}
	return nc.FlushTimeout(n.cfg.OpTimeout)
}

// Health checks the connection state and, in jetstream mode, asks the stream
// for its info; in core mode it flushes (SPEC §10.7). A PING would answer
// happily with JetStream disabled, and the state would never leave UP.
//
// The first successful check also runs the one-time validation.
func (n *NATS) Health(ctx context.Context) error {
	if n.closed.Load() {
		return ErrClosed
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
		if !n.validated.Load() {
			if err := n.validateSize(nc, nil); err != nil {
				return err
			}
			n.validated.Store(true)
			n.log.Info("nats core mode: best effort, a message nobody subscribes to is lost",
				"largest_message", n.maxMessageBytes(), "max_payload", nc.MaxPayload())
		}
		return nil
	}

	stream, err := js.Stream(ctx, n.cfg.Stream)
	if err != nil {
		if !n.validated.Load() && errors.Is(err, nats.ErrNoResponders) {
			// Nobody answers the stream API: a server, or an account, without
			// JetStream -- which SPEC §10.7 lists as fatal, and which as a
			// network failure would hold the boot for sink.boot_timeout and
			// then exit with the wrong reason. AccountInfo tells the two
			// apart. It is asked only here, on a failure of the first check:
			// asked on every boot, it would hang a role whose permissions
			// grant the stream's API but not $JS.API.INFO.
			if _, aerr := js.AccountInfo(ctx); errors.Is(aerr, jetstream.ErrJetStreamNotEnabled) ||
				errors.Is(aerr, jetstream.ErrJetStreamNotEnabledForAccount) {
				return fatalErr("validate", fmt.Errorf(
					"JetStream is not enabled on this server or for this account; sink.nats.mode = "+
						"jetstream needs it (docs/SPEC.md §10.7): %w", aerr))
			}
		}
		return classifyNATS("stream", err)
	}
	if !n.validated.Load() {
		if err := n.validate(nc, stream.CachedInfo()); err != nil {
			return err
		}
		n.validated.Store(true)
	}
	return nil
}

// coverageSubject is what the stream must cover: the exact subject, or every
// facility-severity subject of this instance's space (SPEC §10.7).
func (n *NATS) coverageSubject() string {
	s := "loghub." + n.cfg.Workspace + ".syslog"
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		s += ".*.*"
	}
	return s
}

// maxMessageBytes is what the largest message this server can publish counts
// against max_payload: the worst JLH in Data, plus the header block, whose
// Nats-Expected-Stream carries the stream name in jetstream mode.
func (n *NATS) maxMessageBytes() int64 {
	need := n.cfg.MaxEventBytes + natsHeaderMax
	if n.cfg.Mode == NATSModeJetStream {
		need += int64(natsExpectedStreamHeader + len(n.cfg.Stream))
	}
	return need
}

// validate is the boot contract of SPEC §10.7 in jetstream mode. Everything it
// refuses is ClassFatal: the stream is created by whoever runs NATS, from
// docker/nats/stream.json, and this server never creates or alters one
// (invariant §20.17).
func (n *NATS) validate(nc *nats.Conn, info *jetstream.StreamInfo) error {
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
				"confirmed (docs/SPEC.md §10.7); set no_ack=false", n.cfg.Stream))
	}

	// Local pattern matching, not a server lookup by subject: the question is
	// whether EVERY subject this server can publish on lands in this stream,
	// and with facility-severity that is a wildcard no lookup answers.
	want := n.coverageSubject()
	covered := false
	for _, p := range cfg.Subjects {
		if subjectCovers(p, want) {
			covered = true
			break
		}
	}
	if !covered {
		return fatalErr("validate", fmt.Errorf(
			"stream %q (subjects %v) does not cover %q; apply docker/nats/stream.json for this space",
			n.cfg.Stream, cfg.Subjects, want))
	}

	// A stream that captures the control plane would store every beacon and
	// replay old ones to every new consumer (invariant §20.21). loghub.> is the
	// classic way to get there.
	beacon := n.cfg.CtlNamespace + ".beacon"
	for _, p := range cfg.Subjects {
		if subjectCovers(p, beacon) {
			return fatalErr("validate", fmt.Errorf(
				"stream %q subject %q captures the control plane subject %q; the data stream must "+
					"never cover it (use loghub.<space>.syslog and loghub.<space>.syslog.>, never loghub.>)",
				n.cfg.Stream, p, beacon))
		}
	}

	if err := n.validateSize(nc, &cfg); err != nil {
		return err
	}
	n.log.Info("stream validated",
		"stream", n.cfg.Stream, "subjects", cfg.Subjects, "covers", want,
		"storage", cfg.Storage.String(), "replicas", cfg.Replicas,
		"duplicate_window", cfg.Duplicates.String(),
		"largest_message", n.maxMessageBytes(), "max_payload", nc.MaxPayload())
	return nil
}

// validateSize compares the largest message with the server's max_payload and,
// with a stream, its max_msg_size and the file store's record ceiling (SPEC
// §3.9, invariant §20.20). A message accepted and then refused for its size
// would be logic, and logic drops the WHOLE chunk.
func (n *NATS) validateSize(nc *nats.Conn, cfg *jetstream.StreamConfig) error {
	if n.cfg.MaxEventBytes <= 0 {
		return nil
	}
	need := n.maxMessageBytes()
	if mp := nc.MaxPayload(); mp < need {
		return fatalErr("validate", fmt.Errorf(
			"nats-server max_payload=%d is below the largest message this server can publish "+
				"(%d bytes: the worst JLH of %d and the headers; docs/SPEC.md §3.9); raise "+
				"max_payload or lower syslog.max_message_bytes", mp, need, n.cfg.MaxEventBytes))
	}
	if cfg == nil {
		return nil
	}
	if ms := int64(cfg.MaxMsgSize); ms > 0 && ms < need {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_msg_size=%d is below the largest message this server can publish "+
				"(%d bytes; docs/SPEC.md §3.9); raise it or lower syslog.max_message_bytes",
			n.cfg.Stream, ms, need))
	}
	// A file store has a record ceiling of its own, below max_payload's upper
	// range, and past it the refusal is 10077, fatal: sizing max_payload up to
	// 64 MB would otherwise turn one large message into the end of the process.
	subject := len(n.coverageSubject())
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		subject += natsGranularSuffixMax - len(".*.*")
	}
	if record := need + int64(subject) + 64; cfg.Storage == jetstream.FileStorage &&
		record > jsFileStoreMaxRecord {
		return fatalErr("validate", fmt.Errorf(
			"stream %q uses file storage, which stores records of at most %d bytes, below the "+
				"largest message this server can publish (%d bytes; docs/SPEC.md §3.9); lower "+
				"syslog.max_message_bytes", n.cfg.Stream, jsFileStoreMaxRecord, record))
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

// JetStream API error codes this sink classifies by number (SPEC §10.7). The
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

// classifyNATS maps a client or API error onto the class the state machine acts
// on. The table is SPEC §10.7; keep the two in step.
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

	// Credentials refused, or a connection closed for good: nothing a retry
	// can change.
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrConnectionClosed):
		return ClassFatal

	// The stream is gone or JetStream is off: every chunk would be refused.
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrJetStreamNotEnabled),
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return ClassFatal

	// One message does not fit. Unreachable after the boot check of SPEC §3.9
	// unless the server changed underneath.
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
	// discarded chunk is not (invariant §20.18).
	return ClassNetwork
}
