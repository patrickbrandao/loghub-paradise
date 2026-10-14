package sink

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
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
	// 2 × (sink.workers × sink.batch + ring.max_inflight) (SPEC §10.7).
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

	// own is the subject set of this instance's space, built once: the hot
	// path finds it by one string comparison, with no map lookup and no
	// allocation, and the same comparison is what tells a foreign space apart
	// (writeCore). subjects caches the set of every OTHER space seen. The space comes from the item head, written by
	// the identity of the instance that RECEIVED the message -- never by an
	// emitter -- so those keys are the Workspaces of the emergency dumps this
	// instance reingests, which an operator controls.
	own      *natsSubjects
	subjects sync.Map // string -> *natsSubjects

	// blind holds the foreign spaces core mode has warned about (writeCore):
	// one WARN per space, never one per message (SPEC §14.2).
	blind sync.Map // string -> struct{}

	// asyncErrLog samples the client's asynchronous errors. A publish
	// permission denied produces one per message, and SPEC §14.2 allows no
	// per-message log line on the default path.
	asyncErrLog *obs.Sampler

	validated atomic.Bool
	closed    atomic.Bool

	// perSubjectWarned is the max_msgs_per_subject last warned about, so the
	// validation that runs on every health check says it once per value, not
	// once per check.
	perSubjectWarned atomic.Int64

	// verdict is a permanent refusal the client reported asynchronously -- a
	// permissions violation on the data path, a reconnection whose TLS can
	// never succeed (isTLSRefusal) -- which no synchronous call
	// returns: a denied publish only times out (jetstream) or vanishes (core),
	// and a denied stream info or ack subscription only times out. Health and
	// every Write turn it into FATAL (SPEC §10.7).
	verdict atomic.Pointer[error]
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
	n.own = n.newSubjects(cfg.Workspace)
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
		nats.ReconnectErrHandler(n.onReconnectErr),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			m.SinkNATSReconnectsTotal.Inc()
			log.Warn("nats reconnected", "url", connectedURL(nc))
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
// with the count of what was suppressed in between, and keeps a permanent
// refusal of the data path as the verdict.
func (n *NATS) asyncError(err error) {
	if deniesTheDataPath(err) {
		n.setVerdict(err)
	}
	if ok, suppressed := n.asyncErrLog.Allow(); ok {
		n.log.Error("nats async error", "error", err, "suppressed", suppressed)
	}
}

func (n *NATS) setVerdict(err error) { n.verdict.CompareAndSwap(nil, &err) }

// verdictErr is the verdict as the fatal error every operation returns once it
// is set: Health, and every Write in both modes (SPEC §10.7). A Write that
// ignored it would publish -- and, in core mode, confirm -- chunks the server
// has already refused, until the next health check.
func (n *NATS) verdictErr(op string) error {
	if v := n.verdict.Load(); v != nil {
		return fatalErr(op, fmt.Errorf("the NATS server refused this server permanently: %w", *v))
	}
	return nil
}

// noteLastError keeps the connection's last error as the verdict when it is a
// denial of the data path. The ErrorHandler runs on the client's callback
// goroutine, after the reader has already stored the -ERR in LastError; a
// caller that has just seen its own round trip complete reads it here instead
// of racing the handler.
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
	// independent review, 2026-10-13; SPEC §10.7).
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

// onReconnectErr is the ReconnectErrCB (SPEC §10.7). nats.go calls it only for
// a reconnection whose TCP dial failed; a TLS refusal comes later, in the
// handshake, and is caught by noteLastError from Health. Kept for a client
// version that reports it here. Only a verdict is logged: every failed dial of
// a long outage as an ERROR would repeat what the disconnection WARN, /readyz
// and relplog_sink_up already say (the rule go-relplog-nats-server took from
// here, 2026-10-14). The verdict only with no other server to go to
// (soleServer). Nothing outside the if.
func (n *NATS) onReconnectErr(nc *nats.Conn, err error) {
	if isTLSRefusal(err) && soleServer(nc) {
		n.setVerdict(err)
		n.asyncError(err)
	}
}

// soleServer reports that the client has no server to go to but the one it
// is using: its pool -- the configured URLs plus every server the cluster
// announced since (nats.go adds them on its own) -- has one entry. Only then
// is a TLS refusal a verdict on this client (SPEC §10.7).
func soleServer(nc *nats.Conn) bool { return nc == nil || len(nc.Servers()) <= 1 }

// escalate turns a network failure into the verdict once one is set. A publish
// the server denied never gets an ack, so the future ends in a timeout --
// network, retried forever with the sink UP -- and a denied ack subscription
// or stream info ends the same way. With the verdict in hand that timeout is
// known to be permanent. The taint is kept: futures may still be pending.
func (n *NATS) escalate(nc *nats.Conn, op string, err error) error {
	if ClassOf(err) != ClassNetwork {
		return err
	}
	if nc != nil {
		n.noteLastError(nc)
	}
	v := n.verdictErr(op)
	if v == nil {
		return err
	}
	if taints(err) {
		return tainting(v)
	}
	return v
}

// deniesTheDataPath reports a permissions violation on what the data path
// needs (SPEC §10.7): a publish to a data subject -- loghub.<space>.syslog or
// its facility-severity form, of any space, since a reingested dump publishes
// on its own -- anything on the JetStream API ($JS.API.>, which carries the
// stream info of Health), and a subscription on _INBOX, where the publish acks
// and the API's answers arrive: denied there, every ack is lost and every
// chunk times out. Nothing else on this connection subscribes (the beacon
// only publishes), so an _INBOX denial is always the data path's.
//
// A denial on the beacon's subject is NOT one: the control plane failing must
// not take the data down with it (SPEC §11).
func deniesTheDataPath(err error) bool {
	if err == nil || !errors.Is(err, nats.ErrPermissionViolation) {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "$JS.API") {
		return true
	}
	// "Permissions Violation for Publish to "<subject>"", or "for Subscription
	// to "<subject>"" (with a queue, more text follows the quotes). The
	// operation is matched without case: the client keeps the server's words.
	i := strings.Index(msg, ` to "`)
	if i < 0 {
		return false
	}
	subject, _, ok := strings.Cut(msg[i+len(` to "`):], `"`)
	if !ok {
		return false
	}
	head := strings.ToLower(msg[:i])
	switch {
	case strings.HasSuffix(head, "for subscription"):
		return strings.HasPrefix(subject, "_INBOX.")
	case strings.HasSuffix(head, "for publish"):
		// The beacon's <ctl_namespace>.beacon never has the shape: its last
		// token is neither "syslog" nor a severity name, whatever the
		// namespace. A prefix match on "loghub." would have caught it with
		// the default namespace, loghub._ctl.
		return isDataSubject(subject)
	}
	return false
}

// isDataSubject reports whether subject has the shape of a data subject of
// SPEC §3.8: loghub.<space>.syslog, or loghub.<space>.syslog.<facility>.<severity>
// with the RFC 5427 names. Both forms are recognised whatever
// sink.nats.subjects says: a denial is about the server's permissions, not
// about which form this life publishes.
func isDataSubject(subject string) bool {
	if !strings.HasPrefix(subject, "loghub.") {
		return false
	}
	if strings.HasSuffix(subject, ".syslog") {
		return len(subject) > len("loghub..syslog")
	}
	rest, sev, ok := cutLast(subject)
	if !ok || !isName(convert.SeverityNames[:], sev) {
		return false
	}
	rest, fac, ok := cutLast(rest)
	if !ok || !isName(convert.FacilityNames[:], fac) {
		return false
	}
	return strings.HasSuffix(rest, ".syslog") && len(rest) > len("loghub..syslog")
}

func cutLast(s string) (rest, last string, ok bool) {
	i := strings.LastIndexByte(s, '.')
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func isName(names []string, s string) bool {
	for _, n := range names {
		if n == s {
			return true
		}
	}
	return false
}

// connectedURL is the server this connection reached, scheme and host only.
// nats.go's ConnectedUrlRedacted hides a password and nothing else: a URL whose
// user part is a token (nats://<token>@host, which LOGHUB_EB_URL accepts,
// SPEC §10.9) came out whole, in the INFO line of every boot and the WARN of
// every reconnect. A URL that does not parse is never echoed: the bare address
// stands in for it.
func connectedURL(nc *nats.Conn) string {
	u, err := url.Parse(nc.ConnectedUrl())
	if err != nil || u.Host == "" {
		return nc.ConnectedAddr()
	}
	return u.Scheme + "://" + u.Host
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
		if isTLSRefusal(err) && len(n.cfg.URLs) > 1 {
			// nats.Connect tries every URL and returns the error of the last
			// one: the TLS refusal of one server does not say the others
			// cannot serve -- one may be down for a moment. Network, waited
			// out to sink.boot_timeout, with the refusal named (SPEC §10.7).
			return nil, nil, networkErr("connect", fmt.Errorf(
				"a server of sink.nats.urls refused TLS, and no other answered: %w", err))
		}
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

func (n *NATS) newSubjects(space string) *natsSubjects {
	s := &natsSubjects{exact: "loghub." + space + ".syslog"}
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		for pri := range s.granular {
			s.granular[pri] = s.exact + "." + convert.FacilityNames[pri/8] + "." + convert.SeverityNames[pri%8]
		}
	}
	return s
}

// subjectsFor is the subject set of space. The own space is the hot path: a
// string comparison, no lookup. Any other comes from a dump and is cached, so
// no envelope ever builds its subject string.
func (n *NATS) subjectsFor(space string) *natsSubjects {
	if space == n.cfg.Workspace {
		return n.own
	}
	if v, ok := n.subjects.Load(space); ok {
		return v.(*natsSubjects)
	}
	v, _ := n.subjects.LoadOrStore(space, n.newSubjects(space))
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
	if err := n.verdictErr("publish"); err != nil {
		return err
	}
	nc, js, err := n.connect()
	if err != nil {
		return err
	}
	if n.cfg.Mode == NATSModeCore {
		return n.writeCore(ctx, nc, b)
	}
	return n.writeJetStream(ctx, nc, js, b)
}

// writeJetStream publishes one PublishMsgAsync per envelope, and only once the
// N futures are in the air waits for the N acknowledgements, under the
// sink.op_timeout of the chunk.
//
// Data points into the chunk buffer, which goes back to the pool only after
// every future resolved (a nil return). Every error after the first publish is
// marked as tainting: futures may still be pending, and the buffer must be left
// to the GC (SPEC §10.7, §15.2).
func (n *NATS) writeJetStream(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, b *Batch) error {
	// refused is the first message refused for good (logic: too large, 10054
	// from the stream or ErrMaxPayload from the client before it leaves) and
	// rejected marks every such index. One such refusal does not stop the
	// chunk: the other messages are still published and their futures say
	// which ones the stream STORED, and those are not lost (SPEC §10.2,
	// Error.Delivered). Stopping at a publish-time refusal -- a reconnection
	// to a node with a smaller max_payload, a dump line from a life with a
	// larger syslog.max_message_bytes -- left every later envelope
	// unpublished yet confirmed, and counted the earlier ones, stored, lost.
	var refused *Error
	var rejected []bool
	reject := func(i int, op string, err error) {
		if refused == nil {
			refused = &Error{Class: ClassLogic, Op: op + " uuid=" + b.Envelopes[i].UUID(), Err: err}
			rejected = make([]bool, len(b.Envelopes))
		}
		rejected[i] = true
	}
	futs := make([]jetstream.PubAckFuture, len(b.Envelopes))
	published := 0
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
			if classifyNATSError(err) == ClassLogic {
				reject(i, "publish", err)
				continue
			}
			err = classifyNATS("publish", err)
			if published > 0 {
				err = tainting(err)
			}
			return n.escalate(nc, "publish", err)
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
			op := "ack"
			if c != ClassNetwork {
				op += " uuid=" + b.Envelopes[i].UUID()
			}
			if c != ClassLogic {
				// Retried (network) or the end (fatal) as a whole chunk: which
				// futures resolved does not matter, and some are still pending.
				return n.escalate(nc, "ack", tainting(classifyNATS(op, err)))
			}
			reject(i, "ack", err)
		case <-ctx.Done():
			return n.escalate(nc, "ack", tainting(networkErr("ack", ctx.Err())))
		}
	}
	if dups > 0 {
		// Inside the stream's duplicate_window a redelivery is acknowledged as
		// a duplicate and stored once; outside it, it duplicates
		// (at-least-once, SPEC §10.2).
		n.m.SinkNATSDuplicatesTotal.Add(dups)
	}
	if refused != nil {
		refused.Delivered = delivered(rejected)
		// Every future has resolved, but SPEC §10.7 leaves the buffer of any
		// failed write to the GC, and this path is rare.
		return tainting(refused)
	}
	return nil
}

// delivered lists the indexes not rejected, for Error.Delivered.
func delivered(rejected []bool) []int {
	out := make([]int, 0, len(rejected))
	for i, r := range rejected {
		if !r {
			out = append(out, i)
		}
	}
	return out
}

// writeCore publishes by core NATS and flushes once per chunk (SPEC §10.7). The
// client copies Data into its write buffer inside PublishMsg, so nothing reads
// the chunk buffer after this returns. Success means the server RECEIVED the
// messages, not that anyone stored them.
//
// A message refused before it leaves (ErrMaxPayload: larger than this server's
// max_payload) is refused alone, as in jetstream mode: the rest of the chunk is
// still published, and only the refused ones are lost (SPEC §10.2).
func (n *NATS) writeCore(ctx context.Context, nc *nats.Conn, b *Batch) error {
	var refused *Error
	var rejected []bool
	before := nc.LastError()
	reconnects := nc.Stats().Reconnects
	for i := range b.Envelopes {
		e := &b.Envelopes[i]
		if space := e.Item.Head.Space; space != n.cfg.Workspace {
			n.warnForeign(space)
		}
		if err := nc.PublishMsg(n.message(e)); err != nil {
			if classifyNATSError(err) == ClassLogic {
				if refused == nil {
					refused = &Error{Class: ClassLogic, Op: "publish uuid=" + e.UUID(), Err: err}
					rejected = make([]bool, len(b.Envelopes))
				}
				rejected[i] = true
				continue
			}
			return n.escalate(nc, "publish", classifyNATS("publish", err))
		}
	}
	if beforeCoreFlush != nil {
		beforeCoreFlush()
	}
	if err := n.flush(ctx, nc); err != nil {
		return n.escalate(nc, "flush", n.flushErr(nc, "flush", err))
	}
	if nc.Stats().Reconnects != reconnects {
		// The connection was re-established during the chunk. A flush that
		// STARTED while the client was reconnecting queues its PING for the
		// new server, whose PONG resolves it -- but the PUBs went down the
		// old socket. A success would claim the server received them.
		// Uncertain, so not confirmed: the retry publishes them again
		// (go-relplog-nats-server, independent review, 2026-10-13; SPEC §10.7).
		return &Error{Class: ClassNetwork, Op: "flush",
			Err: errors.New("the connection was re-established during the chunk; its messages may not have reached the server")}
	}
	// A denied publish never fails PublishMsg: the server answers with an
	// -ERR, and it comes before the PONG of the flush, so LastError holds it
	// by now, without a race against the ErrorHandler. Without this, a chunk
	// the server refused was confirmed, counted as written and released --
	// lost -- and so was every chunk until the next health check turned the
	// verdict into FATAL (SPEC §10.7, invariant §20.18).
	n.noteLastError(nc)
	if err := n.verdictErr("publish"); err != nil {
		return err
	}
	if last := nc.LastError(); maskedDenial(before, last) {
		// nats.go keeps only the LAST -ERR: a denial of the beacon subject
		// that landed between this chunk's PUBs and the flush's PING hides
		// a denial of the data path that came before it, and the chunk would
		// be confirmed although the server refused it. Uncertain, so not
		// confirmed: the retry finds the verdict the ErrorHandler set by then,
		// or succeeds (independent review, 2026-10-11).
		return &Error{Class: ClassNetwork, Op: "publish",
			Err: fmt.Errorf("a permission denial arrived during the chunk and may hide one of the data path: %w", last)}
	}
	if refused != nil {
		refused.Delivered = delivered(rejected)
		return refused
	}
	return nil
}

// warnForeign says, once per space, that core mode is publishing an envelope of
// another space -- the reingestion of an emergency dump from a life with
// another Workspace. With no stream behind it there is nothing to ask whether
// anyone listens on that subject: unless something subscribes there it is
// lost, and the dump is removed as delivered (SPEC §12.2 covers only
// jetstream). Said once, the loss is at least not silent. The caller compares
// the space with the Workspace first, so the own space -- the hot path --
// never reaches the map.
func (n *NATS) warnForeign(space string) {
	if _, seen := n.blind.LoadOrStore(space, struct{}{}); seen {
		return
	}
	subject := n.subjectsFor(space).exact
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		subject += ".*.*"
	}
	n.log.Warn("nats core mode: publishing envelopes of another space (an emergency dump from "+
		"another Workspace); with no stream behind them they are lost unless something "+
		"subscribes to their subject (docs/SPEC.md §10.7)",
		"subject", subject, "space", space, "workspace", n.cfg.Workspace)
}

// beforeCoreFlush runs between the publications of a core chunk and its
// flush. Tests only: nil in the binary.
var beforeCoreFlush func()

// flushErr classifies the error of a flush. nats.go fails a pending flush with
// ErrConnectionClosed when the connection DROPS -- the channel of its PING is
// closed -- although the client is only RECONNECTING and will be back. The
// table classes ErrConnectionClosed as fatal (a client that closed for good),
// so a NATS blip in the middle of the flush of a core chunk, or of the core
// health check, was FATAL, a dump and exit 3 instead of a retry
// (go-syslog-nats-server TESTING §8 item 1). It is network while the
// connection is not closed; closed for good, it stays fatal (SPEC §10.7).
func (n *NATS) flushErr(nc *nats.Conn, op string, err error) error {
	if errors.Is(err, nats.ErrConnectionClosed) && nc != nil && !nc.IsClosed() {
		return networkErr(op, fmt.Errorf("the connection dropped during the flush and is reconnecting: %w", err))
	}
	return classifyNATS(op, err)
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
// Every successful check also runs the validation of SPEC §10.7 -- the stream
// contract in jetstream mode, max_payload in core mode -- against what it has
// just fetched. Only its log line is once.
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

	if n.cfg.Mode == NATSModeCore {
		if err := n.flush(ctx, nc); err != nil {
			return n.escalate(nc, "health", n.flushErr(nc, "health", err))
		}
		// The flush's PONG follows any -ERR the server sent before it.
		n.noteLastError(nc)
		if err := n.verdictErr("health"); err != nil {
			return err
		}
		// On every check, not once: a reconnection may land on a server with
		// a smaller max_payload, and every larger message would then be
		// refused by the client, logic, one chunk after another (SPEC §10.7).
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
		// A denied stream info, or a denied _INBOX subscription its answer
		// needed, only times out; the verdict says it is permanent.
		return n.escalate(nc, "health", n.streamErr(err))
	}
	// The whole contract on every check, against the info this call has
	// already fetched (no extra round trip). Checked once, a stream edited
	// underneath -- its subjects narrowed, no_ack turned on, a smaller
	// max_msg_size, loghub.> -- answered every publish with a timeout or a
	// refusal, network or logic, forever, while this check kept the sink UP
	// (SPEC §10.7). Only this instance's own subject is required: a dump of
	// another space, reingested at boot, never makes its subject part of the
	// contract, so narrowing the stream after that dump drained -- what the
	// operator may do -- is not a FATAL. validated only gates the log line and
	// the no-responders verdict of streamErr.
	info := stream.CachedInfo()
	if err := n.validate(nc.MaxPayload(), info); err != nil {
		return err
	}
	if !n.validated.Swap(true) {
		cfg := info.Config
		n.log.Info("stream validated",
			"stream", n.cfg.Stream, "subjects", cfg.Subjects, "covers", n.coverageSubject(),
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
		// whose hub is down. The client cannot tell the two apart: jetstream's
		// AccountInfo, asked here until 2026-10-11, maps no responders to "not
		// enabled" itself, and its request to $JS.API.INFO, denied to a role
		// of least privilege, now becomes a permanent verdict on the data path
		// (deniesTheDataPath). Either way nothing can be confirmed now, and as
		// network the boot waited out sink.boot_timeout to exit with the wrong
		// reason (SPEC §10.7). After a validated check the same answer is a
		// blip: network.
		return fatalErr("validate", fmt.Errorf(
			"nobody answers the JetStream API: JetStream is disabled on this server, or no "+
				"JetStream server is reachable from it; sink.nats.mode = jetstream needs it "+
				"(docs/SPEC.md §10.7): %w", err))
	}
	return classifyNATS("stream", err)
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

// validate is the contract of SPEC §10.7 in jetstream mode, checked by every
// Health call against the stream's info and the connected server's
// max_payload. It takes max_payload as a value, not the connection, so the
// refusals and the accepted path are testable without a server. Everything it
// refuses is ClassFatal: the stream is created by whoever runs NATS, from
// docker/nats/stream.json, and this server never creates or alters one
// (invariant §20.17).
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
		// chunk is confirmed, released from the ring and lost, with no error
		// and no counter (invariant §20.18). The PubAck is what gives
		// ring=none its meaning, so a stream that may discard on acceptance
		// cannot be this sink's (decided 2026-10-11, SPEC §10.7).
		return fatalErr("validate", fmt.Errorf(
			"stream %q has retention=interest: it acknowledges and keeps nothing while no consumer is "+
				"interested, so a confirmed chunk could be lost (docs/SPEC.md §10.7); use "+
				"retention=limits (or workqueue)", n.cfg.Stream))
	}
	// Every event of a space shares one subject (with facility-severity, one
	// per priority): a per-subject cap keeps only the last few of a whole
	// space and silently discards the rest as they arrive, each one
	// acknowledged. Legitimate for a "latest only" stream, which is why it is
	// a warning and not a refusal -- said once per value, not once per check.
	if v := max(cfg.MaxMsgsPerSubject, 0); n.perSubjectWarned.Swap(v) != v && v > 0 {
		n.log.Warn("stream keeps at most max_msgs_per_subject messages per subject; every event of "+
			"a space shares one subject, so older events are discarded as new ones are acknowledged "+
			"(docs/SPEC.md §10.7)", "stream", n.cfg.Stream, "max_msgs_per_subject", v)
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

	return n.validateSize(maxPayload, &cfg)
}

// validateSize compares the largest message with the server's max_payload and,
// with a stream, its max_msg_size and the file store's record ceiling (SPEC
// §3.9, invariant §20.20). A message accepted and then refused for its size
// would be logic, and logic drops the WHOLE chunk.
func (n *NATS) validateSize(maxPayload int64, cfg *jetstream.StreamConfig) error {
	if n.cfg.MaxEventBytes <= 0 {
		return nil
	}
	need := n.maxMessageBytes()
	if mp := maxPayload; mp < need {
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
	subject := len(n.coverageSubject())
	if n.cfg.Subjects == NATSSubjectsFacilitySeverity {
		subject += natsGranularSuffixMax - len(".*.*")
	}
	record := need + int64(subject) + 64
	// A stream that refuses new messages when full, smaller than one record:
	// the first large message is refused for good, and every boot after the
	// dump validates and then fails its reingestion the same way. With discard
	// old the stream makes room instead. Ported from SN/N (independent review,
	// 2026-10-11).
	if cfg.Discard == jetstream.DiscardNew && cfg.MaxBytes > 0 && cfg.MaxBytes < record {
		return fatalErr("validate", fmt.Errorf(
			"stream %q max_bytes=%d with discard=new is below the largest message this server can "+
				"publish (%d bytes as a record; docs/SPEC.md §3.9); raise max_bytes or lower "+
				"syslog.max_message_bytes", n.cfg.Stream, cfg.MaxBytes, record))
	}
	// A file store has a record ceiling of its own, below max_payload's upper
	// range, and past it the refusal is 10077, fatal: sizing max_payload up to
	// 64 MB would otherwise turn one large message into the end of the process.
	if cfg.Storage == jetstream.FileStorage && record > jsFileStoreMaxRecord {
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
	jsErrStreamStoreFailed     = 10077 // see storeFailedIsTransient
	jsErrHeaderExceedsMaximum  = 10097
	jsErrStreamSealed          = 10109
	jsErrDuplicateInProcess    = 10158
	jsErrStreamTooManyRequests = 10167
)

// isAuthRefusal recognises a refusal of the credentials at CONNECT, which
// nats.go returns with the server's own words: its errors.Is compares the whole
// text, so "User Authentication Expired" never matched ErrAuthExpired
// ("authentication expired"), and "Revoked" matched nothing, so both were
// classed network and the boot waited out sink.boot_timeout to exit with the
// wrong reason (SPEC §10.7). Only phrases about the credentials themselves:
// "permissions violation" is about one subject, and it is the verdict's to
// judge (deniesTheDataPath), never a refusal of the whole connection.
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
// fuses (go-relplog-nats-server, independent review, 2026-10-13, adapted: that
// server has no TLS and refuses every TLS server; SPEC §10.7).
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

	// Credentials refused, a connection closed for good, or a TLS that can
	// never succeed: nothing a retry can change.
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired),
		errors.Is(err, nats.ErrConnectionClosed), isAuthRefusal(err), isTLSRefusal(err):
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
			if api.ErrorCode == jsErrStreamStoreFailed && storeFailedIsTransient(api.Description) {
				return ClassNetwork
			}
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
	// discarded chunk is not (invariant §20.18).
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
// the stored part of the chunk duplicated at reingestion (independent review,
// 2026-10-11). A record too large for the store stays fatal: retrying cannot
// make it fit, and validateSize refuses the configurations that produce it.
//
// The text is nats-server's own (server/store.go, ErrStoreClosed, 2.14.6:
// "store is closed"). This matched "store closed" alone, a text the server
// never sends, so a stream closing was still FATAL (go-syslog-server review,
// 2026-10-13); "store closed" stays for an older wording.
func storeFailedIsTransient(desc string) bool {
	d := strings.ToLower(desc)
	return strings.Contains(d, "maximum messages exceeded") ||
		strings.Contains(d, "maximum bytes exceeded") ||
		strings.Contains(d, "maximum messages per subject exceeded") ||
		strings.Contains(d, "store is closed") ||
		strings.Contains(d, "store closed")
}

// maskedDenial reports a permission denial that arrived during a core chunk
// (last is a new error, not the one before it) and is not one of the data
// path: the one case where LastError can hide the denial that matters.
func maskedDenial(before, last error) bool {
	if last == nil || !errors.Is(last, nats.ErrPermissionViolation) || deniesTheDataPath(last) {
		return false
	}
	return !sameError(before, last)
}

// sameError compares two errors by identity -- every -ERR the client stores
// is a new value -- without panicking on an error type that is not
// comparable.
func sameError(a, b error) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a == b
}
