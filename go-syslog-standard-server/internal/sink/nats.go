package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
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
	Subject     string
	ClientName  string

	ConnectTimeout time.Duration
	// OpTimeout bounds how long the client keeps an unanswered publish pending,
	// so a message -- and the buffer it points at -- is never held past the
	// worker's own deadline (SPEC §10.8.2).
	OpTimeout time.Duration
	// MaxRawBytes and MaxEnvelopeBytes are the longest raw line an enabled
	// transport can accept and the largest envelope that line can produce
	// (SPEC §3.9), checked against max_payload and the stream's max_msg_size at
	// boot (§10.8.5). Zero skips the check.
	MaxRawBytes      int64
	MaxEnvelopeBytes int64
	// MaxPending is the ceiling of async publishes in flight per process.
	MaxPending int
}

// Header names of SPEC §3.8. Nats-Msg-Id is the library's.
const (
	natsHeaderVersion      = "Sls-Version"
	natsHeaderEnvelopeSize = "Sls-Envelope-Size"

	// natsHeaderOverhead bounds the header block this sink writes, the stream
	// name aside: the NATS/1.0 line, Nats-Msg-Id with the id, Sls-Version,
	// Sls-Envelope-Size with its digits, Nats-Expected-Stream and the CRLF
	// framing come to about 150 bytes. The block is small and fixed by
	// construction, which is the whole reason the envelope is not in it
	// (SPEC §3.8).
	natsHeaderOverhead = 256

	// jsFileStoreMaxRecord is the largest record a file-backed stream stores,
	// whatever max_payload says: nats-server refuses a record above its
	// rlBadThresh of 32 MiB with a store failure, 10077, which is fatal here
	// (SPEC §10.8.5, §10.8.6).
	jsFileStoreMaxRecord = 32 << 20

	// natsMaxPooledBuf is the largest batch buffer kept for reuse. A batch of
	// large lines allocates its own, so one burst cannot make every pooled
	// buffer permanently large.
	natsMaxPooledBuf = 4 << 20
)

// NATS is the JetStream destination (SPEC §10.8). Message shape in §3.8.
//
// It is the destination that most resembles Redis -- remote, schemaless, a
// consumer on the other side -- with one difference that decides everything:
// JetStream acknowledges every publish, and that acknowledgement is what gives
// the confirmation of ring=none its meaning.
//
// It is an adapted copy of the sibling service's sink, which ran against a real
// server first, with one deliberate difference in the message: the envelope
// travels in Data, ahead of the raw line, and not in a header. JetStream stores
// a header block's length in sixteen bits and refuses anything above 64 KiB
// with 10097, a 4xx that would be fatal; a syslog envelope passes that with one
// long hostname or a few kilobytes of structured data, and in a header it would
// let a single emitter end the process (SPEC §3.8).
type NATS struct {
	cfg NATSConfig
	log *slog.Logger
	m   *obs.Metrics

	// bufs pools the per-batch Data buffers (see Write).
	bufs sync.Pool

	opts   []nats.Option
	jsOpts []jetstream.JetStreamOpt

	// mu guards the lazy connect; nc and js are set once and never replaced.
	mu sync.Mutex
	nc *nats.Conn
	js jetstream.JetStream

	validated atomic.Bool
	closed    atomic.Bool
}

// NewNATS prepares the client without connecting: the connection is opened by
// the first health check, and the boot's WaitReady owns the retry loop
// (SPEC §10.8.1). The errors here are configuration errors -- a CA file that
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

	n := &NATS{cfg: cfg, log: log, m: m}
	opts := []nats.Option{
		nats.Name(cfg.ClientName),
		nats.Timeout(cfg.ConnectTimeout),
		// The health machine has the fuses that decide when to give up; a
		// client that closed itself after N attempts would turn a long outage
		// into an unrecoverable state with no emergency dump (SPEC §10.8.1).
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		// No reconnect buffer: the ring is the only buffer of this design. A
		// publish during a disconnect fails at once with the network class
		// instead of queueing inside the client, invisible to the ring's
		// accounting, to the transports' backpressure and to the dump
		// (invariant §19.18).
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
			log.Warn("nats reconnected", "url", nc.ConnectedUrl())
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
			log.Error("nats async error", "error", err)
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

// connect opens the connection on first use.
func (n *NATS) connect() (*nats.Conn, jetstream.JetStream, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.nc != nil {
		return n.nc, n.js, nil
	}
	nc, err := nats.Connect(strings.Join(n.cfg.URLs, ","), n.opts...)
	if err != nil {
		return nil, nil, classifyNATS("connect", err)
	}
	js, err := jetstream.New(nc, n.jsOpts...)
	if err != nil {
		nc.Close()
		return nil, nil, classifyNATS("jetstream", err)
	}
	n.nc, n.js = nc, js
	n.log.Info("nats connected", "url", nc.ConnectedUrl(), "max_payload", nc.MaxPayload())
	return nc, js, nil
}

// message builds the wire shape of SPEC §3.8 around data, which must hold the
// envelope of p followed by its raw line.
func (n *NATS) message(p *Prepared, data []byte) *nats.Msg {
	h := nats.Header{}
	h.Set(jetstream.MsgIDHeader, p.Item.Head.ID)
	h.Set(natsHeaderVersion, envelope.Version)
	h.Set(natsHeaderEnvelopeSize, strconv.Itoa(len(p.Env)))
	return &nats.Msg{Subject: n.cfg.Subject, Header: h, Data: data}
}

// getBuf returns an empty buffer with room for size bytes.
func (n *NATS) getBuf(size int) []byte {
	if b, ok := n.bufs.Get().(*[]byte); ok && cap(*b) >= size {
		return (*b)[:0]
	}
	return make([]byte, 0, size)
}

// putBuf keeps a buffer for the next batch. Call it only once every message
// sliced from it has been acknowledged.
func (n *NATS) putBuf(b []byte) {
	if cap(b) > natsMaxPooledBuf {
		return
	}
	b = b[:0]
	n.bufs.Put(&b)
}

// Write publishes the batch asynchronously and then waits for every
// acknowledgement (SPEC §10.8.2).
func (n *NATS) Write(ctx context.Context, batch []Prepared) error {
	if len(batch) == 0 {
		return nil
	}
	if n.closed.Load() {
		return ErrClosed
	}
	_, js, err := n.connect()
	if err != nil {
		return err
	}

	// Every message's Data is the envelope followed by the raw line, copied into
	// one buffer for the whole batch and sliced per message. The copy is not
	// optional: the library keeps a message until its acknowledgement, and the
	// envelope belongs to a Prep that is released when this call returns, so
	// Data has to own its bytes until the last ack. One buffer per batch costs
	// one allocation where a copy per message would cost N, and it is sized up
	// front, so no append can move bytes an earlier message already points at.
	size := 0
	for i := range batch {
		size += len(batch[i].Env) + len(batch[i].Item.Raw)
	}
	buf := n.getBuf(size)

	futs := make([]jetstream.PubAckFuture, 0, len(batch))
	for i := range batch {
		p := &batch[i]
		start := len(buf)
		buf = append(buf, p.Env...)
		buf = append(buf, p.Item.Raw...)
		f, err := js.PublishMsgAsync(n.message(p, buf[start:len(buf):len(buf)]),
			// The server rejects the message if the subject belongs to another
			// stream: a configuration mistake becomes a refusal, not messages
			// in the wrong place.
			jetstream.WithExpectStream(n.cfg.Stream),
			// No retries inside the library: they would re-read msg.Data after
			// this call returned, which is the buffer race SPEC §14.2 forbids.
			// The worker retries, with the item still owning the buffer
			// (SPEC §10.2).
			jetstream.WithRetryAttempts(0))
		if err != nil {
			// The buffer is not pooled on any error path: the messages
			// published before this one are still pending in the library.
			return classifyNATS("publish", err)
		}
		futs = append(futs, f)
	}

	var dups uint64
	for _, f := range futs {
		select {
		case ack := <-f.Ok():
			if ack.Duplicate {
				dups++
			}
		case err := <-f.Err():
			return classifyNATS("ack", err)
		case <-ctx.Done():
			return networkErr("ack", ctx.Err())
		}
	}
	if dups > 0 {
		n.m.SinkNATSDuplicatesTotal.Add(dups)
	}
	// Every future resolved: with retries off and no async error handler,
	// nothing in the library reads a published message's Data again.
	n.putBuf(buf)
	return nil
}

// Health checks the connection state and asks the stream for its info
// (SPEC §10.8.7). A PING would answer happily with JetStream disabled, and the
// state would never leave UP.
//
// The first successful check also runs the one-time validation of SPEC §10.8.5.
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

	stream, err := js.Stream(ctx, n.cfg.Stream)
	if err != nil {
		return classifyNATS("stream", err)
	}
	if !n.validated.Load() {
		if err := n.validate(ctx, nc, js, stream.CachedInfo()); err != nil {
			return err
		}
		n.validated.Store(true)
	}
	return nil
}

// maxMessageBytes is what the largest message this server can publish counts
// against max_payload: Data holding the raw line and the envelope at the bound
// of SPEC §3.9, plus the header block, whose Nats-Expected-Stream carries the
// stream name.
func (n *NATS) maxMessageBytes() int64 {
	return n.cfg.MaxRawBytes + n.cfg.MaxEnvelopeBytes + natsHeaderOverhead + int64(len(n.cfg.Stream))
}

// validate is the boot contract of SPEC §10.8.5. Everything it refuses is
// ClassFatal.
func (n *NATS) validate(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, info *jetstream.StreamInfo) error {
	owner, err := js.StreamNameBySubject(ctx, n.cfg.Subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fatalErr("validate", fmt.Errorf(
				"no stream listens on subject %q; apply docker/nats/stream.json", n.cfg.Subject))
		}
		return classifyNATS("validate", err)
	}
	if owner != n.cfg.Stream {
		return fatalErr("validate", fmt.Errorf(
			"subject %q belongs to stream %q, not %q", n.cfg.Subject, owner, n.cfg.Stream))
	}
	if info == nil {
		return networkErr("validate", errors.New("stream info not available"))
	}
	if info.Config.Sealed {
		return fatalErr("validate", fmt.Errorf("stream %q is sealed and accepts no publishes", n.cfg.Stream))
	}
	if info.Config.Retention == jetstream.InterestPolicy {
		// With interest retention a message nobody is interested in is
		// acknowledged with an ordinary PubAck and not kept: with no consumer
		// bound -- before the first one, or while every one is deleted -- each
		// batch is confirmed, released from the ring and lost, with no error
		// and no counter. The PubAck is what gives ring=none its meaning, so a
		// stream that may discard on acceptance cannot be this sink's (SPEC
		// §10.8.5; ported from go-syslog-server, §21.1 item 6).
		return fatalErr("validate", fmt.Errorf(
			"stream %q has retention=interest: it acknowledges and keeps nothing while no consumer is "+
				"interested, so a confirmed batch could be lost (docs/SPEC.md §10.8.5); use "+
				"retention=limits (or workqueue)", n.cfg.Stream))
	}
	if n.cfg.MaxRawBytes > 0 {
		need := n.maxMessageBytes()
		if mp := nc.MaxPayload(); mp < need {
			return fatalErr("validate", fmt.Errorf(
				"nats-server max_payload=%d is below the largest message this server can accept "+
					"(%d bytes: a raw line of up to %d and its envelope; docs/SPEC.md §3.9); raise "+
					"max_payload or lower tcp.max_frame_bytes and relp.max_frame_bytes",
				mp, need, n.cfg.MaxRawBytes))
		}
		if ms := int64(info.Config.MaxMsgSize); ms > 0 && ms < need {
			return fatalErr("validate", fmt.Errorf(
				"stream %q max_msg_size=%d is below the largest message this server can accept "+
					"(%d bytes; docs/SPEC.md §3.9); raise it or lower tcp.max_frame_bytes and "+
					"relp.max_frame_bytes", n.cfg.Stream, ms, need))
		}
		// A file store has a record ceiling of its own, below max_payload's
		// upper range, and past it the refusal is 10077, fatal: sizing
		// max_payload up to 64 MB would otherwise turn one large message into
		// the end of the process.
		if record := need + int64(len(n.cfg.Subject)) + 64; info.Config.Storage == jetstream.FileStorage &&
			record > jsFileStoreMaxRecord {
			return fatalErr("validate", fmt.Errorf(
				"stream %q uses file storage, which stores records of at most %d bytes, below the "+
					"largest message this server can accept (%d bytes; docs/SPEC.md §3.9); lower "+
					"tcp.max_frame_bytes and relp.max_frame_bytes", n.cfg.Stream, jsFileStoreMaxRecord, record))
		}
	}
	n.log.Info("stream validated",
		"stream", n.cfg.Stream, "subject", n.cfg.Subject,
		"storage", info.Config.Storage.String(), "replicas", info.Config.Replicas,
		"duplicate_window", info.Config.Duplicates.String(),
		"largest_message", n.maxMessageBytes(), "max_payload", nc.MaxPayload())
	return nil
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

// JetStream API error codes this sink classifies by number (SPEC §10.8.6). The
// client library names some of them; the rest come from the server's error
// table, and the numbers are stable across versions.
const (
	jsErrInsufficientResources = 10023
	jsErrMessageExceedsMaximum = 10054
	jsErrStreamNotMatch        = 10060
	jsErrStreamStoreFailed     = 10077
	jsErrHeaderExceedsMaximum  = 10097
	jsErrStreamSealed          = 10109
	jsErrDuplicateInProcess    = 10158
	jsErrStreamTooManyRequests = 10167
)

// classifyNATS maps a client or API error onto the class the state machine acts
// on. The table is SPEC §10.8.6; keep the two in step.
func classifyNATS(op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{classifyNATSError(err), op, err}
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

	// The stream is gone or JetStream is off: every batch would be refused.
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrJetStreamNotEnabled),
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return ClassFatal

	// One message does not fit. Unreachable after the boot check of SPEC
	// §10.8.5 unless the server changed underneath.
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
			case jsErrStreamNotMatch, jsErrStreamSealed:
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
				// fleet at once (SPEC §10.8.6).
				return ClassNetwork
			}
			// Any 429 is backpressure, never the request's form: it passes
			// once the server catches up, like 10167.
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
	// discarded batch is not (invariant §19.17).
	return ClassNetwork
}
