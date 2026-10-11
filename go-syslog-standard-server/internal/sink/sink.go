// Package sink delivers accepted messages to their final destination
// (SPEC §10).
//
// The six implementations -- redis, sqlite, postgres, mysql, nats and none --
// share one interface, one set of drain workers and one health state machine.
// Nothing here consults the ring's mode, and nothing in the ring consults the
// sink's type: the orthogonality of SPEC §2.1 is a property of the code, not
// just the document.
//
// The drain workers do one thing the sibling service's do not: they run the FULL
// PARSE and build the envelope (SPEC §2.2). Only the PRI is read on the receive
// path. So a Sink is handed Prepared values, not bare items -- the parse result
// is computed once per batch and reused across retries.
package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Prepared is one item together with everything derived from its raw line.
//
// Env and Meta are owned by the Prep that built them and are valid only until
// that Prep is released. The Item is NOT: it follows the ownership rule of
// SPEC §14.2 and outlives the Prep whenever a batch is retried or abandoned.
type Prepared struct {
	Item *envelope.Item
	Env  []byte // serialised envelope of SPEC §3.1
	Meta envelope.Meta
}

// Raw is the wire bytes of the message, without any transport framing.
func (p Prepared) Raw() []byte { return p.Item.Raw }

// Sink is a delivery destination.
type Sink interface {
	// Write delivers the whole batch or fails. On failure the caller keeps the
	// items and retries; Write must not take ownership of them or release their
	// buffers unless it returns nil.
	Write(ctx context.Context, batch []Prepared) error

	// Health reports whether the destination is usable. It drives the UP/DOWN
	// state machine and is the ONLY thing that does (SPEC §12.2).
	Health(ctx context.Context) error

	// Close releases the destination's resources.
	Close() error

	// Type is the configured name, for logs and the syslog_config_info metric.
	Type() string
}

// Class categorises a failure so the state machine can act on it without
// knowing anything about the destination.
type Class uint8

const (
	// ClassNetwork is a transient reachability failure. It does NOT change the
	// UP/DOWN state on its own: the item stays in the ring and is retried, so
	// nobody was confirmed for a message that was then lost.
	ClassNetwork Class = iota

	// ClassLogic means the destination is alive and refused this data
	// permanently: WRONGTYPE, NOPERM, a constraint violation. Retrying cannot
	// help, so the event is dropped and counted.
	ClassLogic

	// ClassFatal means the destination is alive but cannot accept writes at
	// all, and waiting will not fix it: Redis OOM, SQLITE_FULL, SQLITE_CORRUPT,
	// a missing table. It ends the process immediately.
	ClassFatal
)

func (c Class) String() string {
	switch c {
	case ClassLogic:
		return "logic"
	case ClassFatal:
		return "fatal"
	}
	return "network"
}

// Error carries a failure together with its class.
type Error struct {
	Class Class
	Op    string
	Err   error
}

func (e *Error) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

func networkErr(op string, err error) error { return &Error{ClassNetwork, op, err} }
func logicErr(op string, err error) error   { return &Error{ClassLogic, op, err} }
func fatalErr(op string, err error) error   { return &Error{ClassFatal, op, err} }

// ClassOf extracts the class of an error, defaulting to network so an
// unrecognised failure is RETRIED rather than silently dropping data
// (invariant §19.17).
func ClassOf(err error) Class {
	if err == nil {
		return ClassNetwork
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassNetwork
}

// IsFatal reports whether an error must end the process.
func IsFatal(err error) bool { return err != nil && ClassOf(err) == ClassFatal }

// isContextErr reports whether a write ended by the clock rather than by an
// answer, which is what taints a batch (SPEC §14.2).
func isContextErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// errKind picks the syslog_sink_errors_total series for a non-fatal, non-logic
// failure. A write that outlived sink.op_timeout is still retried like any
// network failure, but it is counted apart: "slow" and "unreachable" are
// different incidents, and the kind="timeout" series of SPEC §13.1 exists for
// exactly that distinction.
func errKind(err error) obs.SinkErrKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return obs.SinkErrTimeout
	}
	return obs.SinkErrNetwork
}

// ErrClosed is returned by a sink used after Close.
var ErrClosed = fmt.Errorf("sink: closed")

// ----------------------------------------------------------------------- Prep

// Prep turns items into Prepared values: it runs the full parse and serialises
// the envelope.
//
// One Prep serves one drain worker for its whole lifetime. That is the point:
// the syslog.Parsed inside it keeps its structured-data slices and its unescape
// scratch allocated across every batch, so a steady stream of messages stops
// allocating after the first few.
//
// It is NOT safe for concurrent use. ring=none reaches it through a pool
// (see Instrumented), because there the writer is whichever receiver goroutine
// happens to be holding the message.
type Prep struct {
	parsed syslog.Parsed
	out    []Prepared
	nodeID string
	opt    syslog.Options
	m      *obs.Metrics
}

// NewPrep builds a preparer. m may be nil, which is what tests use.
func NewPrep(nodeID string, opt syslog.Options, m *obs.Metrics) *Prep {
	return &Prep{nodeID: nodeID, opt: opt, m: m}
}

// Build parses every item and returns the batch.
//
// The returned slice and every Env inside it are valid until Release. Build must
// be called ONCE per batch, before the retry loop: the parse is deterministic,
// so redoing it on each attempt would burn CPU proportional to the batch at the
// exact moment the system is already in trouble (SPEC §10.2).
func (p *Prep) Build(items []*envelope.Item) []Prepared {
	p.releaseEnvs()
	if cap(p.out) < len(items) {
		p.out = make([]Prepared, 0, len(items))
	}
	p.out = p.out[:0]

	start := time.Now()
	for _, it := range items {
		// The reception instant comes from the item head, never from the clock.
		// That is what makes the envelope derivable from head + raw line, and
		// it is what makes reingestion exact: a message dumped in December and
		// replayed in January must still get December's year for its RFC 3164
		// timestamp (invariant §19.13).
		recv := time.Unix(it.Head.TS, it.Head.NTS).UTC()

		hdr := 0
		if it.Head.FromWire {
			// The PRI was already validated on the receive path; re-reading it
			// costs five bytes and keeps this function independent of whatever
			// the receiver chose to remember.
			if _, n, ok := syslog.PRI(it.Raw); ok {
				hdr = n
			}
		}
		syslog.Parse(&p.parsed, it.Raw, hdr, recv, p.opt)

		env := envelope.AppendJSON(envelope.GetEnv(), it, &p.parsed, p.nodeID)
		p.out = append(p.out, Prepared{
			Item: it,
			Env:  env,
			Meta: envelope.BuildMeta(it, &p.parsed),
		})

		if p.m != nil {
			p.m.ParseTotal[p.parsed.RFC].Inc()
			for k, n := range p.parsed.Anomalies {
				if n > 0 {
					p.m.ParseAnomaliesTotal[k].Add(uint64(n))
				}
			}
		}
	}
	if p.m != nil && len(items) > 0 {
		// One observation per batch, divided, rather than one per message: the
		// histogram answers "is the parse cheap relative to the write", and
		// paying a binary search per message to ask it would be self-defeating.
		p.m.ParseDuration.ObserveDuration(time.Since(start) / time.Duration(len(items)))
	}
	return p.out
}

// Release returns the envelope buffers to the pool.
//
// It is called on EVERY exit path of a batch, including the ones that abandon
// the items to the emergency dump: the dump stores head + raw line and has no
// use for a serialised envelope (SPEC §11.1). The items themselves are governed
// by the stricter rule in SPEC §14.2 and are not touched here.
func (p *Prep) Release() {
	p.releaseEnvs()
	p.out = p.out[:0]
}

// Abandon drops the batch WITHOUT returning the envelope buffers to the pool.
//
// It is for a write that ended in a context error. rueidis returns early on a
// cancelled context but still sends the queued commands, which carry
// BinaryString views of these envelopes; pooling them now would let the next
// batch overwrite bytes that are still being serialised (SPEC §14.2). The
// buffers go to the GC instead, and the Prep stays usable.
func (p *Prep) Abandon() {
	for i := range p.out {
		p.out[i] = Prepared{}
	}
	p.out = p.out[:0]
}

func (p *Prep) releaseEnvs() {
	for i := range p.out {
		envelope.PutEnv(p.out[i].Env)
		p.out[i] = Prepared{}
	}
}

// The obs label enums mirror the parser's, so a parse result can index a metric
// array directly with no translation table.
//
// These assertions make a reordering in EITHER package a compile error rather
// than a silently mislabelled metric. The array-index form is deliberate: a
// plain subtraction would only catch a shift in one direction, because unsigned
// constant arithmetic that comes out positive is perfectly legal.
var (
	_ = [1]struct{}{}[obs.RFC(syslog.RFCUnknown)-obs.RFCUnknown]
	_ = [1]struct{}{}[obs.RFC(syslog.RFC3164)-obs.RFC3164]
	_ = [1]struct{}{}[obs.RFC(syslog.RFC5424)-obs.RFC5424]

	_ = [1]struct{}{}[obs.Anomaly(syslog.AnomDupSDID)-obs.AnomDupSDID]
	_ = [1]struct{}{}[obs.Anomaly(syslog.AnomSDTruncated)-obs.AnomSDTruncated]
	_ = [1]struct{}{}[obs.Anomaly(syslog.AnomSDUnterminated)-obs.AnomSDUnterminated]
	_ = [1]struct{}{}[obs.Anomaly(syslog.AnomBadTimestamp)-obs.AnomBadTimestamp]
)
