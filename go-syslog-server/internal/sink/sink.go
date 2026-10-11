// Package sink delivers accepted messages to their final destination
// (SPEC §10).
//
// The six implementations -- nats, redis, sqlite, postgres, mysql and none --
// share one interface, one set of drain workers and one health state machine.
// Nothing here consults the ring's mode, and nothing in the ring consults the
// sink's type: the orthogonality of SPEC §2.1 is a property of the code, not
// just the document.
//
// The drain workers run the FULL CONVERSION (SPEC §2.2): the ring holds head +
// raw line, and a worker turns each item into exactly one JLH envelope with
// internal/convert. The JLH is the ONLY data any destination receives (decision
// 4): no raw line beside it, no field outside it. So a Sink is handed a Batch of
// Envelopes -- the JLH bytes, the conversion result the SQL columns come from,
// and the item of origin -- built once per chunk and reused, byte for byte,
// across every retry (SPEC §10.2, the JLH outbox rule).
package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// Envelope is one item together with the JLH built from it.
//
// JLH is a slice of the chunk buffer, capacity-limited to its own length so a
// destination appending to it can never write into the next envelope. It is
// valid only until the chunk is finished; destinations must not keep it past
// the Write that received it, except where SPEC §15.2 says the chunk is then
// tainted and left to the GC.
//
// Res carries the promoted columns of SPEC §3.7 (SQLite, MySQL). Its text fields
// are filled only when the conversion ran with convert.Config.Columns (see
// NeedsColumns); SID and Priority always are.
type Envelope struct {
	Item *item.Item
	JLH  []byte
	Res  convert.Result
}

// UUID is the envelope's uuid in canonical lowercase form.
func (e *Envelope) UUID() string { return e.Item.Head.UUID.String() }

// Batch is one chunk of SPEC §10.1: the envelopes of a dequeued batch whose JLH
// sum stays within sink.batch_max_bytes. Bytes is that sum.
type Batch struct {
	Envelopes []Envelope
	Bytes     int
}

// Sink is a delivery destination.
type Sink interface {
	// Write delivers the whole chunk or fails with a classified error
	// (ClassNetwork, ClassLogic, ClassFatal) for the state machine of SPEC §13.3
	// to act on. On failure the caller keeps the items and retries with the
	// SAME batch; Write must never take ownership of the items or release their
	// buffers.
	Write(ctx context.Context, b *Batch) error

	// Health reports whether the destination is usable. It drives the UP/DOWN
	// state machine and is the ONLY thing that does (SPEC §13.3).
	Health(ctx context.Context) error

	// Close releases the destination's resources.
	Close() error

	// Type is the configured name, for logs and the syslog_config_info metric.
	Type() string
}

// ColumnSink is implemented by the destinations that store the promoted columns
// of SPEC §3.7. The workers and the ring=none writer switch
// convert.Config.Columns on for them by themselves: a SQL sink fed by a
// converter without columns would store NULL in every text column, silently.
type ColumnSink interface {
	NeedsColumns() bool
}

// WorkerLimiter is implemented by a destination that admits fewer concurrent
// writers than sink.workers may ask for: SQLite serialises writers, so more than
// one would only produce SQLITE_BUSY (SPEC §10.4). Config warns about the
// request; the workers clamp to the limit regardless.
type WorkerLimiter interface {
	MaxWorkers() int
}

// NeedsColumns reports whether sink.type stores the promoted columns of SPEC
// §3.7, which is when convert.Config.Columns must be on. The other destinations
// carry only the JLH -- loghub.inbox included, which takes the header and the
// payload (§3.7.1) -- and the columns would cost one string per field per
// message for nothing.
func NeedsColumns(sinkType string) bool {
	switch sinkType {
	case "sqlite", "mysql":
		return true
	}
	return false
}

// MaxEventBytes is the worst JLH of SPEC §3.9 for a raw line of at most
// maxMessageBytes octets (L = syslog.max_message_bytes):
//
//	12 × L + 8 KiB
//
// The converted text is at most 6 × L (every byte a control character escaped
// as \u00XX), raw at most another 6 × L, and the keys, numbers, host, tls,
// x-proxy, parse and the envelope header fit the fixed 8 KiB. The boot compares
// it with max_payload and max_allowed_packet; TestWorstCaseJLHIsBounded proves
// it with adversarial lines.
func MaxEventBytes(maxMessageBytes int64) int64 {
	return 12*maxMessageBytes + 8<<10
}

// Class categorises a failure so the state machine can act on it without
// knowing anything about the destination.
type Class uint8

const (
	// ClassNetwork is a transient reachability failure. It does NOT change the
	// UP/DOWN state on its own: the chunk stays with its worker and is retried,
	// so nobody was confirmed for a message that was then lost.
	ClassNetwork Class = iota

	// ClassLogic means the destination is alive and refused this data
	// permanently: a constraint violation, a value too long. Retrying cannot
	// help, so the chunk is dropped and counted.
	ClassLogic

	// ClassFatal means the destination is alive but cannot accept writes at
	// all, and waiting will not fix it: Redis OOM, SQLITE_FULL, a missing table.
	// It ends the process (SPEC §13.3).
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
//
// Taint marks a failure after which a lower layer may still read the chunk's
// buffer even though Write returned: an async publish whose other futures are
// still pending, for example. It has the effect of a context error on the
// ownership rule of SPEC §15.2 -- neither the chunk buffer nor the raw lines go
// back to a pool -- without pretending the clock ended the attempt.
//
// Delivered, on a ClassLogic failure, lists by index into Batch.Envelopes the
// envelopes the destination STORED before refusing the rest. The chunk is
// still settled as a whole -- committed, the refusal is permanent -- but only
// the others are counted and logged as lost (SPEC §10.2). nil means none is
// known to be stored. Only destinations that know it exactly fill it:
//
//	mysql     statements run in order, so the stored prefix is exact
//	nats      jetstream: every future says whether its message was acked
//	postgres  one statement per chunk: nothing is stored when it fails
//	sqlite    one transaction per chunk, rolled back on failure
//	redis     never answers logic (the shared table has no such reply)
type Error struct {
	Class     Class
	Op        string
	Err       error
	Taint     bool
	Delivered []int
}

func (e *Error) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

func networkErr(op string, err error) error { return &Error{Class: ClassNetwork, Op: op, Err: err} }
func logicErr(op string, err error) error   { return &Error{Class: ClassLogic, Op: op, Err: err} }
func fatalErr(op string, err error) error   { return &Error{Class: ClassFatal, Op: op, Err: err} }

// tainting marks a classified error as tainting the chunk (see Error.Taint).
func tainting(err error) error {
	var e *Error
	if errors.As(err, &e) {
		e.Taint = true
		return err
	}
	return &Error{Class: ClassNetwork, Op: "write", Err: err, Taint: true}
}

// settleLogic splits a chunk refused for good (ClassLogic) into the envelopes
// the destination stored anyway and the uuids of the ones that are lost.
// Indexes out of range or repeated are ignored: a wrong count must never come
// out larger than the chunk.
func settleLogic(err error, envs []Envelope) (stored int, lost []string) {
	var delivered []bool
	var e *Error
	if errors.As(err, &e) && len(e.Delivered) > 0 {
		delivered = make([]bool, len(envs))
		for _, i := range e.Delivered {
			if i >= 0 && i < len(envs) && !delivered[i] {
				delivered[i] = true
				stored++
			}
		}
	}
	lost = make([]string, 0, len(envs)-stored)
	for i := range envs {
		if delivered == nil || !delivered[i] {
			lost = append(lost, envs[i].UUID())
		}
	}
	return stored, lost
}

// ClassOf extracts the class of an error, defaulting to network so an
// unrecognised failure is RETRIED rather than silently dropping data
// (invariant §20.18).
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
// answer, which is what taints a chunk (SPEC §15.2).
func isContextErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// taints reports whether, after this failure, a lower layer may still read the
// chunk: the attempt ended by the clock, or the destination said so.
func taints(err error) bool {
	if err == nil {
		return false
	}
	if isContextErr(err) {
		return true
	}
	var e *Error
	return errors.As(err, &e) && e.Taint
}

// errKind picks the syslog_sink_errors_total series for a non-fatal, non-logic
// failure. A write that outlived sink.op_timeout is still retried like any
// network failure, but it is counted apart: "slow" and "unreachable" are
// different incidents, and the kind="timeout" series of SPEC §14.1 exists for
// exactly that distinction.
func errKind(err error) obs.SinkErrKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return obs.SinkErrTimeout
	}
	return obs.SinkErrNetwork
}

// ErrClosed is returned by a sink used after Close.
var ErrClosed = fmt.Errorf("sink: closed")

// colText makes a value safe for a promoted text column (SPEC §3.7).
//
// The payload's strings are valid UTF-8 already (SPEC §6.7) but may hold NUL,
// which PostgreSQL refuses in text with 22021 -- a class-22 error that would
// drop the WHOLE chunk as logic, 255 innocent messages with the one that had a
// NUL in its hostname. The replacement happens in the queryable COPY only, and
// in all three databases, so that they answer the same WHERE the same way; env
// keeps the JLH byte for byte (invariant §20.7).
//
// Invalid UTF-8 is replaced too, as a last line of defence: the converter
// already decodes every field, so this branch should never run.
func colText(s string) string {
	if strings.IndexByte(s, 0) < 0 && utf8.ValidString(s) {
		return s
	}
	s = strings.ToValidUTF8(s, "�")
	return strings.ReplaceAll(s, "\x00", "�")
}

// colTextLen is len(colText(s)) without building it: each NUL grows by two
// bytes. The MySQL chunker sizes rows with it (SPEC §10.6).
func colTextLen(s string) int {
	if utf8.ValidString(s) {
		return len(s) + 2*strings.Count(s, "\x00")
	}
	return len(colText(s))
}

// The obs label enums mirror the converter's, so a conversion result can index
// a metric array directly with no translation table.
//
// These assertions make a reordering in EITHER package a compile error rather
// than a silently mislabelled metric. The array-index form is deliberate: a
// plain subtraction would only catch a shift in one direction, because unsigned
// constant arithmetic that comes out positive is perfectly legal; an index must
// be exactly zero.
var (
	_ = [1]struct{}{}[obs.Format(convert.FormatRFC5424)-obs.FormatRFC5424]
	_ = [1]struct{}{}[obs.Format(convert.FormatRFC3164)-obs.FormatRFC3164]
	_ = [1]struct{}{}[obs.Format(convert.FormatMalformed)-obs.FormatMalformed]
	_ = [1]struct{}{}[obs.Format(convert.NumFormats)-obs.NumFormats]
	_ = [1]struct{}{}[int(convert.NumWarnings)-obs.NumWarnings]
)
