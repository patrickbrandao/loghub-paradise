// Package sink delivers accepted events to their final destination (SPEC §8).
//
// This server has one implementation, nats (SPEC §8.7), behind the same
// interface, drain workers and health state machine as go-webhook-pg-server
// and go-webhook-fast-server, so fixes port by diff (SPEC §2.1). Nothing here
// consults the ring's mode, and nothing in the ring consults the sink: the
// orthogonality of SPEC §2.1 is a property of the code, not just the document.
package sink

import (
	"context"
	"errors"
	"fmt"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
)

// Sink is a delivery destination.
type Sink interface {
	// Write delivers the whole batch or fails. On failure the caller keeps the
	// items and retries; Write must not take ownership of them or release their
	// buffers unless it returns nil. It may Taint them (SPEC §12.3): a lower
	// layer that can still read Env after the call returned says so that way.
	//
	// A ClassLogic error may carry Delivered: the destination stored those
	// items and refused the others for good (SPEC §8.1).
	Write(ctx context.Context, items []*envelope.Item) error

	// Health reports whether the destination is usable. It drives the UP/DOWN
	// state machine and is the ONLY thing that does (SPEC §10.2).
	Health(ctx context.Context) error

	// Close releases the destination's resources.
	Close() error

	// Type is the configured name, for logs and the whs_config_info metric.
	Type() string
}

// Class categorises a failure so the state machine can act on it without
// knowing anything about the destination.
type Class uint8

const (
	// ClassNetwork is a transient reachability failure. It does NOT change the
	// UP/DOWN state on its own: the item stays in the ring and is retried, so
	// nobody was told 200 for an event that was then lost.
	ClassNetwork Class = iota

	// ClassLogic means the destination is alive and refused THIS data
	// permanently: a message larger than it takes. Retrying cannot help, so
	// the event is dropped and counted. A refusal that would hit every event
	// the same way -- a missing stream, a revoked permission -- is not logic
	// but fatal (SPEC §8.7.6).
	ClassLogic

	// ClassFatal means the destination is alive but cannot accept writes at
	// all, and waiting will not fix it: a dropped or sealed stream, JetStream
	// out of resources, credentials refused (SPEC §8.7.6).
	// It ends the process immediately.
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

	// Delivered, on a ClassLogic error, lists the indexes of the batch the
	// destination STORED although it refused others for good: the partial
	// delivery of SPEC §8.1 and §8.7.2. nil means none was stored -- the whole
	// batch was refused. A refusal in the middle of a batch is as partial as
	// one at the end, and treating it as whole counted delivered events as
	// lost and, worse, left the unpublished rest confirmed.
	Delivered []int
}

func (e *Error) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

func networkErr(op string, err error) error { return &Error{Class: ClassNetwork, Op: op, Err: err} }
func logicErr(op string, err error) error   { return &Error{Class: ClassLogic, Op: op, Err: err} }
func fatalErr(op string, err error) error   { return &Error{Class: ClassFatal, Op: op, Err: err} }

// DeliveredOf returns the indexes a logic error reports as stored, and whether
// it reports any at all (SPEC §8.1).
func DeliveredOf(err error) ([]int, bool) {
	var e *Error
	if errors.As(err, &e) && e.Class == ClassLogic && e.Delivered != nil {
		return e.Delivered, true
	}
	return nil, false
}

// ClassOf extracts the class of an error, defaulting to network so an
// unrecognised failure is retried rather than silently dropping data.
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

// errKind picks the whs_sink_errors_total series for a non-fatal, non-logic
// failure. A write that outlived sink.op_timeout is still retried like any
// network failure, but it is counted apart: "slow" and "unreachable" are
// different incidents, and the kind="timeout" series of SPEC §11.1 exists for
// exactly that distinction.
func errKind(err error) obs.SinkErrKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return obs.SinkErrTimeout
	}
	return obs.SinkErrNetwork
}

// ErrClosed is returned by a sink used after Close.
var ErrClosed = fmt.Errorf("sink: closed")
