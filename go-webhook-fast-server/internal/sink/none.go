package sink

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// None discards every batch (SPEC §8.4).
//
// It exists for measurement, and the three combinations it enables are what
// make a performance regression locatable by subtraction rather than by
// profiler:
//
//	ring=none   + sink=none    absolute ceiling of pure HTTP
//	ring=memory + sink=none    the whole internal pipeline, minus destination I/O
//	ring=sqlite + sink=none    the isolated cost of the persistent ring's fsync
//
// This sink NEVER goes DOWN and is never FATAL: there is nothing that can fail.
type None struct{}

func NewNone() *None { return &None{} }

// Write reports success without doing anything.
//
// The events are counted as written, not as dropped: they reached the
// destination the operator configured. whs_events_dropped_total stays at zero
// because nothing was lost -- it was discarded on purpose.
func (n *None) Write(ctx context.Context, items []*envelope.Item) error { return nil }

func (n *None) Health(ctx context.Context) error { return nil }
func (n *None) Close() error                     { return nil }
func (n *None) Type() string                     { return "none" }
