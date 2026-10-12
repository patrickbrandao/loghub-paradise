package sink

import "context"

// None discards everything (SPEC §10.8).
//
// It exists to measure the cost of the receive path, the framing, the
// conversion and the serialisation with no destination I/O at all: the workers
// still build every JLH (SPEC §10.1), or the benchmark would measure a pipeline
// that does not exist. It is the only case where "accepted" and "discarded"
// coexist, and it is by explicit configuration.
//
// It never leaves UP: there is nothing that could fail.
type None struct{}

func NewNone() *None { return &None{} }

func (None) Write(context.Context, *Batch) error { return nil }
func (None) Health(context.Context) error        { return nil }
func (None) Close() error                        { return nil }
func (None) Type() string                        { return "none" }
