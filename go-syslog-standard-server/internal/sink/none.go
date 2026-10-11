package sink

import "context"

// None discards everything (SPEC §10.6).
//
// It exists to measure the cost of the receive path, the framing, the parse and
// the envelope with no destination I/O at all. It is the only case where
// "accepted" and "discarded" coexist, and it is by explicit configuration.
//
// It never leaves UP: there is nothing that could fail.
type None struct{}

func NewNone() *None { return &None{} }

func (None) Write(context.Context, []Prepared) error { return nil }
func (None) Health(context.Context) error            { return nil }
func (None) Close() error                            { return nil }
func (None) Type() string                            { return "none" }
