package app

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
)

// memSink is the destination of the in-process suite: it keeps a copy of every
// envelope it confirms, and it can be taken DOWN or made FATAL on cue. It lives
// here, in a test file, and reaches the app through App.sinkOverride; the
// binary has PostgreSQL and nothing else (SPEC §0, §15.3).
//
// The copy is the point: the item's buffer goes back to the pool once Write
// returns nil (SPEC §12.3), so keeping a reference would read another client's
// envelope later.
type memSink struct {
	mu   sync.Mutex
	envs [][]byte

	down  atomic.Bool // Health and Write fail as the network does
	fatal atomic.Bool // Health and Write answer "never" (SPEC §8.5.7)
	// hang makes Write wait for its context with Health still UP: a
	// destination that takes the batch and never answers.
	hang atomic.Bool

	// refuse, when set, makes a batch holding an envelope it matches fail as
	// logic -- the class-22 refusal of PostgreSQL -- with nothing written.
	refuse func(env []byte) bool
}

var (
	errMemDown  = &sink.Error{Class: sink.ClassNetwork, Op: "memsink", Err: errors.New("connection refused")}
	errMemFatal = &sink.Error{Class: sink.ClassFatal, Op: "memsink", Err: errors.New("relation does not exist")}
)

func (s *memSink) Write(ctx context.Context, items []*envelope.Item) error {
	if s.fatal.Load() {
		return errMemFatal
	}
	if s.down.Load() {
		return errMemDown
	}
	if s.hang.Load() {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return &sink.Error{Class: sink.ClassNetwork, Op: "memsink", Err: err}
	}
	if s.refuse != nil {
		for _, it := range items {
			if s.refuse(it.Env) {
				return &sink.Error{Class: sink.ClassLogic, Op: "memsink", Err: errors.New("22021 invalid byte sequence")}
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range items {
		s.envs = append(s.envs, bytes.Clone(it.Env))
	}
	return nil
}

func (s *memSink) Health(context.Context) error {
	if s.fatal.Load() {
		return errMemFatal
	}
	if s.down.Load() {
		return errMemDown
	}
	return nil
}

func (s *memSink) Close() error { return nil }
func (s *memSink) Type() string { return "memsink" }

// stored returns a copy of every envelope confirmed so far.
func (s *memSink) stored() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.envs...)
}

// count is how many envelopes were confirmed so far.
func (s *memSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.envs)
}
