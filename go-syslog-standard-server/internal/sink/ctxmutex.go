package sink

import "context"

// ctxMutex is a mutex whose Lock can also be waited for within a context: the
// health probe of sink=sqlite must not wait for the writer beyond
// sink.health_timeout (SPEC §10.4). It is a one-slot channel; build it with
// newCtxMutex.
type ctxMutex struct{ ch chan struct{} }

func newCtxMutex() ctxMutex { return ctxMutex{ch: make(chan struct{}, 1)} }

func (m ctxMutex) Lock() { m.ch <- struct{}{} }

func (m ctxMutex) Unlock() { <-m.ch }

// LockCtx takes the lock, or reports false when ctx ends first (nothing is
// held then).
func (m ctxMutex) LockCtx(ctx context.Context) bool {
	select {
	case m.ch <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}
