package ring

// Tests written after the independent review of the standard server on
// 2026-09-14 (who returns a buffer to the pool, and when, on the ring=none
// write path; SPEC §15.2), ported with the code they guard.

import (
	"context"
	"errors"
	"testing"
	"time"
)

// SPEC §15.2, the ring=none path: the buffer goes back to the pool when the
// synchronous write returns success -- ownership moves on exactly as it does
// after a worker's confirmed batch. On an error the ring leaves the decision to
// the receiver, which knows which errors still reference the buffer.
func TestNoneReleasesTheItemAfterASuccessfulWrite(t *testing.T) {
	ok := &probingWriter{probe: func(context.Context) error { return nil }}
	r := NewNone(ok, 4, time.Second)

	it := mkItem(0, 10)
	if err := r.Enqueue(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	if it.Raw != nil || !it.Released() {
		t.Fatal("a confirmed synchronous write left the item unreleased; ring=none never pooled on success")
	}

	failing := &probingWriter{probe: func(context.Context) error { return errors.New("destination refused") }}
	r = NewNone(failing, 4, time.Second)
	it = mkItem(1, 10)
	if err := r.Enqueue(context.Background(), it); err == nil {
		t.Fatal("the write error was swallowed")
	}
	if it.Raw == nil || it.Released() {
		t.Fatal("the ring released an item after a FAILED write; that decision belongs to the receiver (SPEC §15.2)")
	}
}
