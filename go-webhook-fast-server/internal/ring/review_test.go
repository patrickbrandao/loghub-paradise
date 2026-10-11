package ring

// The pool hand-back of ring=none (SPEC §12.3, the ownership rule).

import (
	"context"
	"errors"
	"testing"
	"time"
)

// SPEC §12.3, the `none` row: the buffers go back to the pool when the
// synchronous write returns success -- ownership moves on exactly as it does
// after a worker's confirmed batch. On an error the ring leaves the decision to
// the handler, which knows which errors still reference the buffers.
func TestNoneReleasesTheItemAfterASuccessfulWrite(t *testing.T) {
	ok := &probingWriter{probe: func(context.Context) error { return nil }}
	r := NewNone(ok, 4, time.Second)

	it := mkItem(10)
	if err := r.Enqueue(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	if !it.Released() || it.Env != nil {
		t.Fatal("a confirmed synchronous write left the item unreleased; ring=none never pooled on success")
	}

	failing := &probingWriter{probe: func(context.Context) error { return errors.New("destination refused") }}
	r = NewNone(failing, 4, time.Second)
	it = mkItem(10)
	if err := r.Enqueue(context.Background(), it); err == nil {
		t.Fatal("the write error was swallowed")
	}
	if it.Released() || it.Env == nil {
		t.Fatal("the ring released an item after a FAILED write; that decision belongs to the handler (SPEC §12.3)")
	}
}
