package ring

import (
	"context"
	"errors"
	"testing"
)

// Regression tests from the known-issues round of 2026-10-14 (SPEC §22.1).

// An abort closes the ring and only then reads its count (SPEC §13.4). An
// Enqueue that passed the closed check before the Close and reserved its slot
// after the count was read would be in the ring, answered 200, and lost by the
// os.Exit without being counted. Enqueue re-checks closed after the
// reservation: either the abort's count sees the reservation, or the Enqueue
// sees the close and gives up -- never an uncounted 200.
func TestEnqueueSeesACloseThatRacedTheReservation(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	afterReserve = func() { _ = r.Close() }
	defer func() { afterReserve = nil }()
	err := r.Enqueue(context.Background(), mkItem(0, 100))
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue racing a Close = %v, want ErrClosed", err)
	}
	if st := r.Stats(); st.Items != 0 || st.Bytes != 0 {
		t.Fatalf("a refused Enqueue left accounting behind: %+v", st)
	}
}

// Control: with no Close, the hook changes nothing.
func TestEnqueueWithoutACloseIsUnaffectedByTheHook(t *testing.T) {
	r := NewMemory(10, 1<<20, 0)
	afterReserve = func() {}
	defer func() { afterReserve = nil }()
	if err := r.Enqueue(context.Background(), mkItem(0, 100)); err != nil {
		t.Fatal(err)
	}
	if st := r.Stats(); st.Items != 1 || st.Bytes != held(100) {
		t.Fatalf("stats = %+v", st)
	}
}
