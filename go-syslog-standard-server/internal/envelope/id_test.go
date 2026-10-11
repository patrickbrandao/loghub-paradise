package envelope

import (
	"testing"

	"github.com/google/uuid"
)

// github.com/google/uuid is a TEST-ONLY dependency, used here as an independent
// oracle: an id this package mints must be accepted by a second implementation
// as a valid version 7, RFC 4122 variant UUID. A layout bug in the generator
// would otherwise pass every test written against the generator's own idea of
// the format.
func TestNewIDIsAValidUUIDv7(t *testing.T) {
	for i := 0; i < 1000; i++ {
		s := NewID()
		u, err := uuid.Parse(s)
		if err != nil {
			t.Fatalf("%q does not parse: %v", s, err)
		}
		if u.Version() != 7 {
			t.Fatalf("%q is version %d, want 7", s, u.Version())
		}
		if u.Variant() != uuid.RFC4122 {
			t.Fatalf("%q has variant %v, want RFC 4122", s, u.Variant())
		}
		if len(s) != 36 {
			t.Fatalf("%q is not the canonical 36-character form", s)
		}
	}
}

// Ids are an ordering key (SPEC §3.1): minted in sequence, they must not go
// backwards. Equal-microsecond pairs may tie on the time bits and then order on
// the random tail, so the check is "never before the previous millisecond",
// which is what the 48-bit prefix guarantees.
func TestNewIDDoesNotGoBackwardsInTime(t *testing.T) {
	prev, _ := uuid.Parse(NewID())
	for i := 0; i < 10000; i++ {
		cur, _ := uuid.Parse(NewID())
		pt, ct := prev.Time(), cur.Time()
		if ct < pt {
			t.Fatalf("id time went backwards: %v then %v", prev, cur)
		}
		prev = cur
	}
}

func TestNewIDIsUnique(t *testing.T) {
	seen := make(map[string]bool, 50000)
	for i := 0; i < 50000; i++ {
		s := NewID()
		if seen[s] {
			t.Fatalf("duplicate id %s after %d", s, i)
		}
		seen[s] = true
	}
}
