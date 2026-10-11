package envelope

import (
	crand "crypto/rand"
	rand "math/rand/v2"

	loghubuuid "github.com/patrickbrandao/go-loghub-uuid"
)

// UUIDv7 generation (RFC 9562 §5.7) via
// github.com/patrickbrandao/go-loghub-uuid at Level2.
//
// Level2 keeps the standard 48-bit millisecond timestamp in bytes 0-5 and packs
// the microsecond fraction of that millisecond (0-999) into the 12 bits of
// rand_a. rand_b stays fully random. Version (7) and variant (RFC 4122) are
// preserved, so the value is a valid UUIDv7 and still sorts lexicographically
// by creation time -- now to microsecond resolution. Instances of this server
// do not share state (SPEC §1); with millisecond-only ids every message minted
// in the same millisecond across N nodes ordered arbitrarily, and Level2
// shrinks that window a thousandfold (SPEC §16.3).
//
// The library's layout, version/variant masking and canonical text formatting
// are used as-is. The random bits are NOT: NewGeneratorWith feeds the generator
// math/rand/v2's top-level Uint64, whose per-P ChaCha8 state is lock-free,
// syscall-free and cryptographically strong. The library's own default seeds a
// statistical PCG from crypto/rand and, should crypto/rand ever fail, silently
// degrades to a clock-seeded PCG; neither behaviour is wanted on this path.
//
// At Level2 rand_a stops carrying entropy: an id holds 62 random bits inside a
// microsecond bucket instead of 74 inside a millisecond one. Against collision
// that is a net LOSS -- 12 bits surrendered buys 1000x more buckets, so ~4.1x
// more expected collisions for the same traffic. It is accepted because the
// absolute figure stays absurd and because the requirement here is collision,
// not secrecy: nothing derives a capability, token or signature from an id.
var uuidGen = loghubuuid.NewGeneratorWith(rand.Uint64)

// NewID returns a fresh UUIDv7 (Level2) in canonical 8-4-4-4-12 text form.
//
// It is generated before the item enters the ring, so the message has an
// identity for logs and correlation independent of any sink and of whether the
// sink ever accepts it. On the RELP transport it is also what makes the
// acknowledgement traceable to a row in the destination.
//
// The clock reading embedded here is the library's own, taken inside Generate;
// it is NOT the reading that fills ts/uts/nts. The two differ, and at Level2
// the difference is visible because both carry microseconds. ts/uts/nts are the
// authoritative instant (SPEC §3.1); the time inside the id is an ORDERING KEY.
func NewID() string {
	return uuidGen.GenerateString(loghubuuid.Level2)
}

// init fails loudly if the platform cannot provide entropy at all, rather than
// letting the process mint predictable ids.
func init() {
	var b [1]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("envelope: system entropy source unavailable: " + err.Error())
	}
}
