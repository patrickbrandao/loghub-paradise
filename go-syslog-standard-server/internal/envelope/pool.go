package envelope

import "sync"

// Buffer pooling, per SPEC §14.1 and §14.2.
//
// Size classes stop at 256 KiB. Above that a buffer is allocated fresh and
// NEVER returned to a pool: pooling worst-case buffers makes the pool hold the
// worst case forever, and Go's large object heap already handles those well.
//
// The classes are smaller than go-webhook-server's (4/16/64/256 KiB) because
// the distribution is different. A syslog message has a median of 150-250 bytes
// and a p99 that rarely passes 2 KiB; a webhook body starts at kilobytes. Using
// the sibling's classes would round every single message up to 4 KiB and waste
// most of the pool.
//
// The ownership rule is absolute and is what the whole scheme rests on:
//
//	The Item owns its buffer. It returns to the pool exactly once, and only
//	after the item has been confirmed by the sink (or committed to the ring's
//	own storage). A retried batch does NOT return it -- the item still owns it.
//
// Returning a buffer that something still references would put one emitter's
// line inside another emitter's event. It is silent, it does not show up under
// light load, and it is the worst bug this program could have.

const (
	classTiny   = 2 << 10
	classSmall  = 8 << 10
	classMedium = 64 << 10
	classLarge  = 256 << 10

	// PoolMaxBytes is the largest message that is pooled. Anything above is
	// allocated per message.
	PoolMaxBytes = classLarge
)

var rawPools = [4]sync.Pool{
	{New: func() any { b := make([]byte, 0, classTiny); return &b }},
	{New: func() any { b := make([]byte, 0, classSmall); return &b }},
	{New: func() any { b := make([]byte, 0, classMedium); return &b }},
	{New: func() any { b := make([]byte, 0, classLarge); return &b }},
}

// envPool holds envelope serialisation buffers. These live inside one drain
// worker's batch and never enter the ring, so one class suffices: the envelope
// is bounded by the RFC 5424 field limits plus parse.sd_max_*.
var envPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 1<<10); return &b },
}

// classFor returns the pool index for a required capacity, or -1 when the size
// is beyond the largest class.
func classFor(n int) int {
	switch {
	case n <= classTiny:
		return 0
	case n <= classSmall:
		return 1
	case n <= classMedium:
		return 2
	case n <= classLarge:
		return 3
	}
	return -1
}

// GetRaw returns a zero-length slice with capacity for at least n bytes, and
// the class it came from. A class of -1 means the buffer is not pooled and must
// not be returned.
func GetRaw(n int) (buf []byte, class int) {
	class = classFor(n)
	if class < 0 {
		return make([]byte, 0, n), -1
	}
	p := rawPools[class].Get().(*[]byte)
	return (*p)[:0], class
}

// PutRaw returns a pooled buffer. class must be the value GetRaw returned;
// passing -1 is a no-op, so callers need no special case for huge messages.
func PutRaw(buf []byte, class int) {
	if class < 0 || class >= len(rawPools) || buf == nil {
		return
	}
	b := buf[:0]
	rawPools[class].Put(&b)
}

// GetEnv returns a zero-length envelope serialisation buffer.
func GetEnv() []byte {
	p := envPool.Get().(*[]byte)
	return (*p)[:0]
}

// PutEnv returns an envelope buffer. Buffers that grew far past the class size
// are dropped rather than pooled, so one pathological message with enormous
// structured data cannot make every pooled buffer permanently large.
func PutEnv(buf []byte) {
	if buf == nil || cap(buf) > 64<<10 {
		return
	}
	b := buf[:0]
	envPool.Put(&b)
}
