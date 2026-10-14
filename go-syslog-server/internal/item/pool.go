package item

import "sync"

// Buffer pooling, per SPEC §15.1 and §15.2.
//
// Three classes: 2 KiB, 8 KiB and 64 KiB. A syslog message has a median of
// 150-250 bytes and a p99 that rarely passes 2 KiB, and syslog.max_message_bytes
// caps every message at 64 KiB by default (SPEC §5.5). Above the largest class
// -- only when an operator raises that limit -- a buffer is allocated per
// message and NEVER pooled: pooling worst-case buffers makes the pool hold the
// worst case forever.
//
// The ownership rule is absolute:
//
//	The Item owns its buffer. It returns to the pool exactly once, and only
//	after the item has been confirmed by the sink. A retried batch does NOT
//	return it -- the item still owns it.

const (
	classTiny   = 2 << 10
	classSmall  = 8 << 10
	classMedium = 64 << 10

	// PoolMaxBytes is the largest message that is pooled.
	PoolMaxBytes = classMedium
)

var rawPools = [3]sync.Pool{
	{New: func() any { b := make([]byte, 0, classTiny); return &b }},
	{New: func() any { b := make([]byte, 0, classSmall); return &b }},
	{New: func() any { b := make([]byte, 0, classMedium); return &b }},
}

func classFor(n int) int {
	switch {
	case n <= classTiny:
		return 0
	case n <= classSmall:
		return 1
	case n <= classMedium:
		return 2
	}
	return -1
}

// GetRaw returns a zero-length slice with capacity for at least n bytes, and the
// class it came from. A class of -1 means the buffer must not be returned.
func GetRaw(n int) (buf []byte, class int) {
	class = classFor(n)
	if class < 0 {
		return make([]byte, 0, n), -1
	}
	p := rawPools[class].Get().(*[]byte)
	return (*p)[:0], class
}

// PutRaw returns a pooled buffer. Passing class -1 is a no-op. A buffer that
// grew past its class (an append that reallocated) is dropped rather than
// pooled under the wrong class.
func PutRaw(buf []byte, class int) {
	if class < 0 || class >= len(rawPools) || buf == nil {
		return
	}
	if cap(buf) < classSizes[class] {
		return
	}
	b := buf[:0]
	rawPools[class].Put(&b)
}

var classSizes = [3]int{classTiny, classSmall, classMedium}

// CopyRaw copies b into a pooled buffer and returns it with its class. It is
// what every receiver uses: the read buffer belongs to the connection and is
// overwritten by the next frame.
func CopyRaw(b []byte) ([]byte, int) {
	buf, class := GetRaw(len(b))
	return append(buf, b...), class
}

// PooledCap is the capacity GetRaw(n) will hand out: the class size for a
// pooled length, n itself above the largest class. It lets the configuration
// compute what one message of syslog.max_message_bytes costs the memory ring
// (SPEC §9.2, §13.2) before any buffer exists.
func PooledCap(n int) int {
	if c := classFor(n); c >= 0 {
		return classSizes[c]
	}
	return n
}
