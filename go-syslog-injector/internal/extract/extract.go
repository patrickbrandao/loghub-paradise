// Package extract finds the syslog messages inside captured traffic (SPEC
// §6.6): one per UDP datagram, and the frames of a reassembled TCP stream in
// any of the three framings this ecosystem speaks -- RFC 6587 octet-counting,
// RFC 6587 non-transparent (LF) and RELP, whose "syslog" commands carry them.
//
// The framing is detected once per stream, like the servers do it (the
// go-syslog-server SPEC §5.2); a stream that began before the capture, or
// that lost bytes in the capture, is resynchronized on the next position that
// verifiably starts a frame, and what was skipped is counted.
package extract

import (
	"bytes"
	"time"
)

// Mode is the framing of a stream.
type Mode uint8

const (
	ModeDetect Mode = iota
	ModeOctet
	ModeLF
	ModeRELP
	ModeIgnore
)

func (m Mode) String() string {
	return [...]string{"detect", "octet-counting", "non-transparent", "relp", "ignored"}[m]
}

// MaxFrame bounds a frame: a larger declared length is not a frame start, and
// a longer line is cut there (SPEC §6.6).
const MaxFrame = 16 << 20

// resyncWindow: a resynchronizing stream with no candidate keeps only this
// tail, enough for a frame header split across two segments.
const resyncWindow = 64

// Stats counts what the extractor did, over all streams.
type Stats struct {
	Datagrams     uint64 // UDP datagrams taken as messages
	DatagramsSkip uint64 // UDP datagrams that do not look like syslog
	Streams       [5]uint64
	Messages      uint64 // messages from streams
	RELPOther     uint64 // RELP frames that were not "syslog" (open, close)
	Resyncs       uint64
	SkippedBytes  uint64 // bytes dropped while resynchronizing
	LostFrames    uint64 // frames cut by a capture gap or by the end of the stream
	LongLines     uint64 // non-transparent lines cut at MaxFrame
}

// Datagram reports whether a UDP payload is taken as a message. Strict (no
// --pcap-port given) requires the PRI's "<": without it, DNS and NTP would be
// replayed as syslog.
func Datagram(p []byte, strict bool, st *Stats) bool {
	if len(p) == 0 || (strict && p[0] != '<') {
		st.DatagramsSkip++
		return false
	}
	st.Datagrams++
	return true
}

// Stream extracts the messages of one TCP direction. It implements
// pcap.StreamHandler.
type Stream struct {
	emit   func(msg []byte, ts time.Time)
	st     *Stats
	strict bool
	mode   Mode
	resync bool
	buf    []byte
	ts     time.Time
}

// NewStream returns the parser of one direction. A midstream direction starts
// resynchronizing: its first byte is not a frame start.
func NewStream(strict, midstream bool, st *Stats, emit func([]byte, time.Time)) *Stream {
	return &Stream{emit: emit, st: st, strict: strict, resync: midstream}
}

// Mode returns the framing decided so far.
func (s *Stream) Mode() Mode { return s.mode }

// Data implements pcap.StreamHandler.
func (s *Stream) Data(b []byte, ts time.Time) {
	if s.mode == ModeIgnore {
		return
	}
	s.ts = ts
	s.buf = append(s.buf, b...)
	s.run(false)
}

// Gap implements pcap.StreamHandler: the frame in progress lost bytes and
// cannot be replayed whole.
func (s *Stream) Gap(int) {
	if s.mode == ModeIgnore {
		return
	}
	if len(s.buf) > 0 {
		s.st.LostFrames++
		s.st.SkippedBytes += uint64(len(s.buf))
		s.buf = s.buf[:0]
	}
	if !s.resync {
		s.resync = true
		s.st.Resyncs++
	}
}

// End implements pcap.StreamHandler.
func (s *Stream) End() {
	if s.mode == ModeIgnore {
		return
	}
	s.run(true)
	if len(s.buf) == 0 {
		s.count()
		return
	}
	if s.mode == ModeLF && !s.resync {
		// The server delivers a last line without terminator whole (its
		// SPEC §5.3), so the replay does too.
		s.out(s.buf)
	} else {
		s.st.LostFrames++
		s.st.SkippedBytes += uint64(len(s.buf))
	}
	s.buf = nil
	s.count()
}

func (s *Stream) count() {
	s.st.Streams[s.mode]++
	s.mode = ModeIgnore // End is final
}

func (s *Stream) out(m []byte) {
	s.st.Messages++
	s.emit(m, s.ts)
}

// run consumes every complete frame in the buffer.
func (s *Stream) run(final bool) {
	for len(s.buf) > 0 && s.mode != ModeIgnore {
		if s.resync {
			if !s.sync(final) {
				return
			}
			continue
		}
		if s.mode == ModeDetect {
			m, ok := detect(s.buf, s.strict, final)
			if !ok {
				return // need more bytes
			}
			s.mode = m
			if m == ModeIgnore {
				s.buf = nil
				return
			}
		}
		n, msg, st := frame(s.mode, s.buf, final)
		switch st {
		case fNeed:
			return
		case fBad:
			s.resync = true
			s.st.Resyncs++
			continue
		case fOK:
			if msg != nil {
				s.out(msg)
			}
		case fLong:
			s.st.LongLines++
			s.out(msg)
		case fOther:
			s.st.RELPOther++
		}
		s.buf = s.buf[n:]
	}
}

// sync drops bytes up to the next verified frame start. It returns false
// when it needs more data.
func (s *Stream) sync(final bool) bool {
	pos, m, ok := findSync(s.buf, s.mode, s.strict, final)
	if !ok {
		if final {
			s.st.SkippedBytes += uint64(len(s.buf))
			s.buf = s.buf[:0]
			return false
		}
		if len(s.buf) > MaxFrame+resyncWindow {
			drop := len(s.buf) - resyncWindow
			s.st.SkippedBytes += uint64(drop)
			s.buf = append(s.buf[:0], s.buf[drop:]...)
		}
		return false
	}
	s.st.SkippedBytes += uint64(pos)
	s.buf = s.buf[pos:]
	if s.mode == ModeDetect {
		s.mode = m
	}
	s.resync = false
	return true
}

type fstate uint8

const (
	fNeed fstate = iota
	fBad
	fOK
	fOther // a RELP frame that carries no message
	fLong  // a non-transparent line cut at MaxFrame
)

// detect decides the framing from the first bytes. ok is false when the
// buffer is too short to decide.
func detect(b []byte, strict, final bool) (Mode, bool) {
	if cmd, _, st := relpHeader(b); st == hOK {
		switch cmd {
		case "open", "syslog", "close":
			return ModeRELP, true
		case "rsp", "serverclose":
			return ModeIgnore, true // the server's direction of a RELP session
		}
		// "2026 is a year": words, not a RELP command; try the others.
	} else if st == hNeed && !final {
		return 0, false
	}
	if n, _, st := octetHeader(b); st == hOK {
		if !strict {
			return ModeOctet, true
		}
		if len(b) <= n {
			if final {
				return ModeIgnore, true
			}
			return 0, false
		}
		if b[n] == '<' {
			return ModeOctet, true
		}
	} else if st == hNeed && !final {
		return 0, false
	}
	if !strict || b[0] == '<' {
		return ModeLF, true
	}
	return ModeIgnore, true
}

type hstate uint8

const (
	hNeed hstate = iota
	hBad
	hOK
)

// octetHeader parses "MSG-LEN SP". n is the header length.
func octetHeader(b []byte) (n, msgLen int, st hstate) {
	if len(b) == 0 {
		return 0, 0, hNeed
	}
	if b[0] < '1' || b[0] > '9' {
		return 0, 0, hBad
	}
	v := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c == ' ' {
			if v > MaxFrame {
				return 0, 0, hBad
			}
			return i + 1, v, hOK
		}
		if c < '0' || c > '9' || i >= 10 {
			return 0, 0, hBad
		}
		v = v*10 + int(c-'0')
	}
	return 0, 0, hNeed
}

// relpHeader parses "TXNR SP COMMAND SP" (the DATALEN is parsed by frame).
// n is the offset of DATALEN.
func relpHeader(b []byte) (cmd string, n int, st hstate) {
	i := 0
	for i < len(b) && i < 10 && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	if i == len(b) {
		return "", 0, hNeed
	}
	if i == 0 || i > 9 || b[i] != ' ' {
		return "", 0, hBad
	}
	j := i + 1
	for j < len(b) && j-i <= 33 && b[j] >= 'a' && b[j] <= 'z' {
		j++
	}
	if j == len(b) {
		if j-i > 33 {
			return "", 0, hBad
		}
		return "", 0, hNeed
	}
	if j == i+1 || j-i > 33 || b[j] != ' ' {
		return "", 0, hBad
	}
	return string(b[i+1 : j]), j + 1, hOK
}

// frame parses one frame of a known mode at the start of b.
func frame(m Mode, b []byte, final bool) (n int, msg []byte, st fstate) {
	switch m {
	case ModeOctet:
		h, l, hs := octetHeader(b)
		switch hs {
		case hNeed:
			return 0, nil, fNeed
		case hBad:
			return 0, nil, fBad
		}
		if len(b) < h+l {
			return 0, nil, fNeed
		}
		return h + l, b[h : h+l], fOK
	case ModeLF:
		i := bytes.IndexAny(b, "\n\x00")
		if i < 0 {
			if len(b) >= MaxFrame {
				return MaxFrame, b[:MaxFrame], fLong
			}
			return 0, nil, fNeed
		}
		line := b[:i]
		for len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if len(line) == 0 {
			return i + 1, nil, fOK // empty frame: nothing to replay
		}
		return i + 1, line, fOK
	case ModeRELP:
		cmd, h, hs := relpHeader(b)
		switch hs {
		case hNeed:
			return 0, nil, fNeed
		case hBad:
			return 0, nil, fBad
		}
		// DATALEN, then SP DATA or nothing, then LF.
		i := h
		v := 0
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			if i-h >= 9 {
				return 0, nil, fBad
			}
			v = v*10 + int(b[i]-'0')
			i++
		}
		if i == len(b) {
			return 0, nil, fNeed
		}
		if i == h || v > MaxFrame {
			return 0, nil, fBad
		}
		var data []byte
		if v > 0 {
			if b[i] != ' ' {
				return 0, nil, fBad
			}
			if len(b) < i+1+v+1 {
				return 0, nil, fNeed
			}
			data = b[i+1 : i+1+v]
			i += 1 + v
		}
		if i >= len(b) {
			return 0, nil, fNeed
		}
		if b[i] != '\n' {
			return 0, nil, fBad
		}
		if cmd != "syslog" {
			return i + 1, nil, fOther
		}
		if len(data) == 0 {
			return i + 1, nil, fOK
		}
		return i + 1, data, fOK
	}
	return 0, nil, fBad
}

// findSync returns the first position where a frame verifiably starts. For a
// known mode only that mode is searched; ModeDetect searches all three and
// returns the mode found. ok is false when no position qualifies yet -- also
// when a candidate before the best one is still waiting for bytes, so the
// choice never depends on how the capture split the stream.
func findSync(b []byte, m Mode, strict, final bool) (pos int, found Mode, ok bool) {
	best, wait := -1, -1
	var bestMode Mode
	try := func(mode Mode) {
		if m != ModeDetect && m != mode {
			return
		}
		p, w := syncMode(b, mode, strict, final)
		if p >= 0 && (best < 0 || p < best) {
			best, bestMode = p, mode
		}
		if w >= 0 && (wait < 0 || w < wait) {
			wait = w
		}
	}
	try(ModeRELP)
	try(ModeOctet)
	try(ModeLF)
	if best < 0 || (!final && wait >= 0 && wait <= best) {
		return 0, 0, false
	}
	return best, bestMode, true
}

// pendCap: while resynchronizing, a candidate whose frame has not arrived
// yet holds the search only if it declares at most this much; a larger
// declared length is more likely text that looks like a header.
const pendCap = 64 << 10

// syncMode returns the first verified candidate of one mode, or -1; and the
// position of a candidate before it that still waits for bytes, or -1.
func syncMode(b []byte, mode Mode, strict, final bool) (pos, wait int) {
	switch mode {
	case ModeLF:
		// The byte after a terminator starts a line. Strict streams also
		// want the PRI there, so text inside a binary flow does not match.
		for i := 0; i < len(b); i++ {
			if b[i] != '\n' && b[i] != 0 {
				continue
			}
			if i+1 >= len(b) {
				if final {
					return -1, -1
				}
				return -1, i + 1
			}
			if !strict || b[i+1] == '<' {
				return i + 1, -1
			}
		}
	case ModeRELP:
		// A RELP frame starts after a LF and parses whole up to its own LF.
		for i := 0; i < len(b); i++ {
			if i > 0 && b[i-1] != '\n' {
				continue
			}
			if !relpKnown(b[i:]) {
				continue
			}
			switch _, _, st := frame(ModeRELP, b[i:], final); st {
			case fOK, fOther:
				return i, -1
			case fNeed:
				if !final && relpPending(b[i:]) {
					return -1, i
				}
			}
		}
	case ModeOctet:
		// A candidate "LEN SP <" is verified by the frame after it: its
		// header must parse whole (and start with "<" when strict).
		for i := 0; i < len(b); i++ {
			if b[i] < '1' || b[i] > '9' || (i > 0 && b[i-1] >= '0' && b[i-1] <= '9') {
				continue
			}
			h, l, st := octetHeader(b[i:])
			if st != hOK || i+h >= len(b) {
				// The header, or the byte after it, has not arrived.
				if (st == hNeed || st == hOK) && !final {
					return -1, i
				}
				continue
			}
			if strict && b[i+h] != '<' {
				continue
			}
			end := i + h + l
			switch {
			case end > len(b):
				if !final && l <= pendCap {
					return -1, i
				}
				continue
			case end == len(b):
				if final {
					return i, -1
				}
				return -1, i
			}
			nh, _, nst := octetHeader(b[end:])
			switch nst {
			case hOK:
				if !strict {
					return i, -1
				}
				if end+nh < len(b) {
					if b[end+nh] == '<' {
						return i, -1
					}
					continue
				}
				if final {
					return i, -1
				}
				return -1, i
			case hNeed:
				if final {
					return i, -1
				}
				return -1, i
			}
		}
	}
	return -1, -1
}

// relpKnown reports whether b starts with a header of a RELP command (a
// "1 a 5 ..." in a message is not a frame start).
func relpKnown(b []byte) bool {
	cmd, _, st := relpHeader(b)
	if st != hOK {
		return st == hNeed
	}
	switch cmd {
	case "open", "syslog", "close", "rsp", "serverclose":
		return true
	}
	return false
}

// relpPending reports whether b starts with a RELP header that could still
// become a frame of at most pendCap bytes.
func relpPending(b []byte) bool {
	cmd, h, st := relpHeader(b)
	if st == hNeed {
		return true
	}
	if st != hOK {
		return false
	}
	switch cmd {
	case "open", "syslog", "close", "rsp", "serverclose":
	default:
		return false
	}
	v := 0
	for i := h; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
		v = v*10 + int(b[i]-'0')
		if v > pendCap {
			return false
		}
	}
	return true
}
