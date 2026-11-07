package verify

import (
	"context"
	"encoding/base64"
	"hash/fnv"
	"regexp"
	"strconv"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
)

// Matcher finds the seq of the run in a message (SPEC §8.5).
type Matcher struct{ re *regexp.Regexp }

// NewMatcher compiles the pattern of tmpl.Template.Pattern.
func NewMatcher(pattern string) (*Matcher, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return &Matcher{re: re}, nil
}

// Seq returns the seq of the run in e: in message, else in raw.
func (m *Matcher) Seq(e Entry) (uint64, bool) {
	if s, ok := m.find(e.Message); ok {
		return s, true
	}
	raw := e.Raw
	if e.RawEncoding == "base64" {
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return 0, false
		}
		raw = string(b)
	}
	return m.find(raw)
}

func (m *Matcher) find(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	g := m.re.FindStringSubmatch(s)
	if g == nil {
		return 0, false
	}
	v, err := strconv.ParseUint(g[1], 10, 64)
	return v, err == nil
}

// Options of a check.
type Options struct {
	Run      string
	Since    time.Time     // start of the run
	Timeout  time.Duration // --verify-timeout
	AllowDup bool          // --verify-allow-duplicates
	// Poll and Settle: how often to read, and how long nothing new must
	// appear once every acknowledged message is there (defaults 250 ms, 1 s).
	Poll, Settle time.Duration
}

// tally counts the copies of each seq: a vector indexed by seq (SPEC §8.5).
type tally struct {
	sent     uint64
	copies   []uint16 // saturating, index seq
	first    []uint32 // hash of the uuid of the first copy
	sameUUID uint64   // copies past the first that repeat its uuid
	beyond   uint64   // records of the run with a seq outside 1..sent
	entries  uint64
	matched  uint64
}

func newTally(sent uint64) *tally {
	return &tally{sent: sent, copies: make([]uint16, sent+1), first: make([]uint32, sent+1)}
}

func uuidHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() | 1 // never 0, which means "no copy yet"
}

func (t *tally) add(m *Matcher, e Entry) {
	t.entries++
	seq, ok := m.Seq(e)
	if !ok {
		return
	}
	t.matched++
	if seq == 0 || seq > t.sent {
		t.beyond++
		return
	}
	u := uuidHash(e.UUID)
	if t.copies[seq] == 0 {
		t.first[seq] = u
	} else if t.first[seq] == u {
		t.sameUUID++
	}
	if t.copies[seq] < 0xFFFF {
		t.copies[seq]++
	}
}

func isAcked(acked []uint64, seq uint64) bool {
	w := seq / 64
	return w < uint64(len(acked)) && acked[w]&(1<<(seq%64)) != 0
}

// allAcked reports whether every acknowledged seq has a copy.
func (t *tally) allAcked(acked []uint64) bool {
	for s := uint64(1); s <= t.sent; s++ {
		if t.copies[s] == 0 && isAcked(acked, s) {
			return false
		}
	}
	return true
}

// MaxLostRanges bounds lost_seqs.
const MaxLostRanges = 20

// result is the count with the names and meanings of the SI (SPEC §8.5,
// decision 6 of 2026-11-07): expected = the successes (acked), duplicated =
// the copies past the first, unexpected = the records whose seq is not a
// success (informative only).
func (t *tally) result(acked []uint64, allowDup bool) *report.Verify {
	v := &report.Verify{DuplicatedSameUUID: t.sameUUID, Unexpected: t.beyond, EntriesRead: t.entries}
	var lo, hi uint64 // the open range of lost seqs
	closeRange := func() {
		if lo == 0 || len(v.LostSeqs) >= MaxLostRanges {
			return
		}
		r := strconv.FormatUint(lo, 10)
		if hi > lo {
			r += "-" + strconv.FormatUint(hi, 10)
		}
		v.LostSeqs = append(v.LostSeqs, r)
	}
	for s := uint64(1); s <= t.sent; s++ {
		a, n := isAcked(acked, s), uint64(t.copies[s])
		switch {
		case a && n > 0:
			v.Expected++
			v.Found++
		case a:
			v.Expected++
			v.Lost++
			if lo != 0 && hi == s-1 {
				hi = s
			} else {
				closeRange()
				lo, hi = s, s
			}
		case n == 0:
			v.MissingUnacked++
		default:
			v.Unexpected += n
		}
		if n > 1 {
			v.Duplicated += n - 1
		}
	}
	closeRange()
	v.OK = v.Lost == 0 && (allowDup || v.Duplicated == 0)
	return v
}

// openReader opens the reader of a check (a variable for the tests).
var openReader = open

// Check reads the destination until every acknowledged message of the run
// is there and nothing new appeared for Settle, or until Timeout (SPEC
// §8.5). sent is the number of seqs the producer released (1..sent); acked
// the bitmap of the acknowledged ones. A read error is retried until the
// deadline; when the last read failed, the count is not the destination's:
// Unreadable (exit 4).
func Check(ctx context.Context, t *Target, m *Matcher, sent uint64, acked []uint64, o Options) *report.Verify {
	if o.Poll <= 0 {
		o.Poll = 250 * time.Millisecond
	}
	if o.Settle <= 0 {
		o.Settle = time.Second
	}
	start := time.Now()
	deadline := start.Add(o.Timeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	tl := newTally(sent)
	var r reader
	var lastErr error
	lastOK, complete := false, false
	lastNew := start
	var lastMatched uint64
	for {
		if r == nil {
			var err error
			if r, err = openReader(ctx, t, o.Since, o.Run, false); err != nil {
				r, lastErr, lastOK = nil, err, false
			}
		}
		if r != nil {
			into := tl
			if !r.Incremental() {
				// A full read counts afresh, and replaces the count only
				// when it ends: one cut short left a false "lost"
				// (review finding 5 of 2026-11-07).
				into = newTally(sent)
			}
			before := lastMatched
			if err := r.Read(ctx, func(e Entry) { into.add(m, e) }); err != nil {
				lastErr, lastOK = err, false
				if !r.Incremental() {
					r.Close()
					r = nil
				}
			} else {
				tl, lastOK, lastErr = into, true, nil
			}
			// Incremental readers count up; a full one recounts: either
			// way, a different total means something new arrived.
			if lastMatched = tl.matched; lastMatched != before {
				lastNew = time.Now()
			}
		}
		if lastOK && tl.allAcked(acked) && time.Since(lastNew) >= o.Settle {
			complete = true
			break
		}
		if !time.Now().Add(o.Poll).Before(deadline) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(o.Poll):
		}
		if ctx.Err() != nil {
			break
		}
	}
	if r != nil {
		r.Close()
	}
	v := tl.result(acked, o.AllowDup)
	v.Destination = t.Display
	v.Complete = complete
	v.WaitedSec = time.Since(start).Seconds()
	if !lastOK {
		v.OK = false
		v.Unreadable = true
		if lastErr != nil {
			v.Error = lastErr.Error()
		} else {
			v.Error = "the destination was not read within --verify-timeout"
		}
	}
	return v
}
