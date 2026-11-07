// Package report counts what a run did and prints it (SPEC §8): one outcome
// per message, a latency histogram, connection events, and the source's own
// counters (what a capture had and what was skipped).
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Hist is a log-linear latency histogram: 32 sub-buckets per power of two,
// so a percentile is off by at most ~3%, in a fixed 16 KiB.
type Hist struct {
	b   [64 * 32]uint64
	n   uint64
	sum time.Duration
	max time.Duration
	min time.Duration
}

const subBits = 5

func bucket(ns uint64) int {
	if ns < 1<<subBits {
		return int(ns)
	}
	e := bits.Len64(ns) - subBits
	return e<<subBits | int(ns>>(e-1)&(1<<subBits-1))
}

// lower bound, in ns, of bucket i
func bucketLow(i int) uint64 {
	if i < 1<<subBits {
		return uint64(i)
	}
	e := i >> subBits
	sub := uint64(i & (1<<subBits - 1))
	return (1<<subBits | sub) << (e - 1)
}

// Add records one latency.
func (h *Hist) Add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.b[bucket(uint64(d))]++
	if h.n == 0 || d < h.min {
		h.min = d
	}
	h.n++
	h.sum += d
	if d > h.max {
		h.max = d
	}
}

// Merge adds o into h.
func (h *Hist) Merge(o *Hist) {
	if o.n == 0 {
		return
	}
	for i, v := range o.b {
		h.b[i] += v
	}
	if h.n == 0 || o.min < h.min {
		h.min = o.min
	}
	h.n += o.n
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

// Quantile returns the q-quantile (0 < q <= 1), the midpoint of its bucket.
func (h *Hist) Quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.n)))
	if rank < 1 {
		rank = 1
	}
	var acc uint64
	for i, v := range h.b {
		acc += v
		if acc >= rank {
			lo := bucketLow(i)
			hi := bucketLow(i + 1)
			d := time.Duration((lo + hi) / 2)
			if d > h.max {
				d = h.max
			}
			if d < h.min {
				d = h.min
			}
			return d
		}
	}
	return h.max
}

// Shard is one worker's counters. Done may be called from any goroutine.
type Shard struct {
	r      *Report
	mu     sync.Mutex
	counts map[string]uint64
	hist   Hist
	acked  []uint64 // bitmap of the acknowledged seqs, with TrackAcked
}

// Done records the outcome of message seq. ok says whether the outcome is a
// success for the exit code (SPEC §8.3); bytes is what was put on the wire.
func (s *Shard) Done(seq uint64, outcome string, ok bool, lat time.Duration, bytes int) {
	s.mu.Lock()
	s.counts[outcome]++
	if ok {
		s.hist.Add(lat)
		if s.r.track && seq > 0 {
			w := int(seq / 64)
			if w >= len(s.acked) {
				s.acked = append(s.acked, make([]uint64, w+1-len(s.acked)+len(s.acked)/2)...)
			}
			s.acked[w] |= 1 << (seq % 64)
		}
	}
	s.mu.Unlock()
	if ok {
		s.r.ok.Add(1)
		s.r.bytes.Add(uint64(bytes))
	} else {
		s.r.failed.Add(1)
	}
}

// Report aggregates the shards of a run.
type Report struct {
	ok, failed, bytes atomic.Uint64
	dials, dialErrs   atomic.Uint64
	broken            atomic.Uint64

	mu       sync.Mutex
	shards   []*Shard
	events   map[string]uint64
	rejTexts int // distinct "rejected: " texts so far (SPEC §8.2)
	track    bool

	// Filled by the caller before Write.
	Info   map[string]string
	Source map[string]uint64
	Start  time.Time
	End    time.Time
	// OKOutcomes names the outcomes that count as success, in print order.
	OKOutcomes []string
	// Verify is the destination check (SPEC §8.5), when one ran.
	Verify *Verify
}

// TrackAcked makes the shards remember which seqs were acknowledged, for
// the destination check. Call before the run.
func (r *Report) TrackAcked() { r.track = true }

// Acked returns the acknowledged seqs as a bitmap (bit seq%64 of word
// seq/64), merged from the shards.
func (r *Report) Acked() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []uint64
	for _, s := range r.shards {
		s.mu.Lock()
		if len(s.acked) > len(out) {
			out = append(out, make([]uint64, len(s.acked)-len(out))...)
		}
		for i, w := range s.acked {
			out[i] |= w
		}
		s.mu.Unlock()
	}
	return out
}

// Rejection limits (SPEC §8.2): the text comes from the server.
const (
	MaxRejectTexts = 32
	MaxRejectText  = 120
	RejectOther    = "rejected: (other texts)"
)

// Rejected counts a non-200 answer by its text, with bounded cardinality.
func (r *Report) Rejected(text string) {
	key := "rejected: " + CleanText(text, MaxRejectText)
	r.mu.Lock()
	if _, ok := r.events[key]; !ok {
		if r.rejTexts >= MaxRejectTexts {
			key = RejectOther
		} else {
			r.rejTexts++
		}
	}
	r.events[key]++
	r.mu.Unlock()
}

// CleanText cuts s at max bytes without splitting a UTF-8 character and
// replaces control bytes (and invalid UTF-8) by '?'.
func CleanText(s string, max int) string {
	var b strings.Builder
	for _, c := range s {
		if c < 0x20 || c == 0x7f || c == utf8.RuneError {
			c = '?'
		}
		if b.Len()+utf8.RuneLen(c) > max {
			break
		}
		b.WriteRune(c)
	}
	return strings.TrimSpace(b.String())
}

// New returns an empty report.
func New() *Report {
	return &Report{events: map[string]uint64{}, Info: map[string]string{}}
}

// NewShard returns a shard for one worker.
func (r *Report) NewShard() *Shard {
	s := &Shard{r: r, counts: map[string]uint64{}}
	r.mu.Lock()
	r.shards = append(r.shards, s)
	r.mu.Unlock()
	return s
}

// Dial records a connection attempt.
func (r *Report) Dial(err error) {
	if err != nil {
		r.dialErrs.Add(1)
		r.Event("dial_error: " + shortErr(err))
		return
	}
	r.dials.Add(1)
}

// Broken records a connection lost during the run.
func (r *Report) Broken(err error) {
	r.broken.Add(1)
	r.Event("conn_broken: " + shortErr(err))
}

// Event counts a named event (deduplicated by text).
func (r *Report) Event(name string) {
	r.mu.Lock()
	r.events[name]++
	r.mu.Unlock()
}

// Dials returns the successful connections so far.
func (r *Report) Dials() uint64 { return r.dials.Load() }

// Totals returns the messages completed so far.
func (r *Report) Totals() (ok, failed uint64) { return r.ok.Load(), r.failed.Load() }

func shortErr(err error) string {
	s := err.Error()
	// "dial tcp 127.0.0.1:1514: connect: connection refused" -> keep the tail,
	// which is what repeats across attempts; the address is in the header.
	if i := strings.LastIndex(s, ": "); i >= 0 && i+2 < len(s) {
		s = s[i+2:]
	}
	return s
}

// Summary is the machine-readable report (--report json).
type Summary struct {
	Info       map[string]string `json:"info"`
	Start      time.Time         `json:"start"`
	ElapsedSec float64           `json:"elapsed_sec"`
	Messages   uint64            `json:"messages"`
	OK         uint64            `json:"ok"`
	Failed     uint64            `json:"failed"`
	Outcomes   map[string]uint64 `json:"outcomes"`
	Bytes      uint64            `json:"bytes"`
	RateMsgs   float64           `json:"rate_msgs_per_sec"`
	RateBytes  float64           `json:"rate_bytes_per_sec"`
	Latency    *Latency          `json:"latency,omitempty"`
	Conns      Conns             `json:"connections"`
	Events     map[string]uint64 `json:"events,omitempty"`
	Source     map[string]uint64 `json:"source,omitempty"`
	Verify     *Verify           `json:"verify,omitempty"`
}

// Verify is the result of the destination check (SPEC §8.5).
type Verify struct {
	Destination    string   `json:"destination"`
	OK             bool     `json:"ok"`
	Sent           uint64   `json:"sent"`
	Acked          uint64   `json:"acked"`
	Found          uint64   `json:"found"`
	Lost           uint64   `json:"lost"`
	MissingUnacked uint64   `json:"missing_unacked"`
	FoundUnacked   uint64   `json:"found_unacked"`
	Duplicated     uint64   `json:"duplicated"`
	ExtraCopies    uint64   `json:"extra_copies"`
	ExtraSameUUID  uint64   `json:"extra_copies_same_uuid"`
	Unexpected     uint64   `json:"unexpected"`
	EntriesRead    uint64   `json:"entries_read"`
	LostSample     []uint64 `json:"lost_sample,omitempty"`
	WaitedSec      float64  `json:"waited_sec"`
	Error          string   `json:"error,omitempty"`
	// Unreadable: the destination could not be read at all (exit 5).
	Unreadable bool `json:"unreadable,omitempty"`
}

// Latency is in microseconds.
type Latency struct {
	Min  float64 `json:"min_us"`
	Mean float64 `json:"mean_us"`
	P50  float64 `json:"p50_us"`
	P90  float64 `json:"p90_us"`
	P99  float64 `json:"p99_us"`
	P999 float64 `json:"p999_us"`
	Max  float64 `json:"max_us"`
}

// Conns are the connection events.
type Conns struct {
	Opened     uint64 `json:"opened"`
	DialErrors uint64 `json:"dial_errors"`
	Broken     uint64 `json:"broken"`
}

// Summarize merges the shards.
func (r *Report) Summarize() Summary {
	var h Hist
	out := map[string]uint64{}
	r.mu.Lock()
	for _, s := range r.shards {
		s.mu.Lock()
		h.Merge(&s.hist)
		for k, v := range s.counts {
			out[k] += v
		}
		s.mu.Unlock()
	}
	ev := make(map[string]uint64, len(r.events))
	for k, v := range r.events {
		ev[k] = v
	}
	r.mu.Unlock()
	el := r.End.Sub(r.Start).Seconds()
	s := Summary{
		Info:       r.Info,
		Start:      r.Start,
		ElapsedSec: el,
		OK:         r.ok.Load(),
		Failed:     r.failed.Load(),
		Outcomes:   out,
		Bytes:      r.bytes.Load(),
		Conns:      Conns{Opened: r.dials.Load(), DialErrors: r.dialErrs.Load(), Broken: r.broken.Load()},
		Events:     ev,
		Source:     r.Source,
		Verify:     r.Verify,
	}
	s.Messages = s.OK + s.Failed
	if el > 0 {
		s.RateMsgs = float64(s.OK) / el
		s.RateBytes = float64(s.Bytes) / el
	}
	if h.n > 0 {
		us := func(d time.Duration) float64 { return float64(d) / 1e3 }
		s.Latency = &Latency{
			Min:  us(h.min),
			Mean: us(h.sum / time.Duration(h.n)),
			P50:  us(h.Quantile(0.50)),
			P90:  us(h.Quantile(0.90)),
			P99:  us(h.Quantile(0.99)),
			P999: us(h.Quantile(0.999)),
			Max:  us(h.max),
		}
	}
	return s
}

// WriteJSON prints the summary as one JSON object.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r.Summarize())
}

// WriteText prints the human report.
func (r *Report) WriteText(w io.Writer) error {
	s := r.Summarize()
	var b strings.Builder
	keys := sortedKeys(s.Info)
	for _, k := range keys {
		fmt.Fprintf(&b, "%-14s %s\n", k+":", s.Info[k])
	}
	fmt.Fprintf(&b, "%-14s %.3fs\n", "elapsed:", s.ElapsedSec)
	fmt.Fprintf(&b, "%-14s %d (ok %d, failed %d)\n", "messages:", s.Messages, s.OK, s.Failed)
	if len(s.Outcomes) > 0 {
		b.WriteString("outcomes:\n")
		for _, k := range r.outcomeOrder(s.Outcomes) {
			fmt.Fprintf(&b, "  %-24s %d\n", k, s.Outcomes[k])
		}
	}
	fmt.Fprintf(&b, "%-14s %.1f msg/s, %s/s (%s total)\n", "throughput:", s.RateMsgs, human(s.RateBytes), human(float64(s.Bytes)))
	if l := s.Latency; l != nil {
		fmt.Fprintf(&b, "%-14s min %s  mean %s  p50 %s  p90 %s  p99 %s  p99.9 %s  max %s\n", "latency:",
			dur(l.Min), dur(l.Mean), dur(l.P50), dur(l.P90), dur(l.P99), dur(l.P999), dur(l.Max))
	}
	fmt.Fprintf(&b, "%-14s opened %d, dial errors %d, broken %d\n", "connections:", s.Conns.Opened, s.Conns.DialErrors, s.Conns.Broken)
	if len(s.Events) > 0 {
		b.WriteString("events:\n")
		for _, k := range sortedKeys(s.Events) {
			fmt.Fprintf(&b, "  %-40s %d\n", k, s.Events[k])
		}
	}
	if len(s.Source) > 0 {
		b.WriteString("source:\n")
		for _, k := range sortedKeys(s.Source) {
			fmt.Fprintf(&b, "  %-24s %d\n", k, s.Source[k])
		}
	}
	if v := s.Verify; v != nil {
		verdict := "ok"
		if !v.OK {
			verdict = "FAILED"
		}
		fmt.Fprintf(&b, "%-14s %s %s (waited %.1fs)\n", "verify:", verdict, v.Destination, v.WaitedSec)
		for _, f := range []struct {
			k string
			v uint64
		}{
			{"sent", v.Sent}, {"acked", v.Acked}, {"found", v.Found}, {"lost", v.Lost},
			{"missing_unacked", v.MissingUnacked}, {"found_unacked", v.FoundUnacked},
			{"duplicated", v.Duplicated}, {"extra_copies", v.ExtraCopies},
			{"extra_copies_same_uuid", v.ExtraSameUUID}, {"unexpected", v.Unexpected},
			{"entries_read", v.EntriesRead},
		} {
			if f.v != 0 || f.k == "lost" || f.k == "duplicated" {
				fmt.Fprintf(&b, "  %-24s %d\n", f.k, f.v)
			}
		}
		if len(v.LostSample) > 0 {
			fmt.Fprintf(&b, "  %-24s %v\n", "lost_sample", v.LostSample)
		}
		if v.Error != "" {
			fmt.Fprintf(&b, "  %-24s %s\n", "error", v.Error)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func (r *Report) outcomeOrder(m map[string]uint64) []string {
	var out []string
	seen := map[string]bool{}
	for _, k := range r.OKOutcomes {
		if _, ok := m[k]; ok {
			out = append(out, k)
			seen[k] = true
		}
	}
	for _, k := range sortedKeys(m) {
		if !seen[k] {
			out = append(out, k)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	k := make([]string, 0, len(m))
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}

func human(v float64) string {
	switch {
	case v >= 1<<30:
		return fmt.Sprintf("%.2f GiB", v/(1<<30))
	case v >= 1<<20:
		return fmt.Sprintf("%.2f MiB", v/(1<<20))
	case v >= 1<<10:
		return fmt.Sprintf("%.2f KiB", v/(1<<10))
	}
	return fmt.Sprintf("%.0f B", v)
}

func dur(us float64) string {
	switch {
	case us >= 1e6:
		return fmt.Sprintf("%.2fs", us/1e6)
	case us >= 1e3:
		return fmt.Sprintf("%.2fms", us/1e3)
	}
	return fmt.Sprintf("%.0fµs", us)
}

// Progress prints one line per tick until stop is closed.
func (r *Report) Progress(w io.Writer, every time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	var lastOK uint64
	last := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			ok, failed := r.Totals()
			rate := float64(ok-lastOK) / now.Sub(last).Seconds()
			fmt.Fprintf(w, "[%6.1fs] ok %d  failed %d  %.0f msg/s  conns %d\n",
				now.Sub(r.Start).Seconds(), ok, failed, rate, r.Dials())
			lastOK, last = ok, now
		}
	}
}
