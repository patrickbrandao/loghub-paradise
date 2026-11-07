// Package verify checks a run at the destination of the server (SPEC §10):
// after the sending, it reads what the server wrote -- NATS JetStream, the
// PostgreSQL loghub.inbox, a Redis/Valkey stream, the SQLite gelf_events --
// and counts, by the run's _seq markers, what was lost and what arrived
// twice. The formats are those of go-gelf-server's SPEC §3.6-§3.8.
package verify

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Window is the slack before the start of the run for the destinations
// read by time (PostgreSQL, SQLite): the UUIDv7 of the JLH comes from the
// server's clock, which may be another machine's (SPEC §10.3). The streams
// (NATS, Redis) start after their last entry at the check, with no clock.
const Window = 10 * time.Minute

// Poll is the interval between reads; Settle the extra read after every
// expected message was found (a duplicate of at-least-once comes right
// behind).
const (
	Poll   = 250 * time.Millisecond
	Settle = time.Second
)

// Dest is one destination.
type Dest interface {
	// Check opens the destination and confirms it can be read (before the
	// first send: a wrong URL must not cost a load).
	Check(ctx context.Context) error
	// Read adds to c what the destination holds of the run. An incremental
	// destination (a stream, which only grows) adds only what came since the
	// last Read; the others reread the whole window, and the caller resets c
	// first.
	Read(ctx context.Context, c *Counter) error
	Incremental() bool
	Close()
}

// Spec is a parsed --verify-url or --verify-sqlite: usage errors come from
// Parse (exit 2), connection errors from Check (exit 4).
type Spec struct {
	Kind     string   // nats | postgres | redis | sqlite
	Display  string   // scheme and host only (never user, password, path or query), or the file
	raw      string   // the URL for the driver (a NATS list: the elements joined by ",")
	secrets  []string // passwords (or NATS tokens) of every element, scrubbed from driver errors
	space    string   // --verify-space
	stream   string   // nats stream / redis key
	subject  string   // nats filter
	path     string   // sqlite file
	TLSRoots *x509.CertPool
}

// SpaceRE is the grammar of a LogHub space (the loghub.inbox CHECK).
var SpaceRE = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

// Parse reads a --verify-url with --verify-space and --verify-key (key may
// be empty: the default of the destination). Errors never repeat the URL:
// it may hold a password. A NATS list (comma-separated, as nats.go takes it)
// is split and every element parsed here, before any driver sees it: the
// driver's own parse error quotes the element, password included, and the
// second element of a list was never parsed before (review of 2026-11-07).
func Parse(raw, space, key string) (*Spec, error) {
	if !SpaceRE.MatchString(space) {
		return nil, errors.New("--verify-space: want a LogHub space ([A-Za-z0-9_-] words joined by dots)")
	}
	scheme, _, _ := strings.Cut(raw, "://")
	scheme = strings.ToLower(scheme)
	elems := []string{raw}
	if scheme == "nats" || scheme == "tls" {
		elems = strings.Split(raw, ",")
	}
	s := &Spec{space: space}
	var us []*url.URL
	for i, e := range elems {
		u, err := url.Parse(strings.TrimSpace(e))
		if err != nil {
			// Never the *url.Error text: it quotes the element.
			return nil, fmt.Errorf("--verify-url: server %d of the list is not a URL", i+1)
		}
		es := strings.ToLower(u.Scheme)
		if len(elems) > 1 && es != "nats" && es != "tls" {
			return nil, fmt.Errorf("--verify-url: server %d of the list is not nats:// or tls://", i+1)
		}
		if p, ok := u.User.Password(); ok && p != "" {
			s.secrets = append(s.secrets, p)
		} else if u.User != nil && u.User.Username() != "" && (es == "nats" || es == "tls") {
			s.secrets = append(s.secrets, u.User.Username()) // a NATS token goes as the user
		}
		if p := u.Query().Get("password"); p != "" {
			s.secrets = append(s.secrets, p) // libpq's password= parameter
		}
		us = append(us, u)
	}
	u := us[0]
	q := u.Query()
	switch scheme {
	case "nats", "tls":
		s.Kind = "nats"
		// The server publishes to loghub.<space>.gelf on the stream that
		// covers it (go-gelf-server SPEC §3.8, sink.nats.stream).
		s.stream, s.subject = "LOGHUB_GELF", "loghub."+space+".gelf"
		if key != "" {
			s.stream = key
		}
		for _, e := range us {
			if len(e.Query()) > 0 {
				return nil, errors.New("--verify-url: a nats:// URL takes no parameters (--verify-key names the stream)")
			}
		}
	case "postgres", "postgresql":
		s.Kind = "postgres"
		if key != "" {
			return nil, errors.New("--verify-key: the PostgreSQL queue is always loghub.inbox")
		}
	case "redis", "rediss", "valkey", "valkeys":
		s.Kind = "redis"
		// valkey:// is redis://, valkeys:// is rediss:// (the protocol is
		// the same, Valkey round of 2026-11-07).
		switch scheme {
		case "valkey":
			u.Scheme = "redis"
		case "valkeys":
			u.Scheme = "rediss"
		}
		s.stream = "loghub." + space + ".gelf" // sink.redis.stream with {space}
		if key != "" {
			s.stream = key
		}
	default:
		return nil, errors.New("--verify-url: want nats://, tls://, postgres://, postgresql://, redis://, rediss://, valkey:// or valkeys://")
	}
	for i, e := range us {
		if e.Host == "" && !(s.Kind == "postgres" && q.Has("host")) {
			return nil, fmt.Errorf("--verify-url: server %d of the list has no host", i+1)
		}
	}
	if s.Kind == "nats" {
		parts := make([]string, len(us))
		for i, e := range us {
			parts[i] = e.String()
		}
		s.raw = strings.Join(parts, ",")
	} else {
		s.raw = u.String()
	}
	host := u.Host
	if host == "" {
		host = "(socket)"
	}
	s.Display = scheme + "://" + host
	if len(us) > 1 {
		s.Display += fmt.Sprintf(" (+%d)", len(us)-1)
	}
	switch s.Kind {
	case "nats":
		s.Display += " (stream " + s.stream + ", subject " + s.subject + ")"
	case "redis":
		s.Display += " (stream " + s.stream + ")"
	case "postgres":
		s.Display += " (loghub.inbox, space " + space + ")"
	}
	return s, nil
}

// SQLite is the spec of --verify-sqlite.
func SQLite(path, space string) (*Spec, error) {
	if !SpaceRE.MatchString(space) {
		return nil, errors.New("--verify-space: want a LogHub space ([A-Za-z0-9_-] words joined by dots)")
	}
	return &Spec{Kind: "sqlite", path: path, space: space, Display: "sqlite " + path + " (gelf_events, space " + space + ")"}, nil
}

// scrub replaces the passwords of every element in a driver's error text.
func (s *Spec) scrub(t string) string {
	for _, p := range s.secrets {
		for _, f := range []string{p, url.QueryEscape(p), url.PathEscape(p), url.UserPassword("", p).String()[1:]} {
			if f != "" {
				t = strings.ReplaceAll(t, f, "xxxxx")
			}
		}
	}
	return t
}

// Err is a destination error fit to print: a *url.Error (it quotes a URL)
// becomes a generic text, and the passwords are scrubbed. ErrRunPresent
// passes as it is (the app tells it apart).
func (s *Spec) Err(err error) error {
	if err == nil || errors.Is(err, ErrRunPresent) {
		return err
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return errors.New("a server URL does not parse")
	}
	return errors.New(s.scrub(err.Error()))
}

// ErrRunPresent: the destination already holds records of this --run in
// the window (PostgreSQL, SQLite), found before anything is sent: the count
// would mix the two runs (exit 2, SPEC §10.3).
var ErrRunPresent = errors.New("--run already present at the destination; use another --run")

// Open returns the destination of s for the run, reading from since on.
func (s *Spec) Open(run string, since time.Time) Dest {
	switch s.Kind {
	case "nats":
		return &natsDest{s: s, run: run, since: since}
	case "postgres":
		return &pgDest{s: s, run: run, since: since}
	case "redis":
		return &redisDest{s: s, run: run, since: since}
	}
	return &sqliteDest{s: s, run: run, since: since}
}

// Counter is what a destination holds of the run: the copies of each seq
// produced in a vector (review of 2026-11-07, item 10: a map[uint64]uint32
// rebuilt at every full read cost ~40 bytes a seq; now 1 byte), the extra
// copies summed as they come, and the seqs of the run that were never
// produced in a small map.
type Counter struct {
	copies     []uint8 // index seq-1, seq 1..produced; saturating at 255
	extra      uint64  // copies beyond the first, every seq
	beyond     map[uint64]uint32
	Unparsable uint64
}

// NewCounter returns an empty counter for the seqs 1..produced.
func NewCounter(produced uint64) *Counter {
	return &Counter{copies: make([]uint8, produced), beyond: map[uint64]uint32{}}
}

// Count is how many copies of seq were counted (saturating at 255 for a seq
// produced).
func (c *Counter) Count(seq uint64) uint32 {
	if seq >= 1 && seq <= uint64(len(c.copies)) {
		return uint32(c.copies[seq-1])
	}
	return c.beyond[seq]
}

func (c *Counter) add(n uint64) {
	if n >= 1 && n <= uint64(len(c.copies)) {
		if c.copies[n-1] > 0 {
			c.extra++
		}
		if c.copies[n-1] < 255 {
			c.copies[n-1]++
		}
		return
	}
	if c.beyond[n] > 0 {
		c.extra++
	}
	c.beyond[n]++
}

// AddSeq counts one record whose fields.seq is text (a JSON number's text, or
// a string's value).
func (c *Counter) AddSeq(text string) {
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil || n == 0 || strings.HasPrefix(text, "+") {
		c.Unparsable++
		return
	}
	c.add(n)
}

// jlh is the part of an envelope the check reads.
type jlh struct {
	Type    string `json:"type"`
	Payload struct {
		Fields map[string]json.RawMessage `json:"fields"`
	} `json:"payload"`
}

// AddJLH counts one envelope when it is a gelf event of run.
func (c *Counter) AddJLH(b []byte, run string) {
	var e jlh
	if json.Unmarshal(b, &e) != nil || e.Type != "gelf" {
		return
	}
	if text(e.Payload.Fields["run"]) != run || run == "" {
		return
	}
	c.AddSeq(text(e.Payload.Fields["seq"]))
}

// text is a JSON scalar as text: a string's value, a number's literal.
func text(r json.RawMessage) string {
	if len(r) == 0 {
		return ""
	}
	if r[0] == '"' {
		var s string
		if json.Unmarshal(r, &s) != nil {
			return ""
		}
		return s
	}
	return string(r)
}

// Tracker records the outcome of each message by seq (sched.Plan.Track).
type Tracker struct {
	mu sync.Mutex
	st []uint8 // index seq-1: 0 no outcome, 1 success, 2 failure
}

// Done implements sched.Plan.Track.
func (t *Tracker) Done(seq uint64, ok bool) {
	if seq == 0 {
		return
	}
	v := uint8(2)
	if ok {
		v = 1
	}
	t.mu.Lock()
	for uint64(len(t.st)) < seq {
		t.st = append(t.st, 0)
	}
	t.st[seq-1] = v
	t.mu.Unlock()
}

func (t *Tracker) snapshot(produced uint64) []uint8 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]uint8, produced)
	copy(out, t.st)
	return out
}

// Result is the "verify" object of the report (SPEC §10.4). The names and
// their meaning follow go-syslog-injector's (expected = successes,
// duplicated = copies beyond the first, unexpected = seqs of the run found
// that are no success; aligned in the review of 2026-11-07).
type Result struct {
	Destination      string   `json:"destination"`
	Run              string   `json:"run"`
	Reads            uint64   `json:"reads"`
	Waited           float64  `json:"waited"` // seconds, from the end of the sending
	Complete         bool     `json:"complete"`
	Expected         uint64   `json:"expected"`
	Found            uint64   `json:"found"`
	Lost             uint64   `json:"lost"`
	Duplicated       uint64   `json:"duplicated"`
	FailedFound      uint64   `json:"failed_found"`
	FailedMissing    uint64   `json:"failed_missing"`
	Unexpected       uint64   `json:"unexpected"`
	Unparsable       uint64   `json:"unparsable"`
	LostSeqs         []string `json:"lost_seqs,omitempty"` // the first 20 ranges, "4" or "10-19"
	DuplicatedSample []uint64 `json:"duplicated_sample,omitempty"`
	UnexpectedSample []uint64 `json:"unexpected_sample,omitempty"`
	Error            string   `json:"error,omitempty"`
	// Failed says the check could not read the destination at all (exit 4).
	Failed bool `json:"-"`
}

// Bad says the destination lost or doubled a successful message (exit 1).
// unexpected and unparsable are informative, as in go-syslog-injector: a
// failure the server processed anyway is already a failure in the report.
func (r *Result) Bad() bool {
	return r.Lost > 0 || r.Duplicated > 0
}

const (
	sampleMax = 10
	rangesMax = 20
)

// addRange extends the lost ranges with seq (seqs come in ascending order).
func addRange(r []string, last *uint64, start *uint64, seq uint64) []string {
	if *last != 0 && seq == *last+1 {
		*last = seq
		return r
	}
	if *last != 0 {
		r = append(r, rangeText(*start, *last))
	}
	*start, *last = seq, seq
	return r
}

func rangeText(a, b uint64) string {
	if a == b {
		return strconv.FormatUint(a, 10)
	}
	return strconv.FormatUint(a, 10) + "-" + strconv.FormatUint(b, 10)
}

// Classify compares the outcomes with what the destination holds.
func Classify(states []uint8, c *Counter) Result {
	var r Result
	var rs, rl uint64
	full := false
	var dups, unexp []uint64
	for i, st := range states {
		seq := uint64(i + 1)
		n := c.Count(seq)
		switch {
		case st == 1 && n > 0:
			r.Expected++
			r.Found++
		case st == 1:
			r.Expected++
			r.Lost++
			if !full {
				r.LostSeqs = addRange(r.LostSeqs, &rl, &rs, seq)
				full = len(r.LostSeqs) == rangesMax
			}
		case n > 0:
			r.FailedFound++
			r.Unexpected++
			if len(unexp) < sampleMax {
				unexp = append(unexp, seq)
			}
		default:
			r.FailedMissing++
		}
		if n > 1 && len(dups) < sampleMax {
			dups = append(dups, seq)
		}
	}
	if rl != 0 && !full {
		r.LostSeqs = append(r.LostSeqs, rangeText(rs, rl))
	}
	// Seqs of the run that were never produced (0 is unparsable): beyond the
	// last, or produced by a reused --run (SPEC §10.1).
	for seq, n := range c.beyond {
		r.Unexpected++
		unexp = append(unexp, seq)
		if n > 1 {
			dups = append(dups, seq)
		}
	}
	r.Duplicated = c.extra
	r.DuplicatedSample = sample(dups)
	r.UnexpectedSample = sample(unexp)
	r.Unparsable = c.Unparsable
	return r
}

func sample(v []uint64) []uint64 {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	if len(v) > sampleMax {
		v = v[:sampleMax]
	}
	return v
}

// Run waits for the destination to hold every successful message of the
// run, at most timeout after the end of the sending (SPEC §10.3), and
// classifies what it holds. produced is the number of messages produced
// (seq 1..produced). ctx ends the wait early (an interrupt).
func Run(ctx context.Context, s *Spec, d Dest, run string, t *Tracker, produced uint64, timeout time.Duration) Result {
	start := time.Now()
	deadline := start.Add(timeout)
	states := t.snapshot(produced)
	c := NewCounter(produced)
	var r Result
	var reads uint64
	var lastErr error
	good, settled := false, false
	for {
		if !d.Incremental() {
			c = NewCounter(produced)
		}
		rctx, cancel := context.WithDeadline(ctx, deadline.Add(5*time.Second))
		err := d.Read(rctx, c)
		cancel()
		reads++
		if err != nil {
			lastErr = s.Err(err)
		} else {
			good, lastErr = true, nil
			r = Classify(states, c)
		}
		wait := Poll
		if err == nil && r.Lost == 0 {
			if settled {
				r.Complete = true
				break
			}
			settled, wait = true, Settle
		}
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			break
		}
		if !sleep(ctx, min(wait, time.Until(deadline))) {
			break
		}
	}
	if !good {
		// Nothing ever read: the counters would claim every message lost.
		r = Result{Failed: true}
	}
	r.Reads = reads
	r.Destination, r.Run = s.Display, run
	r.Waited = time.Since(start).Seconds()
	if lastErr != nil {
		r.Error = lastErr.Error()
	}
	return r
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// uuidFloor is the smallest UUIDv7 of the millisecond of t, in canonical
// text: every UUIDv7 made at or after t sorts at or above it, as bytes (the
// PostgreSQL uuid order) and as lowercase text (the SQLite column).
func uuidFloor(t time.Time) string {
	ms := uint64(t.UnixMilli())
	if t.UnixMilli() < 0 {
		ms = 0
	}
	var b [16]byte
	for i := range 6 {
		b[i] = byte(ms >> (8 * (5 - i)))
	}
	const hx = "0123456789abcdef"
	out := make([]byte, 0, 36)
	for i, v := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out = append(out, '-')
		}
		out = append(out, hx[v>>4], hx[v&15])
	}
	return string(out)
}
