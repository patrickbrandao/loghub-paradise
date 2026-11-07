// Package verify checks, after a run, what the server wrote to its
// destination (SPEC §8.5): it reads NATS JetStream, PostgreSQL
// (loghub.inbox), Redis/Valkey (a stream) or SQLite (syslog_events), finds
// the messages of the run by their seq= and run= markers, and counts the
// lost and the duplicated ones.
//
// It is the only package of the injector with dependencies (SPEC §2.1): the
// clients the servers themselves use, at the same versions, so the check
// reads the destination the way the producer wrote it.
package verify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/report"
)

// Target is a validated destination (SPEC §8.5), not yet connected.
type Target struct {
	Kind  string // nats | redis | postgres | sqlite
	URL   string // the URL as given (valkey schemes rewritten); never printed
	Path  string // sqlite
	Space string
	Key   string // stream / table override; "" = the default of the kind
	Run   string
	// Since bounds the read: the run's start minus ClockSlack.
	Since time.Time
}

// ClockSlack is how far before the start of the run the destination is read
// (clock skew between the injector and the destination's host).
const ClockSlack = 5 * time.Minute

// Poll interval of the wait (SPEC §8.5).
const pollEvery = 250 * time.Millisecond

// Settle is the wait before the last read once every expected seq is found.
var Settle = time.Second

var (
	runRe   = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)
	keyRe   = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,255}$`)
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	spaceRe = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)
)

// Parse validates the flags of a check. Errors never quote the URL: it may
// carry a password.
func Parse(rawURL, sqlitePath, space, key, run string) (*Target, error) {
	if rawURL != "" && sqlitePath != "" {
		return nil, errors.New("--verify-url and --verify-sqlite are exclusive")
	}
	if !runRe.MatchString(run) {
		return nil, errors.New("--run: with --verify-*, only letters, digits and . _ : - (the marker is searched as text, also inside the escaped JLH)")
	}
	if !spaceRe.MatchString(space) || len(space) > 64 {
		return nil, errors.New("--verify-space: not a LogHub space name")
	}
	if key != "" && !keyRe.MatchString(key) {
		return nil, errors.New("--verify-key: not a name")
	}
	t := &Target{Space: space, Key: key, Run: run}
	if sqlitePath != "" {
		if strings.ContainsAny(sqlitePath, "?#") {
			return nil, errors.New("--verify-sqlite: the path cannot contain ? or #")
		}
		if key != "" && !identRe.MatchString(key) {
			return nil, errors.New("--verify-key: a SQLite table name is letters, digits and _")
		}
		t.Kind, t.Path = "sqlite", sqlitePath
		return t, nil
	}
	scheme, rest, ok := strings.Cut(rawURL, "://")
	if !ok {
		return nil, errors.New("--verify-url: not a URL (want nats://, tls://, postgres://, postgresql://, redis://, rediss://, valkey:// or valkeys://)")
	}
	switch strings.ToLower(scheme) {
	case "nats", "tls":
		t.Kind = "nats"
	case "redis", "rediss":
		t.Kind = "redis"
	case "valkey":
		t.Kind, rawURL = "redis", "redis://"+rest
	case "valkeys":
		t.Kind, rawURL = "redis", "rediss://"+rest
	case "postgres", "postgresql":
		t.Kind = "postgres"
		if key != "" {
			parts := strings.Split(key, ".")
			if len(parts) > 2 || !identRe.MatchString(parts[0]) || !identRe.MatchString(parts[len(parts)-1]) {
				return nil, errors.New("--verify-key: a PostgreSQL table is [schema.]table, letters, digits and _")
			}
		}
	default:
		return nil, errors.New("--verify-url: unsupported scheme (want nats://, tls://, postgres://, postgresql://, redis://, rediss://, valkey:// or valkeys://)")
	}
	t.URL = rawURL
	return t, nil
}

// Destination is what the report shows: scheme and hosts, never the user,
// the password or the query.
func (t *Target) Destination() string {
	if t.Kind == "sqlite" {
		return "sqlite " + t.Path
	}
	scheme, rest, _ := strings.Cut(t.URL, "://")
	var hosts []string
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimPrefix(part, scheme+"://")
		u, err := url.Parse(scheme + "://" + part)
		if err != nil || u.Host == "" {
			hosts = append(hosts, "?")
			continue
		}
		hosts = append(hosts, u.Host)
	}
	return scheme + "://" + strings.Join(hosts, ",")
}

// Counter counts the seqs of the run found in the destination.
type Counter struct {
	seen map[uint64]uint32
}

// NewCounter returns an empty counter.
func NewCounter() *Counter { return &Counter{seen: map[uint64]uint32{}} }

// Reset forgets everything (a reader that rereads the whole destination).
func (c *Counter) Reset() { clear(c.seen) }

// Add counts one copy of seq.
func (c *Counter) Add(seq uint64) { c.seen[seq]++ }

// reader is one kind of destination.
type reader interface {
	// poll reads what is new (or, for a rereading reader, everything after
	// c.Reset) and counts the messages of the run in c.
	poll(ctx context.Context, c *Counter) error
	// final is called when the wait ends: an error here (a stream that
	// never appeared) makes the check fail.
	final() error
	close()
}

// Check is an open destination.
type Check struct {
	t *Target
	r reader
}

// Open connects and checks the stream or table exists (SPEC §8.5), before
// anything is sent: an unreachable destination fails the run at once.
func Open(ctx context.Context, t *Target) (*Check, error) {
	var r reader
	var err error
	switch t.Kind {
	case "nats":
		r, err = openNATS(ctx, t)
	case "redis":
		r, err = openRedis(ctx, t)
	case "postgres":
		r, err = openPostgres(ctx, t)
	case "sqlite":
		r, err = openSQLite(ctx, t)
	default:
		err = fmt.Errorf("unknown destination %q", t.Kind)
	}
	if err != nil {
		return nil, err
	}
	return &Check{t: t, r: r}, nil
}

// Close releases the connection.
func (c *Check) Close() { c.r.close() }

// Wait reads the destination until every expected seq is found or timeout
// passes, and returns the counts.
func (c *Check) Wait(ctx context.Context, expected *report.SeqSet, timeout time.Duration) *report.Verify {
	v := &report.Verify{Destination: c.t.Destination(), Expected: expected.Len()}
	cnt := NewCounter()
	start := time.Now()
	deadline := start.Add(timeout)
	var err error
	for {
		pctx, cancel := context.WithDeadline(ctx, deadline.Add(5*time.Second))
		err = c.r.poll(pctx, cnt)
		cancel()
		if err != nil {
			break
		}
		if tally(cnt, expected, v); v.Found == v.Expected {
			v.Complete = true
			// One more read a moment later: a copy sent with the burst
			// (the same seq twice) lands right after the first one.
			if settle := min(Settle, time.Until(deadline)); settle > 0 && ctx.Err() == nil {
				select {
				case <-time.After(settle):
				case <-ctx.Done():
				}
				pctx, cancel := context.WithDeadline(ctx, deadline.Add(5*time.Second))
				err = c.r.poll(pctx, cnt)
				cancel()
			}
			break
		}
		if !time.Now().Add(pollEvery).Before(deadline) || ctx.Err() != nil {
			break
		}
		select {
		case <-time.After(pollEvery):
		case <-ctx.Done():
		}
	}
	if err == nil && !v.Complete {
		err = c.r.final()
	}
	tally(cnt, expected, v)
	v.WaitedSec = time.Since(start).Seconds()
	if err != nil {
		v.Error = err.Error()
	}
	return v
}

// tally fills the counts of v from what was found.
func tally(c *Counter, expected *report.SeqSet, v *report.Verify) {
	v.Found, v.Duplicated, v.Unexpected = 0, 0, 0
	for seq, n := range c.seen {
		if n > 1 {
			v.Duplicated += uint64(n - 1)
		}
		if expected.Has(seq) {
			v.Found++
		} else {
			v.Unexpected++
		}
	}
	v.Lost = v.Expected - v.Found
	v.LostSeqs = lostRanges(c, expected, 20)
}

func lostRanges(c *Counter, expected *report.SeqSet, max int) string {
	var out []string
	var from, to uint64
	flush := func() {
		if from == 0 {
			return
		}
		if from == to {
			out = append(out, strconv.FormatUint(from, 10))
		} else {
			out = append(out, strconv.FormatUint(from, 10)+"-"+strconv.FormatUint(to, 10))
		}
		from = 0
	}
	for seq := uint64(1); seq < expected.Max() && len(out) < max; seq++ {
		if !expected.Has(seq) || c.seen[seq] > 0 {
			flush()
			continue
		}
		if from == 0 {
			from = seq
		}
		to = seq
	}
	if len(out) < max {
		flush()
	}
	return strings.Join(out, ",")
}

// Markers returns the seq of a message of run: text must hold the words
// run=<run> and seq=<n> (a word runs between the start, the end or a
// space).
func Markers(text, run string) (uint64, bool) {
	var seq uint64
	gotSeq, gotRun := false, false
	for _, w := range strings.FieldsFunc(text, func(r rune) bool { return r == ' ' }) {
		switch {
		case strings.HasPrefix(w, "seq="):
			n, err := strconv.ParseUint(w[4:], 10, 64)
			if err == nil && n > 0 && !gotSeq {
				seq, gotSeq = n, true
			}
		case strings.HasPrefix(w, "run="):
			if w[4:] == run {
				gotRun = true
			}
		}
	}
	if !gotSeq || !gotRun {
		return 0, false
	}
	return seq, true
}

// fromPayload takes the seq from payload.message, or from payload.raw when
// message has no markers (a raw template the server parsed otherwise).
func fromPayload(message, raw, run string) (uint64, bool) {
	if s, ok := Markers(message, run); ok {
		return s, true
	}
	return Markers(raw, run)
}

// jlh is the part of the envelope the check reads.
type jlh struct {
	Space   string `json:"space"`
	Type    string `json:"type"`
	Payload struct {
		Message string `json:"message"`
		Raw     string `json:"raw"`
	} `json:"payload"`
}

// countJLH counts one envelope of the run.
func countJLH(data []byte, t *Target, c *Counter) {
	// Cheap filter before the JSON parse: the run marker is plain text in
	// the escaped JLH (Parse restricts the run to characters JSON keeps).
	if !strings.Contains(string(data), "run="+t.Run) {
		return
	}
	var e jlh
	if json.Unmarshal(data, &e) != nil || e.Type != "syslog" || e.Space != t.Space {
		return
	}
	if seq, ok := fromPayload(e.Payload.Message, e.Payload.Raw, t.Run); ok {
		c.Add(seq)
	}
}
