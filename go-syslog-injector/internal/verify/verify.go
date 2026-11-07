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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
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
	// Since bounds the PostgreSQL and SQLite read: the run's start minus
	// ClockSlack. NATS and Redis start after the stream's last entry at Open
	// instead (SPEC §8.5).
	Since time.Time
	// TLSRoots verifies the destination of tls://, rediss:// and valkeys://
	// (--verify-tls-ca); nil = the system roots.
	TLSRoots *x509.CertPool

	tls     bool     // a TLS scheme (in any element of a NATS list)
	secrets []string // users, passwords and tokens of the URL, scrubbed from errors
}

// ClockSlack is how far before the start of the run the destination is read
// (clock skew between the injector and the destination's host). 10 min in
// the four injectors since 2026-11-07 (it was 5 here).
const ClockSlack = 10 * time.Minute

// Poll interval of the wait (SPEC §8.5).
const pollEvery = 250 * time.Millisecond

// Settle is the wait before the last read once every expected seq is found.
var Settle = time.Second

// readSlice bounds one NATS read: a stream with constant foreign traffic on
// the subject never gives an empty batch (SPEC §8.5).
var readSlice = 2 * time.Second

// ErrRunPresent: the destination already holds messages of --run (SPEC
// §8.5); the run is refused before anything is sent (exit 2).
var ErrRunPresent = errors.New("--run already present at the destination; use another one")

var (
	runRe   = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)
	keyRe   = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,255}$`)
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	spaceRe = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)
)

const schemes = "want nats://, tls://, postgres://, postgresql://, redis://, rediss://, valkey:// or valkeys://"

// Parse validates the flags of a check; caFile is --verify-tls-ca. Errors
// never quote the URL: it may carry a password.
func Parse(rawURL, sqlitePath, space, key, run, caFile string) (*Target, error) {
	t, err := parse(rawURL, sqlitePath, space, key, run)
	if err != nil || caFile == "" {
		return t, err
	}
	if !t.tls {
		return nil, errors.New("--verify-tls-ca needs a TLS destination: tls://, rediss:// or valkeys:// (PostgreSQL takes sslrootcert= in the URL)")
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("--verify-tls-ca: the file cannot be read")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("--verify-tls-ca: no PEM certificate in the file")
	}
	t.TLSRoots = pool
	return t, nil
}

func parse(rawURL, sqlitePath, space, key, run string) (*Target, error) {
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
		return nil, errors.New("--verify-url: not a URL (" + schemes + ")")
	}
	switch strings.ToLower(scheme) {
	case "nats", "tls":
		t.Kind = "nats"
		// A list, as the server's sink.nats.urls. Every element is parsed
		// here, before nats.go sees it: its parse error quoted the element,
		// password included (SPEC §8.5).
		for _, el := range strings.Split(rawURL, ",") {
			u, err := url.Parse(strings.TrimSpace(el))
			if err != nil || u.Host == "" {
				return nil, errors.New("--verify-url: an element of the NATS list is not a URL (encode %, /, #, ? and @ in the password)")
			}
			switch strings.ToLower(u.Scheme) {
			case "nats":
			case "tls":
				t.tls = true
			default:
				return nil, errors.New("--verify-url: every element of a NATS list is nats:// or tls://")
			}
			t.addSecrets(u)
		}
	case "redis", "rediss":
		t.Kind, t.tls = "redis", strings.EqualFold(scheme, "rediss")
	case "valkey":
		t.Kind, rawURL = "redis", "redis://"+rest
	case "valkeys":
		t.Kind, rawURL, t.tls = "redis", "rediss://"+rest, true
	case "postgres", "postgresql":
		t.Kind = "postgres"
		if key != "" {
			parts := strings.Split(key, ".")
			if len(parts) > 2 || !identRe.MatchString(parts[0]) || !identRe.MatchString(parts[len(parts)-1]) {
				return nil, errors.New("--verify-key: a PostgreSQL table is [schema.]table, letters, digits and _")
			}
		}
	default:
		return nil, errors.New("--verify-url: unsupported scheme (" + schemes + ")")
	}
	if t.Kind != "nats" {
		// One URL. A PostgreSQL multi-host (h1,h2) does not parse here and
		// is left to pgx, whose parse error is never forwarded.
		if u, err := url.Parse(rawURL); err == nil {
			t.addSecrets(u)
		}
	}
	t.URL = rawURL
	return t, nil
}

func (t *Target) addSecrets(u *url.URL) {
	if u.User == nil {
		return
	}
	if p, ok := u.User.Password(); ok && p != "" {
		t.secrets = append(t.secrets, p)
	}
	if n := u.User.Username(); n != "" {
		t.secrets = append(t.secrets, n) // a NATS token goes as the user
	}
}

// clean is the error of a client library as the report may show it: a
// *url.Error is never forwarded (its text is the URL), and every user,
// password and token of the URL is scrubbed from the rest (SPEC §8.5).
func (t *Target) clean(prefix string, err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return errors.New(prefix + ": the URL does not parse (encode %, /, #, ? and @ in the password)")
	}
	s := err.Error()
	for _, sec := range t.secrets {
		for _, v := range []string{sec, url.QueryEscape(sec), url.PathEscape(sec)} {
			s = strings.ReplaceAll(s, v, "xxxxx")
		}
	}
	return errors.New(prefix + ": " + s)
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
		if _, after, ok := strings.Cut(part, "://"); ok {
			part = after // the next element of a NATS list, with its own scheme
		}
		u, err := url.Parse(scheme + "://" + part)
		if err != nil || u.Host == "" {
			hosts = append(hosts, "?")
			continue
		}
		hosts = append(hosts, u.Host)
	}
	return scheme + "://" + strings.Join(hosts, ",")
}

// Counter counts the copies of each seq of the run found in the destination:
// a vector indexed by seq, up to the largest expected one (SPEC §8.5; a map
// rebuilt at every full read until 2026-11-07).
type Counter struct {
	copies  []uint16 // index seq; saturating
	beyond  uint64   // records whose seq is past the vector
	records uint64   // every record of the run
}

// NewCounter returns an empty counter for seqs 1..max.
func NewCounter(max uint64) *Counter { return &Counter{copies: make([]uint16, max+1)} }

// fresh is an empty counter of the same size.
func (c *Counter) fresh() *Counter { return NewCounter(uint64(len(c.copies) - 1)) }

// Add counts one copy of seq.
func (c *Counter) Add(seq uint64) {
	c.records++
	if seq >= uint64(len(c.copies)) {
		c.beyond++
		return
	}
	if c.copies[seq] < 0xFFFF {
		c.copies[seq]++
	}
}

func (c *Counter) n(seq uint64) uint16 {
	if seq >= uint64(len(c.copies)) {
		return 0
	}
	return c.copies[seq]
}

// reader is one kind of destination.
type reader interface {
	// poll counts in c the messages of the run: what is new since the last
	// poll, or, for a full reader, everything (c is then a fresh counter).
	poll(ctx context.Context, c *Counter) error
	// full: every poll rereads the whole window (PostgreSQL, SQLite).
	full() bool
	close()
}

// Check is an open destination.
type Check struct {
	t *Target
	r reader
}

// Open connects and checks the stream or table exists (SPEC §8.5), before
// anything is sent: an unreachable destination fails the run at once. A
// destination read by time that already holds the run is ErrRunPresent.
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
	if r.full() {
		// The window read by time would find the rows of an earlier run
		// with the same --run and pass while this one loses everything
		// (SPEC §8.5): refuse now, before the first send.
		c := NewCounter(0)
		if err := r.poll(ctx, c); err != nil {
			r.close()
			return nil, err
		}
		if c.records > 0 {
			r.close()
			return nil, ErrRunPresent
		}
	}
	return &Check{t: t, r: r}, nil
}

// Close releases the connection.
func (c *Check) Close() { c.r.close() }

// Wait reads the destination until every expected seq is found or timeout
// passes, and returns the counts. A read error is retried until the
// deadline; when the last read failed the counts are not the destination's,
// and v.Error says why (SPEC §8.5).
func (c *Check) Wait(ctx context.Context, expected *report.SeqSet, timeout time.Duration) *report.Verify {
	v := &report.Verify{Destination: c.t.Destination(), Expected: expected.Len()}
	cnt := NewCounter(expected.Max())
	start := time.Now()
	deadline := start.Add(timeout)
	var err error
	read := func() {
		into := cnt
		if c.r.full() {
			// A full read cut short is not a count: keep the last whole one.
			into = cnt.fresh()
		}
		pctx, cancel := context.WithDeadline(ctx, deadline.Add(5*time.Second))
		err = c.r.poll(pctx, into)
		cancel()
		if err == nil {
			cnt = into
		}
	}
	for {
		read()
		if tally(cnt, expected, v); err == nil && v.Found == v.Expected {
			v.Complete = true
			// One more read a moment later: a copy sent with the burst
			// (the same seq twice) lands right after the first one.
			if settle := min(Settle, time.Until(deadline)); settle > 0 && ctx.Err() == nil {
				select {
				case <-time.After(settle):
				case <-ctx.Done():
				}
				if ctx.Err() == nil {
					read()
				}
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
	tally(cnt, expected, v)
	v.WaitedSec = time.Since(start).Seconds()
	if err != nil {
		v.Error = err.Error()
	}
	return v
}

// tally fills the counts of v from what was found (SPEC §8.5): duplicated
// is the copies past the first of every seq, unexpected the records whose
// seq is not a success.
func tally(c *Counter, expected *report.SeqSet, v *report.Verify) {
	v.Found, v.Duplicated, v.Unexpected = 0, 0, c.beyond
	for seq := uint64(1); seq < uint64(len(c.copies)); seq++ {
		n := uint64(c.copies[seq])
		if n == 0 {
			continue
		}
		v.Duplicated += n - 1
		if expected.Has(seq) {
			v.Found++
		} else {
			v.Unexpected += n
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
		if !expected.Has(seq) || c.n(seq) > 0 {
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
