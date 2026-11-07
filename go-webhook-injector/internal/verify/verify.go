// Package verify checks the delivery at the destination (SPEC §8.6): after
// the run it reads what the server wrote -- a NATS JetStream stream, the
// PostgreSQL queue loghub.inbox, a Redis/Valkey stream or the SQLite inbox --
// and counts the accepted requests that are missing and the copies that are
// repeated, by the uuid of every 2xx and by the seq/run markers of the body.
package verify

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/report"
)

// Config selects the destination.
type Config struct {
	URL    string // nats://, tls://, postgres://, postgresql://, redis://, rediss://, valkey://, valkeys://
	SQLite string // a SQLite file instead of URL
	Stream string // NATS stream or Redis stream key; "" = automatic
	Table  string // PostgreSQL or SQLite table; "" = the default
	// Timeout bounds the connection check at Open and, in Check, the wait
	// for the accepted requests to show up.
	Timeout time.Duration
	// Since is the start of the PostgreSQL and SQLite window: records
	// older are not read. NATS and Redis start after the stream's last
	// entry at Open instead (SPEC §8.6.3).
	Since time.Time
}

// readBound bounds one read of the destination.
const readBound = time.Minute

// finalRead is the pause before the read after the complete one.
const finalRead = time.Second

// Margin is how far before the start of the run the PostgreSQL and SQLite
// window opens (clock skew between the injector and the server, SPEC
// §8.6.3).
const Margin = time.Minute

// Kind of a destination, from the URL scheme.
func Kind(rawURL string) (string, error) {
	scheme, _, ok := strings.Cut(rawURL, "://")
	if !ok {
		return "", errors.New("--verify-url: want scheme://...")
	}
	switch scheme {
	case "nats", "tls":
		return "nats", nil
	case "postgres", "postgresql":
		return "postgres", nil
	case "redis", "rediss", "valkey", "valkeys":
		return "redis", nil
	}
	// The scheme is not a secret; the rest of the URL may be.
	return "", fmt.Errorf("--verify-url: scheme %q is none of nats, tls, postgres, postgresql, redis, rediss, valkey, valkeys", scheme)
}

var identRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$`)

// ValidTable says whether a table name is a plain lowercase identifier,
// optionally schema-qualified.
func ValidTable(t string) bool { return identRE.MatchString(t) }

// record is one entry of the destination.
type record struct {
	key      string // unique per stored entry: re-reads of it are ignored
	uuid     string
	encoding string
	body     string
	// jlh, when set, is a whole envelope not parsed yet (NATS, Redis): the
	// verifier parses only the ones that may be of the run.
	jlh []byte
}

// reader reads the window of a destination. read delivers every record not
// delivered before, or all of them again (the verifier ignores a key it saw).
type reader interface {
	read(ctx context.Context, fn func(record)) error
	close()
}

// Verifier collects the accepted requests during the run and checks them
// at the destination afterwards.
type Verifier struct {
	r    reader
	dest string
	run  string

	mu       sync.Mutex
	accepted map[string]uint64 // uuid -> seq of a 2xx
	noUUID   map[uint64]bool   // seq of a 2xx without X-LogHub-UUID
	okSeq    map[uint64]bool   // seq of every 2xx

	seen      map[string]bool
	uuidFound map[string]bool
	groups    map[string]int // run records by identity (seq, or uuid without a marker)
	seqFound  map[uint64]bool
	records   uint64
	markers   bool // a record of the run carried the seq/run markers
}

// Open connects to the destination and checks that it can be read, before
// anything is sent (SPEC §8.6.1). Errors never echo the URL.
func Open(ctx context.Context, cfg Config, run string) (*Verifier, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	v := &Verifier{run: run, accepted: map[string]uint64{}, noUUID: map[uint64]bool{}, okSeq: map[uint64]bool{},
		seen: map[string]bool{}, uuidFound: map[string]bool{}, groups: map[string]int{}, seqFound: map[uint64]bool{}}
	since := cfg.Since.Add(-Margin)
	var err error
	if cfg.SQLite != "" {
		if cfg.Table == "" {
			cfg.Table = "inbox"
		}
		v.dest = "sqlite " + cfg.SQLite + " table " + cfg.Table
		v.r, err = openSQLite(ctx, cfg.SQLite, cfg.Table, since)
		return v, err
	}
	kind, err := Kind(cfg.URL)
	if err != nil {
		return nil, err
	}
	v.dest = describe(cfg.URL)
	switch kind {
	case "nats":
		var r *natsReader
		r, err = openNATS(ctx, cfg.URL, cfg.Stream, cfg.Timeout)
		if err == nil {
			v.dest += " stream " + r.stream
			v.r = r
		}
	case "postgres":
		if cfg.Table == "" {
			cfg.Table = "loghub.inbox"
		}
		v.dest += " table " + cfg.Table
		v.r, err = openPG(ctx, cfg.URL, cfg.Table, since)
	case "redis":
		if cfg.Stream != "" {
			v.dest += " stream " + cfg.Stream
		} else {
			v.dest += " streams " + redisPattern
		}
		v.r, err = openRedis(ctx, cfg.URL, cfg.Stream)
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// describe is the destination without credentials, path or query: what the
// report may print.
func describe(raw string) string {
	first, _, _ := strings.Cut(raw, ",")
	u, err := url.Parse(first)
	if err != nil || u.Host == "" {
		scheme, _, _ := strings.Cut(raw, "://")
		return scheme + "://?"
	}
	return u.Scheme + "://" + u.Host
}

// Accept records a final response; only 2xx are expected at the destination.
func (v *Verifier) Accept(seq uint64, status int, uuid string) {
	if status < 200 || status > 299 {
		return
	}
	v.mu.Lock()
	v.okSeq[seq] = true
	if uuid == "" {
		v.noUUID[seq] = true
	} else {
		v.accepted[uuid] = seq
	}
	v.mu.Unlock()
}

// Result is the verification in the report (SPEC §8.6.4).
type Result = report.Verify

// Check reads the destination until every accepted request is found or the
// timeout passes, and counts.
func (v *Verifier) Check(ctx context.Context, timeout time.Duration) Result {
	start := time.Now()
	deadline := start.Add(timeout)
	wait := 250 * time.Millisecond
	var rerr error
	complete := false
	for {
		// A read of a large window may take a while: it has its own bound,
		// not what is left of the wait, so a slow last read still
		// completes, and a destination that hangs still ends the check.
		rctx, cancel := context.WithTimeout(ctx, max(timeout, readBound))
		rerr = v.r.read(rctx, v.add)
		cancel()
		if rerr != nil {
			break
		}
		if res := v.result(); res.Lost == 0 && res.Unverifiable == 0 {
			complete = true
			// One more read a moment later: a copy written after the last
			// accepted one (a duplicate of the run) would be missed by
			// stopping at the first complete read (found by SI against
			// PostgreSQL).
			t := time.NewTimer(finalRead)
			select {
			case <-ctx.Done():
				t.Stop()
				rerr = ctx.Err()
			case <-t.C:
				rctx, cancel := context.WithTimeout(ctx, max(timeout, readBound))
				rerr = v.r.read(rctx, v.add)
				cancel()
			}
			break
		}
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		t := time.NewTimer(min(wait, left))
		select {
		case <-ctx.Done():
			t.Stop()
			rerr = ctx.Err()
		case <-t.C:
		}
		if rerr != nil {
			break
		}
		wait = min(2*wait, 2*time.Second)
	}
	res := v.result()
	res.Waited = time.Since(start).Seconds()
	res.Complete = complete && rerr == nil
	if rerr != nil {
		res.Error = rerr.Error()
	}
	return res
}

// Close releases the destination.
func (v *Verifier) Close() { v.r.close() }

func (v *Verifier) add(r record) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.seen[r.key] {
		return
	}
	v.seen[r.key] = true
	if r.jlh != nil {
		// The serializer writes "uuid" first (W SPEC §3.1): a record that
		// is neither accepted nor carries the run is skipped unparsed.
		const pre = `{"uuid":"`
		if len(r.jlh) >= len(pre)+36 && string(r.jlh[:len(pre)]) == pre {
			if _, ok := v.accepted[string(r.jlh[len(pre):len(pre)+36])]; !ok && !bytes.Contains(r.jlh, []byte(v.run)) {
				return
			}
		}
		r = fromJLH(r.key, r.jlh)
	}
	_, accepted := v.accepted[r.uuid]
	seq, marked := marker(r.encoding, r.body, v.run)
	if !accepted && !marked {
		// Another run, another client. Check runs after the run ended, so
		// the accepted set is complete: this record will never be ours.
		return
	}
	v.records++
	if accepted {
		v.uuidFound[r.uuid] = true
	}
	if marked {
		v.markers = true
		v.seqFound[seq] = true
		v.groups["s"+strconv.FormatUint(seq, 10)]++
	} else {
		v.groups["u"+r.uuid]++
	}
}

func (v *Verifier) result() Result {
	v.mu.Lock()
	defer v.mu.Unlock()
	res := Result{Destination: v.dest, Records: v.records}
	res.Expected = uint64(len(v.accepted) + len(v.noUUID))
	for u := range v.accepted {
		if v.uuidFound[u] {
			res.Found++
		}
	}
	var noUUIDLost uint64
	var lost []uint64
	for u, s := range v.accepted {
		if !v.uuidFound[u] {
			lost = append(lost, s)
		}
	}
	for s := range v.noUUID {
		if v.seqFound[s] {
			res.Found++
		} else {
			noUUIDLost++
			if v.markers {
				lost = append(lost, s)
			}
		}
	}
	res.LostSeqs = ranges(lost, 20)
	res.Lost = res.Expected - res.Found
	if !v.markers {
		// Accepted without a uuid, and no record with markers: nothing can
		// tell whether they arrived.
		res.Unverifiable = noUUIDLost
		res.Lost -= noUUIDLost
	}
	for k, n := range v.groups {
		if n > 1 {
			res.Duplicated += uint64(n - 1)
		}
		if k[0] == 's' {
			if s, err := strconv.ParseUint(k[1:], 10, 64); err == nil && !v.okSeq[s] {
				res.Unexpected++
			}
		}
	}
	return res
}

// ranges writes sorted seqs as at most n ranges ("5", "7-9"); a last
// "..." says there were more.
func ranges(seqs []uint64, n int) []string {
	if len(seqs) == 0 {
		return nil
	}
	slices.Sort(seqs)
	var out []string
	for i := 0; i < len(seqs); {
		j := i
		for j+1 < len(seqs) && seqs[j+1] <= seqs[j]+1 {
			j++
		}
		if len(out) == n {
			return append(out, "...")
		}
		if seqs[j] == seqs[i] {
			out = append(out, strconv.FormatUint(seqs[i], 10))
		} else {
			out = append(out, strconv.FormatUint(seqs[i], 10)+"-"+strconv.FormatUint(seqs[j], 10))
		}
		i = j + 1
	}
	return out
}

// marker reads the seq of a body that carries this run's markers: a JSON
// object with "run" equal to run and an integer "seq" (SPEC §8.6.3).
func marker(encoding, body, run string) (uint64, bool) {
	b := []byte(body)
	if encoding == "base64" {
		d, err := base64.StdEncoding.DecodeString(body)
		if err != nil {
			return 0, false
		}
		b = d
	}
	// Cheap test first: most records of a busy destination are not ours.
	if run == "" || !bytes.Contains(b, []byte(run)) || !bytes.Contains(b, []byte(`"seq"`)) {
		return 0, false
	}
	var m struct {
		Seq json.RawMessage `json:"seq"`
		Run *string         `json:"run"`
	}
	if err := json.Unmarshal(b, &m); err != nil || m.Run == nil || *m.Run != run {
		return 0, false
	}
	// An integer, not "12" in quotes (json.Number would take both).
	s, err := strconv.ParseUint(string(m.Seq), 10, 64)
	if err != nil {
		return 0, false
	}
	return s, true
}

// envelope is the part of a JLH envelope the check reads (W SPEC §3.1).
type envelope struct {
	UUID    string `json:"uuid"`
	Payload struct {
		Content struct {
			Encoding string `json:"encoding"`
			Body     string `json:"body"`
		} `json:"content"`
	} `json:"payload"`
}

func fromJLH(key string, b []byte) record {
	var e envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return record{key: key}
	}
	return record{key: key, uuid: e.UUID, encoding: e.Payload.Content.Encoding, body: e.Payload.Content.Body}
}

// uuidFloor is the smallest UUIDv7 of the millisecond of t: UUIDv7 sorts by
// time, bytewise and as lowercase text.
func uuidFloor(t time.Time) string {
	ms := uint64(max(t.UnixMilli(), 0))
	return fmt.Sprintf("%08x-%04x-0000-0000-000000000000", ms>>16, ms&0xffff)
}
