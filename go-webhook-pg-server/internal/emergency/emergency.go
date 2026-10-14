// Package emergency writes and reingests the emergency dump of SPEC §9.
//
// The dump is a property of the RING, not of the sink. It fires whenever the
// process is leaving with events it accepted and could not deliver: a fatal
// sink failure, or a shutdown drain that ran out of time.
package emergency

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
)

// File format, per SPEC §9.1: JSON Lines.
//
//	<JLH>\n
//	<JLH>\n
//	... repeated
//
// Each line is exactly the bytes the client saw confirmed. The JLH is already a
// one-line JSON document -- the serializer writes no whitespace between tokens
// and escapes every control byte inside a string -- so the newline is the whole
// framing: no length to trust, and a damaged line cannot take the reader out of
// step with the next one.
const (
	filePrefix = "emg-"
	fileSuffix = ".jsonl"
	corruptExt = ".corrupt"
	// tmpSuffix marks a dump still being written, under a hidden name the
	// listing never matches (SPEC §9.1).
	tmpSuffix = ".tmp"
	// foreignSuffix is the binary dump of go-webhook-fast-server: recognised
	// only to be reported, never read (SPEC §9.2).
	foreignSuffix = ".bin"
)

// Store reads and writes dumps in one directory.
type Store struct {
	dir   string
	agent string // the AgentUUID: whose dump a file is (SPEC §9.1)
	// maxLine is the longest line Read accepts: the largest envelope the
	// current configuration can produce (SPEC §3.2, §9.2). Without a ceiling,
	// a corrupt file with no newline would be one allocation the size of the
	// file.
	maxLine int64
	abandon func() bool
}

// NewStore returns a store for dir. maxLine is the ceiling of one line on
// read; zero or less means no ceiling, which only the tests use.
func NewStore(dir, agentUUID string, maxLine int64) *Store {
	return &Store{dir: dir, agent: agentUUID, maxLine: maxLine}
}

// SetAbandon installs the check a Write in progress consults: true means the
// process was aborted and counted the dump's events lost, so the dump gives up
// and removes its temporary file instead of publishing a file the next boot
// would reingest -- counted lost AND delivered (SPEC §10.3). Checked before
// the first line, every 1024 lines and before the publish; a dump that
// publishes between the last check and the abort is both counted and kept, an
// overcount, never a loss (port of the syslog/RELP family, 2026-10-13).
func (s *Store) SetAbandon(f func() bool) { s.abandon = f }

func (s *Store) abandoned() bool { return s.abandon != nil && s.abandon() }

// ErrAbandoned is Write giving up because the process was aborted.
var ErrAbandoned = errors.New("emergency: dump abandoned: the process was aborted")

// Dir reports the configured directory.
func (s *Store) Dir() string { return s.dir }

// Write dumps items to a new file and returns its path and how many items were
// left out because they could not be framed as one line.
//
// Durability here is the whole point, so the file is fsynced AND the directory
// entry is fsynced. Without the second one, a crash right after this call can
// leave a file whose name never made it to disk -- the data would be written
// and unreachable.
//
// The file is written under a hidden temporary name and linked to the final
// one only once complete and synced (SPEC §9.1). Written under the final name,
// a process killed mid-write left a file that, cut at a line boundary, read
// clean -- and was removed after being delivered in part (the review of
// go-syslog-nats-server, ported from go-webhook-nats-server).
//
// An envelope with a raw 0x0A would split its line in two and spoil the next
// one. The serializer never produces it (the fuzz targets check), but the dump
// is the last line of defence and checks again: such an item is left OUT and
// reported, and the rest is written. Refusing the whole dump would lose every
// event to protect one (SPEC §9.1).
func (s *Store) Write(items []*envelope.Item) (path string, skipped int, err error) {
	ok := make([]*envelope.Item, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		if bytes.IndexByte(it.Env, '\n') >= 0 {
			skipped++
			continue
		}
		ok = append(ok, it)
	}
	if len(ok) == 0 {
		return "", skipped, nil
	}
	if s.abandoned() {
		return "", skipped, ErrAbandoned
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return "", skipped, fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}

	name := fmt.Sprintf("%s%s-%d%s", filePrefix, sanitize(s.agent), time.Now().UnixNano(), fileSuffix)
	path = filepath.Join(s.dir, name)
	tmp := filepath.Join(s.dir, "."+name+tmpSuffix)

	// O_EXCL: never append to or truncate an existing dump, which would be
	// another instance's pending data.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return "", skipped, fmt.Errorf("emergency: cannot create %s: %w", tmp, err)
	}

	if err := encodeUntil(f, ok, s.abandoned); err != nil {
		f.Close()
		return "", skipped, s.abort(tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", skipped, s.abort(tmp, err)
	}
	if err := f.Close(); err != nil {
		return "", skipped, s.abort(tmp, err)
	}
	if s.abandoned() {
		return "", skipped, s.abort(tmp, ErrAbandoned)
	}
	if err := publish(tmp, path); err != nil {
		return "", skipped, s.abort(tmp, err)
	}
	// Best effort here, unlike Remove and Quarantine: the dump is complete,
	// synced and under its final name, and discarding it for the directory
	// fsync would lose it (SPEC §9.1).
	_ = syncDir(s.dir)
	return path, skipped, nil
}

// link is os.Link, replaceable by the tests: a file system without hard links
// is not something a test can count on having.
var link = os.Link

// publish gives the complete temporary file its final name, never over an
// existing dump: the stamp makes a collision unlikely, and Link refuses one
// where Rename would silently replace it.
//
// A file system without hard links -- vfat, exFAT, the object-store FUSE
// mounts -- refuses Link whatever the names, and every dump would fail there,
// the whole ring lost, where writing in place had worked. There the name is
// checked free and the file renamed: the nanosecond stamp in the name is
// what keeps the window between the two harmless.
func publish(tmp, path string) error {
	err := link(tmp, path)
	if err == nil {
		_ = os.Remove(tmp)
		return nil
	}
	if errors.Is(err, os.ErrExist) {
		return err
	}
	if _, serr := os.Lstat(path); !errors.Is(serr, os.ErrNotExist) {
		return fmt.Errorf("%w (and %s exists)", err, filepath.Base(path))
	}
	if rerr := os.Rename(tmp, path); rerr != nil {
		return fmt.Errorf("%w (and the rename that replaces it on a file system without "+
			"hard links failed: %v)", err, rerr)
	}
	return nil
}

// encode writes items as JSON Lines: each envelope followed by a newline. The
// caller has already left out any envelope with a raw newline.
func encode(dst io.Writer, items []*envelope.Item) error { return encodeUntil(dst, items, nil) }

// encodeUntil is encode that gives up with ErrAbandoned when stop says so,
// asked every 1024 lines (Store.SetAbandon); a nil stop never stops.
func encodeUntil(dst io.Writer, items []*envelope.Item, stop func() bool) error {
	w := bufio.NewWriterSize(dst, 1<<20)
	for i, it := range items {
		if i&1023 == 1023 && stop != nil && stop() {
			return ErrAbandoned
		}
		if _, err := w.Write(it.Env); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
	}
	return w.Flush()
}

// abort removes a partially written dump: a truncated file that nobody knows is
// truncated is worse than no file, because reingestion would silently deliver
// half a batch and delete the rest. path is the temporary name.
func (s *Store) abort(path string, cause error) error {
	_ = os.Remove(path)
	return fmt.Errorf("emergency: writing %s: %w", path, cause)
}

// List returns the dump files in chronological order. The name carries a
// nanosecond timestamp, so ordering by it is chronological order whatever the
// agent uuid of the instance that wrote the file.
func (s *Store) List() ([]string, error) { return s.list(fileSuffix) }

// Foreign returns the binary dumps of go-webhook-fast-server found in the
// directory. They are never read here (SPEC §9.2); the boot only reports them,
// so an operator who moved an instance over learns there is something to
// reingest with the other program.
func (s *Store) Foreign() ([]string, error) { return s.list(foreignSuffix) }

func (s *Store) list(suffix string) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		// Regular files only: a FIFO named like a dump blocked os.Open
		// forever, the boot hung and ignored SIGTERM; a symlink to /dev/zero
		// was read for ever. Neither is a dump this server wrote.
		if !e.Type().IsRegular() || !strings.HasPrefix(n, filePrefix) || !strings.HasSuffix(n, suffix) {
			continue
		}
		out = append(out, filepath.Join(s.dir, n))
	}
	sort.Slice(out, func(i, j int) bool { return tsOf(out[i], suffix) < tsOf(out[j], suffix) })
	return out, nil
}

// Leftovers deals with the temporary files a dump leaves when it is cut short
// (SPEC §9.1). A temporary that is the same file as the dump under its final
// name is a twin -- the process died between the link and the removal -- and
// is removed: the dump itself is what reingestion reads. Any other is an
// interrupted dump -- its events counted lost only if a second signal aborted
// it; after a SIGKILL or a crash nobody counted them -- and it is returned,
// for the boot to report, and never read or removed here.
func (s *Store) Leftovers() (interrupted []string, err error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	for _, e := range entries {
		n := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(n, "."+filePrefix) ||
			!strings.HasSuffix(n, fileSuffix+tmpSuffix) {
			continue
		}
		tmp := filepath.Join(s.dir, n)
		final := filepath.Join(s.dir, strings.TrimSuffix(n[1:], tmpSuffix))
		ti, terr := os.Lstat(tmp)
		fi, ferr := os.Lstat(final)
		if terr == nil && ferr == nil && os.SameFile(ti, fi) {
			_ = os.Remove(tmp)
			continue
		}
		interrupted = append(interrupted, tmp)
	}
	return interrupted, nil
}

// tsOf extracts the nanosecond stamp from a dump filename.
func tsOf(path, suffix string) int64 {
	base := strings.TrimSuffix(filepath.Base(path), suffix)
	i := strings.LastIndexByte(base, '-')
	if i < 0 {
		return 0
	}
	n, _ := strconv.ParseInt(base[i+1:], 10, 64)
	return n
}

// ReadResult reports what came out of one file.
type ReadResult struct {
	Items []*envelope.Item
	// DamagedLines counts the lines skipped as damage (SPEC §9.2): a line
	// that is not an envelope this server writes, a last line with no
	// newline, a line above the ceiling. The intact lines around them are
	// still returned: partial recovery beats none.
	DamagedLines int
	// DamagedBytes is how many bytes those lines held, newlines included.
	DamagedBytes int64
	// EmptyLines counts the lone newlines skipped as nothing (SPEC §9.2). With
	// it every byte of a file is accounted for: an item and its newline, a
	// damaged line, or an empty one.
	EmptyLines int
}

// Damaged reports whether any line was skipped.
func (r *ReadResult) Damaged() bool { return r.DamagedLines > 0 }

// Read parses one dump file.
//
// This decoder reads from disk after a crash, which makes it the second place
// in this server that parses untrusted structured input. A damaged line is
// skipped and the next one read: the newline is the framing, so damage never
// spreads past the line it is on.
func (s *Store) Read(path string) (*ReadResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readFrom(f, s.maxLine)
}

// readFrom is Read over any reader; the fuzz target feeds it from memory, so it
// is not throttled by the file system.
func readFrom(src io.Reader, maxLine int64) (*ReadResult, error) {
	res := &ReadResult{}
	r := bufio.NewReaderSize(src, 1<<16)
	for {
		line, n, complete, err := readLine(r, maxLine)
		if err != nil {
			return res, err
		}
		if n == 0 {
			return res, nil // clean end
		}
		if complete && n == 1 {
			res.EmptyLines++ // nothing written, nothing lost
			continue
		}
		if !complete || line == nil {
			// Cut short by the end of the file, or longer than any envelope
			// this configuration can produce.
			res.DamagedLines++
			res.DamagedBytes += n
			continue
		}
		env := line[:len(line)-1]
		h, err := checkLine(env)
		if err != nil {
			res.DamagedLines++
			res.DamagedBytes += n
			continue
		}
		// Buffers here come from make, not the pool: they outlive any pool
		// class and reingestion is a boot-time path where allocation is free.
		res.Items = append(res.Items, envelope.NewItem(h.UUID.String(), env, -1))
	}
}

// errNotOurs is a line that is not an envelope this server could have written.
var errNotOurs = errors.New("emergency: not an envelope of this server")

// checkLine holds one line to "an envelope this server writes" (SPEC §9.2),
// which is stricter than "a JSON object with the right header":
//
//   - ParseHead checks the header and only scans the payload's braces, so
//     json.Valid reads the whole of it: "{]" or "{,,,}" is not a document.
//   - json.Valid checks neither UTF-8 nor escapes. A raw invalid byte or an
//     escaped lone surrogate ("\ud800") is valid to it and refused by jsonb
//     (22021, 22P02) -- class 22, logic -- and one such line, from bit rot or a
//     hand edit, made every boot exit 3 before the public port opened
//     (independent review, 2026-10-07). The serializer writes UTF-8 only and
//     only two kinds of \u escape: \u00XX for control bytes and \u2028/\u2029
//     (SPEC §12.1); anything else did not come from it -- and neither does a
//     raw U+2028 or U+2029, which it always escapes.
func checkLine(env []byte) (envelope.Head, error) {
	h, _, err := envelope.ParseHead(env)
	if err != nil {
		return h, err
	}
	if !utf8.Valid(env) || !json.Valid(env) || !onlyOurEscapes(env) ||
		bytes.Contains(env, rawLS) || bytes.Contains(env, rawPS) {
		return h, errNotOurs
	}
	return h, nil
}

// rawLS and rawPS are U+2028 and U+2029 in UTF-8. The serializer always
// escapes them (SPEC §12.1), so a raw one did not come from it (SPEC §9.2;
// port of go-webhook-server, W 20). Here nothing compares keys by bytes and
// jsonb normalises, so it was only slack, but the gate is the family's.
var rawLS, rawPS = []byte("\xe2\x80\xa8"), []byte("\xe2\x80\xa9")

// onlyOurEscapes reports whether every escape in env is one the serializer
// writes (envelope.appendEscape and the U+2028/U+2029 case, SPEC §9.2): \"
// \\ \n \r \t, \u00XX in lowercase hex for a control byte with no short
// form, and \u2028/\u2029 -- nothing else. \/ \b \f, the \u of a printable
// character and uppercase hex are valid JSON this server never writes, so a
// record carrying one is not ours (W 34; until 2026-10-14 any
// two-character escape and any \u00XX passed, against §9.2). A backslash
// always starts an escape in valid JSON, so the pairs are walked in order and
// "\\u0000" (an escaped backslash, then text) is not mistaken for one. It is
// go-webhook-nats-server's check (emergency/shape.go).
func onlyOurEscapes(env []byte) bool {
	for i := 0; i < len(env); i++ {
		if env[i] != '\\' {
			continue
		}
		if i+1 >= len(env) {
			return false
		}
		switch env[i+1] {
		case '"', '\\', 'n', 'r', 't':
			i++
			continue
		case 'u':
		default:
			return false // \/ \b \f: valid JSON, never written
		}
		if i+6 > len(env) || !ourUnicodeEscape(env[i+2:i+6]) {
			return false
		}
		i += 5
	}
	return true
}

// ourUnicodeEscape is the four hex digits of a \u escape the serializer writes:
// lowercase hex, a control byte without a short form, or a JavaScript line
// terminator.
func ourUnicodeEscape(q []byte) bool {
	if string(q) == "2028" || string(q) == "2029" {
		return true
	}
	if q[0] != '0' || q[1] != '0' || (q[2] != '0' && q[2] != '1') {
		return false
	}
	lo := lowerHex(q[3])
	if lo < 0 {
		return false
	}
	v := int(q[2]-'0')<<4 | lo
	return v != '\n' && v != '\r' && v != '\t'
}

// lowerHex is the value of a lowercase hex digit, or -1: the serializer's
// hexDigits are lowercase, so 'A'-'F' is not ours.
func lowerHex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	}
	return -1
}

// readLine reads one line of at most max bytes before its newline.
//
// It returns the line with its newline in a fresh slice, the number of bytes
// consumed, and whether the line ended in a newline. A line longer than max is
// consumed to its end without being kept: line is nil and n says how much was
// skipped. n == 0 means the file ended cleanly. A nil line with complete set is
// only ever the over-long case.
func readLine(r *bufio.Reader, max int64) (line []byte, n int64, complete bool, err error) {
	var buf []byte
	over := false
	for {
		frag, ferr := r.ReadSlice('\n')
		n += int64(len(frag))
		if !over {
			if max > 0 && int64(len(buf)+len(frag)) > max+1 {
				over, buf = true, nil
			} else {
				buf = append(buf, frag...)
			}
		}
		switch {
		case ferr == nil:
			if over {
				return nil, n, true, nil
			}
			return buf, n, true, nil
		case errors.Is(ferr, bufio.ErrBufferFull):
			continue
		case errors.Is(ferr, io.EOF):
			if over {
				return nil, n, true, nil
			}
			return buf, n, false, nil
		default:
			return nil, n, false, ferr
		}
	}
}

// Remove deletes a fully reingested dump.
//
// The directory is fsynced after the unlink. Without it a power loss right
// after the reingestion can bring the name back, and the next boot delivers
// the whole dump a second time -- absorbed by ON CONFLICT (uuid) only while
// the queue still holds those uuids, a duplicate once a consumer took them. An
// error wrapping ErrNotDurable means the file WAS removed and only that
// durability is in doubt; the caller logs it at WARN (SPEC §9.2; port of the
// syslog/RELP family, 2026-10-13).
func (s *Store) Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Quarantine renames a damaged file aside instead of deleting it (SPEC §9.2).
// Its intact lines have already been reingested; the damaged ones are
// unrecoverable by this program but may still be worth a human's attention,
// and deleting evidence of a crash is not this program's call. Reingesting the
// quarantined file again is safe: the lines already delivered are duplicates
// the queue's ON CONFLICT (uuid) absorbs.
//
// A non-empty name means the file WAS quarantined, whatever the error says:
// the rename happened, and the error, if any, is the directory fsync that
// followed (errors.Is(err, ErrNotDurable)), which only means the new name may
// not survive a power loss. The caller logs that at WARN and still reports the
// quarantine under the new name. An empty name means the file is still where
// it was.
func (s *Store) Quarantine(path string) (string, error) {
	dst := path + corruptExt
	if err := os.Rename(path, dst); err != nil {
		return "", err
	}
	return dst, syncDir(filepath.Dir(dst))
}

// Count reports how many files are pending and how many events they hold.
//
// Every listed file counts, readable or not: one the boot could not read stays
// in place and is still awaiting reingestion, and leaving it out hid it from
// whs_emergency_files (SPEC §11.1; §19.1 item 12, closed 2026-10-13 in the
// port of the syslog/RELP family, the rule of go-webhook-fast-server). Its
// events are unknown and not counted.
//
// It decodes every file in full -- the format has no header-only mode -- which
// is acceptable for a directory that is empty except during an incident: the
// boot calls it once, after the reingestion has removed what it delivered. The
// dump adds to the gauges rather than calling it.
func (s *Store) Count() (files int, events int64, err error) {
	paths, err := s.List()
	if err != nil {
		return 0, 0, err
	}
	for _, p := range paths {
		files++
		res, err := s.Read(p)
		if err != nil {
			continue
		}
		events += int64(len(res.Items))
	}
	return files, events, nil
}

// syncDir fsyncs a directory so a change to its entries is durable. Some file
// systems refuse fsync on a directory, and a directory writable but not
// readable (mode 0300) cannot even be opened for it: both are ErrNotDurable,
// the open included. Write ignores it -- no reason to discard a dump that is
// complete, synced and under its final name, which Write did on a failed open
// -- and Remove and Quarantine report it, AFTER the unlink or the rename
// succeeded: "cannot remove" for a file that is gone would be false (port of
// the syslog/RELP family, 2026-10-13).
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: cannot open directory %s to fsync it: %w", ErrNotDurable, dir, err)
	}
	defer d.Close()
	if err := dirSync(d); err != nil {
		return fmt.Errorf("%w: fsync of directory %s: %w", ErrNotDurable, dir, err)
	}
	return nil
}

// ErrNotDurable marks the failure of a directory fsync after the operation on
// the directory entry itself succeeded: the unlink of Remove or the rename of
// Quarantine happened, and only its durability is in doubt.
var ErrNotDurable = errors.New("emergency: the directory entry may not be durable")

// dirSync is (*os.File).Sync, behind a variable only so the tests can make a
// directory fsync fail: no portable file system does that on demand.
var dirSync = func(d *os.File) error { return d.Sync() }

// sanitize keeps an agent uuid usable inside a filename.
func sanitize(s string) string {
	if s == "" {
		return "agent"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && i < 64; i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
