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

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
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
}

// NewStore returns a store for dir. maxLine is the ceiling of one line on
// read; zero or less means no ceiling, which only the tests use.
func NewStore(dir, agentUUID string, maxLine int64) *Store {
	return &Store{dir: dir, agent: agentUUID, maxLine: maxLine}
}

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
// go-syslog-nats-server; ported to go-webhook-pg-server on 2026-10-11).
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

	if err := encode(f, ok); err != nil {
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
	if err := publish(tmp, path); err != nil {
		return "", skipped, s.abort(tmp, err)
	}
	syncDir(s.dir)
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
// mounts -- refuses Link whatever the names, and every dump failed there,
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
func encode(dst io.Writer, items []*envelope.Item) error {
	w := bufio.NewWriterSize(dst, 1<<20)
	for _, it := range items {
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

// payloadPrefix is how the writer opens every payload of this server (SPEC
// §3.2): mode first, and always fast.
const payloadPrefix = `{"mode":"fast",`

// checkLine holds one line to "an envelope this server writes" (SPEC §9.2),
// which is stricter than "a JSON object with the right header". The line goes
// to NATS WITHOUT conversion, with its subject and headers taken from its own
// header, and NATS never reads Data: a line that passed here without being a
// JLH of this server would be published and confirmed as legitimate. In
// go-webhook-pg-server the queue's CHECKs and jsonb refused part of that; here
// nothing downstream refuses anything (the lesson of go-syslog-nats-server).
//
//   - ParseHead checks the header -- the writer's exact shape, the uuid and
//     from forms, space and type in their grammars (the space becomes subject
//     tokens), the numbers in range -- and only scans the payload's braces.
//   - type, level and step are this server's fixed values (SPEC §3.1), and the
//     payload opens the way the writer opens it: a go-syslog-* dump dropped in
//     the directory by mistake has the same file name and is damage, not
//     events.
//   - json.Valid reads the whole document: "{]" or "{,,,}" is not one, and
//     utf8.Valid the bytes, which json.Valid does not.
//   - ourShape holds the payload to what the writer gives it: its keys in
//     order, no key twice, integers only, only the serializer's escapes,
//     bounded nesting (the independent review of this module, 2026-10-10).
//   - The uuid carries the event's instant, to the nanosecond: the writer
//     takes both from one clock sample (SPEC §3.1, invariant 14).
func checkLine(env []byte) (envelope.Head, error) {
	h, payload, err := envelope.ParseHead(env)
	if err != nil {
		return h, err
	}
	if h.Type != envelope.Type || h.Level != envelope.Level || h.Step != 0 ||
		!bytes.HasPrefix(payload, []byte(payloadPrefix)) {
		return h, errNotOurs
	}
	if !utf8.Valid(env) || !json.Valid(env) || !ourShape(payload) {
		return h, errNotOurs
	}
	if at, ok := h.UUID.TimestampWithLevel(uuidv7.Level3); !ok || !at.Equal(h.Event.Time()) {
		return h, errNotOurs
	}
	return h, nil
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
func (s *Store) Remove(path string) error { return os.Remove(path) }

// Quarantine renames a damaged file aside instead of deleting it (SPEC §9.2).
// Its intact lines have already been reingested; the damaged ones are
// unrecoverable by this program but may still be worth a human's attention,
// and deleting evidence of a crash is not this program's call. Reingesting the
// quarantined file again is safe inside the stream's duplicate_window: the
// lines already delivered carry the same Nats-Msg-Id (SPEC §8.7.3).
func (s *Store) Quarantine(path string) (string, error) {
	dst := path + corruptExt
	if err := os.Rename(path, dst); err != nil {
		return "", err
	}
	return dst, nil
}

// Count reports how many files are pending and how many events they hold.
//
// It decodes every file in full -- the format has no header-only mode -- which
// is acceptable for a directory that is empty except during an incident. The
// tests use it to assert the store's contents; the whs_emergency_* gauges are
// set by the boot reingestion and the dump themselves, not from here.
func (s *Store) Count() (files int, events int64, err error) {
	paths, err := s.List()
	if err != nil {
		return 0, 0, err
	}
	for _, p := range paths {
		res, err := s.Read(p)
		if err != nil {
			continue
		}
		files++
		events += int64(len(res.Items))
	}
	return files, events, nil
}

// syncDir fsyncs a directory so a newly created filename is durable. Best
// effort: some file systems refuse fsync on a directory, and a directory
// writable but not readable (mode 0300) cannot even be opened for it. Neither
// is a reason to discard a dump that is complete, synced and under its final
// name -- which Write did on a failed open, and reported the temporary name.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}

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
