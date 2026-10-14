// Package emergency writes and reingests the emergency dump of SPEC §12.
//
// The dump is a property of the RING, not of the sink. It fires when the
// process is leaving with a non-empty memory ring (or a batch the workers were
// still holding): a FATAL sink, or a shutdown drain that ran out of time. With
// ring=sqlite and ring=redis the items are already outside the process, and
// with ring=none there is never a backlog, so none of them needs it.
package emergency

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// File format, per SPEC §12.1:
//
//	<len_head> <len_raw>\n
//	<head bytes>
//	<raw line bytes>
//	... repeated
//
// The header line is ASCII decimal; the two payloads follow as raw bytes. This
// is deliberately NOT line-delimited JSON: a syslog line is arbitrary bytes,
// and escaping it to fit a text format would cost a full re-encode of every
// pending message at the worst possible moment.
//
// What is dumped is exactly the pair of SPEC §3.4 -- item head plus raw line,
// with the same head encoder as ring.db and the Redis ring -- and nothing else,
// because nothing else is needed: the JLH is a pure function of those two and
// the frozen conversion config (SPEC §3.5, invariant §20.13). A reingested item
// is indistinguishable from the original: same uuid, same clock sample, same
// addresses, so the same envelope byte for byte.
//
// That removes a trap that cost a real bug in the sibling service. Its dump
// stored a FINISHED ENVELOPE, so the queryable columns of the destination had
// to be rebuilt by parsing escaped JSON on the way back in; forgetting it wrote
// rows with an empty path and ts = 0, invisible to the very query those columns
// exist for, with an intact envelope beside them hiding the loss. Here the head
// IS the metadata and comes back whole.
const (
	filePrefix = "emg-"
	fileSuffix = ".bin"
	corruptExt = ".corrupt"

	// tmpSuffix, with a leading dot on the name, marks a dump still being
	// written: .emg-<agent>-<unixnano>.bin.tmp, which List never returns
	// (SPEC §12.1).
	tmpSuffix = ".tmp"

	// maxLenField is a sanity ceiling on one declared length. The real check
	// is against what is left of the file; this one only keeps the arithmetic
	// far from overflow.
	maxLenField = 1 << 31

	// maxHeaderLine bounds the "<len_head> <len_raw>\n" line: two ten-digit
	// numbers, a space and the newline. A corrupt file with no newline for a
	// gigabyte must not be read into one string to find out.
	maxHeaderLine = 32
)

// Store reads and writes dumps in one directory.
type Store struct {
	dir     string
	agent   string
	abandon func() bool
}

// NewStore builds a store for emergency.dir. agentUUID is the instance's
// AgentUUID, which names the files it writes (SPEC §12.1).
func NewStore(dir, agentUUID string) *Store { return &Store{dir: dir, agent: agentUUID} }

// SetAbandon installs the check a Write in progress consults: true means the
// process was aborted and counted the dump's messages lost, so the dump gives
// up and removes its temporary file instead of publishing a file the next boot
// would reingest (SPEC §13.4). Checked every 1024 records and before the
// publish; a dump that publishes between the last check and the abort is both
// counted and kept, an overcount, never a loss (ported from
// go-relplog-nats-server, independent review of 2026-10-13).
func (s *Store) SetAbandon(f func() bool) { s.abandon = f }

func (s *Store) abandoned() bool { return s.abandon != nil && s.abandon() }

// ErrAbandoned is Write giving up because the process was aborted.
var ErrAbandoned = errors.New("emergency: dump abandoned: the process was aborted")

// Dir reports the configured directory.
func (s *Store) Dir() string { return s.dir }

// Prepare creates the directory and proves it is writable, AT BOOT
// (SPEC §12.3, §13.1 step 6): a directory created during the incident fails
// during the incident, and a permission problem found while dumping is a
// message lost instead of a refused start.
func (s *Store) Prepare() error {
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}
	f, err := os.CreateTemp(s.dir, ".probe-*")
	if err != nil {
		return fmt.Errorf("emergency: %s is not writable: %w", s.dir, err)
	}
	name := f.Name()
	f.Close()
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("emergency: cannot remove the probe in %s: %w", s.dir, err)
	}
	return nil
}

// Write dumps items to a new file and returns its path.
//
// Durability here is the whole point. The records go to a hidden temporary
// name (.emg-…bin.tmp, which List never sees), the file is fsynced, linked to
// its final name (renamed, on a file system without hard links: publish) and
// the directory is fsynced. Written under the final name, a dump cut at a
// record boundary -- a 1 MiB buffer flush ending exactly between two records,
// then a SIGKILL, an OOM or an abort before the fsync -- would read back as a
// clean file, be delivered in part and then REMOVED, the tail lost with no
// count; and another instance sharing the directory could take a file still
// being written. Without the directory fsync, a crash right after this call
// can leave a name that never made it to disk -- the data written and
// unreachable. Any failure removes what was written (SPEC §12.1, §12.3).
func (s *Store) Write(items []*item.Item) (string, error) {
	if len(items) == 0 {
		return "", nil
	}
	if s.abandoned() {
		return "", ErrAbandoned
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}

	name := FileName(s.agent, time.Now())
	path := filepath.Join(s.dir, name)
	tmp := filepath.Join(s.dir, "."+name+tmpSuffix)

	// O_EXCL: never append to or truncate an existing file, which would be
	// another instance's -- or an earlier incident's -- pending data.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", tmp, err)
	}

	w := bufio.NewWriterSize(f, 1<<20)
	head := make([]byte, 0, 512)
	hdr := make([]byte, 0, maxHeaderLine)
	for i, it := range items {
		if i&1023 == 1023 && s.abandoned() {
			f.Close()
			return "", s.abort(tmp, ErrAbandoned)
		}
		if it == nil {
			continue
		}
		head = item.EncodeHead(head[:0], &it.Head)
		hdr = strconv.AppendInt(hdr[:0], int64(len(head)), 10)
		hdr = append(hdr, ' ')
		hdr = strconv.AppendInt(hdr, int64(len(it.Raw)), 10)
		hdr = append(hdr, '\n')
		if _, err := w.Write(hdr); err != nil {
			f.Close()
			return "", s.abort(tmp, err)
		}
		if _, err := w.Write(head); err != nil {
			f.Close()
			return "", s.abort(tmp, err)
		}
		if _, err := w.Write(it.Raw); err != nil {
			f.Close()
			return "", s.abort(tmp, err)
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		return "", s.abort(tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", s.abort(tmp, err)
	}
	if err := f.Close(); err != nil {
		return "", s.abort(tmp, err)
	}
	if s.abandoned() {
		return "", s.abort(tmp, ErrAbandoned)
	}
	if err := publish(tmp, path); err != nil {
		return "", s.abort(tmp, err)
	}
	if err := syncDir(s.dir); err != nil {
		// The name is published but may not survive a power loss, and the
		// process would leave believing the messages were saved: the dump
		// failed (SPEC §12.3), and the file goes with it.
		_ = os.Remove(path)
		return "", s.abort(tmp, err)
	}
	return path, nil
}

// link is os.Link, replaceable by the tests: a file system without hard links
// is not something a test can count on having.
var link = os.Link

// publish gives the complete temporary file its final name, never over an
// existing dump: the stamp makes a collision unlikely, and Link refuses one
// where Rename would silently replace it (SPEC §12.1).
//
// A file system without hard links -- vfat, exFAT, the object-store FUSE
// mounts -- refuses Link whatever the names, and every dump would fail there,
// the whole ring lost, where writing in place had worked. There the final
// name is checked free and the file renamed: the nanosecond stamp in the name
// is what keeps the window between the two harmless. A Link refused because
// the name exists is no reason to try. Either way the directory fsync that
// follows decides the dump, the renamed one included (ported from
// go-syslog-nats-server, which keeps that fsync fatal as this server does).
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

// FileName is emg-<agent_uuid>-<unixnano>.bin (SPEC §12.1).
//
// The nanosecond stamp is always nineteen digits (from 2001 to 2286), so
// within one agent the lexicographic order of the names IS the temporal order,
// which is the order List and the reingestion use (SPEC §12.2).
func FileName(agentUUID string, t time.Time) string {
	return filePrefix + sanitize(agentUUID) + "-" + strconv.FormatInt(t.UnixNano(), 10) + fileSuffix
}

// abort removes a partially written dump: a truncated file that nobody knows is
// truncated is worse than no file, because reingestion would silently deliver
// half a batch and delete the rest. path is the temporary name.
func (s *Store) abort(path string, cause error) error {
	_ = os.Remove(path)
	return fmt.Errorf("emergency: writing %s: %w", path, cause)
}

// Leftovers deals with the temporary files a dump leaves when it is cut short
// (SPEC §12.1); the boot calls it once, before List.
//
// A temporary that is the same file as the dump under its final name is a
// twin -- the process died between the link and the removal of the temporary
// -- and is removed: the dump itself is what reingestion reads. Any other is
// an interrupted dump. It is returned, for the boot to report at WARN, and
// never read or removed here: it may be complete (a SIGKILL after the last
// write) or cut anywhere, its messages were counted as lost only if a second
// signal aborted the dump, and only the operator can decide; renamed to its
// final name, its intact records are reingested and the rest quarantined at
// the next boot. Without this nothing told the operator it was there (the
// review of go-webhook-nats-server).
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

// List returns the dump files in lexicographic order of name (SPEC §12.2):
// temporal per agent, thanks to the fixed-width stamp of FileName. Quarantined
// files (.corrupt) and anything else in the directory are ignored.
func (s *Store) List() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(n, filePrefix) || !strings.HasSuffix(n, fileSuffix) {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = filepath.Join(s.dir, n)
	}
	return out, nil
}

// ReadResult reports what came out of one file.
type ReadResult struct {
	Items []*item.Item
	// Truncated is set when the file ended mid-record or a header line was
	// unreadable. The intact prefix is still returned: partial recovery beats
	// none.
	Truncated bool
	// TruncatedBytes is how much unusable tail was discarded.
	TruncatedBytes int64
	// Undecodable counts records whose head would not decode. They are skipped
	// rather than ending the file: the lengths located the records after them
	// exactly.
	Undecodable int
	// UndecodableBytes is the size of those records, header line included.
	UndecodableBytes int64
}

// Damaged reports whether anything in the file could not be recovered. The app
// then renames the file to .corrupt (Quarantine) after the intact items are
// confirmed, instead of removing it, and logs the byte counts at ERROR
// (SPEC §12.2).
func (r *ReadResult) Damaged() bool { return r.Truncated || r.Undecodable > 0 }

// Read parses one dump file.
//
// This decoder reads from disk after a crash, which makes it a parser of
// untrusted structured input and one of the mandatory fuzz targets
// (SPEC §18.3, FuzzRead). It never trusts a declared length: a corrupt header
// claiming 2 GiB must produce a truncation report, not an allocation.
func (s *Store) Read(path string) (*ReadResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	res := &ReadResult{}
	if err := readFrom(bufio.NewReaderSize(f, 1<<20), fi.Size(), res, false); err != nil {
		return res, err
	}
	return res, nil
}

// readFrom is Read without the file, so the fuzz target can drive it from a
// byte slice. With countOnly the raw lines are skipped, not allocated, and
// res.Items receives nil placeholders whose number is the count.
func readFrom(r *bufio.Reader, size int64, res *ReadResult, countOnly bool) error {
	var consumed int64
	var scratch []byte

	for {
		line, err := readHeaderLine(r)
		if err == io.EOF && len(line) == 0 {
			return nil // clean end
		}
		if err != nil && err != io.EOF && err != errHeaderTooLong {
			return err
		}
		if err != nil {
			// A header line with no terminator, or one far too long to be a
			// header: the file was cut, or this is not a header at all.
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return nil
		}

		headLen, rawLen, perr := parseHeader(line)
		if perr != nil {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return nil
		}
		// The declared lengths must fit in what is left of the file. This is
		// the check that turns a corrupt header into a truncation report
		// instead of a multi-gigabyte allocation.
		remaining := size - consumed - int64(len(line))
		if headLen+rawLen > remaining {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return nil
		}

		if int64(cap(scratch)) < headLen {
			scratch = make([]byte, headLen)
		}
		head := scratch[:headLen]
		if _, err := io.ReadFull(r, head); err != nil {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return nil
		}
		h, derr := item.DecodeHead(head)

		var raw []byte
		if countOnly || derr != nil {
			_, err = r.Discard(int(rawLen))
		} else {
			// Buffers here come from make, not the pool: reingestion is a
			// boot-time path where allocation is free, and an exact-size
			// buffer holds a ring's worth of 200-byte lines in a tenth of
			// what the 2 KiB pool class would. The class of -1 is what stops
			// Release from filing them anywhere.
			raw = make([]byte, rawLen)
			_, err = io.ReadFull(r, raw)
		}
		if err != nil {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return nil
		}
		recLen := int64(len(line)) + headLen + rawLen
		consumed += recLen

		if derr != nil {
			// This record is unusable, but the ones after it are not: the
			// lengths located them exactly. Skipping is strictly better than
			// abandoning the rest of the file.
			res.Undecodable++
			res.UndecodableBytes += recLen
			continue
		}
		if countOnly {
			res.Items = append(res.Items, nil)
			continue
		}
		res.Items = append(res.Items, item.New(h, raw, -1))
	}
}

var errHeaderTooLong = errors.New("emergency: header line too long")

// readHeaderLine reads up to and including '\n', at most maxHeaderLine bytes.
func readHeaderLine(r *bufio.Reader) ([]byte, error) {
	var buf [maxHeaderLine]byte
	n := 0
	for {
		c, err := r.ReadByte()
		if err != nil {
			return buf[:n], err
		}
		if n == len(buf) {
			return buf[:n], errHeaderTooLong
		}
		buf[n] = c
		n++
		if c == '\n' {
			return append([]byte(nil), buf[:n]...), nil
		}
	}
}

// parseHeader accepts exactly what Write produces: two decimal numbers, one
// space, one newline. Anything looser -- signs, padding, a CR -- is a header
// this program did not write, so it is corruption, and treating it as such
// stops the read at the last record known to be intact.
func parseHeader(line []byte) (headLen, rawLen int64, err error) {
	if len(line) < 4 || line[len(line)-1] != '\n' {
		return 0, 0, fmt.Errorf("malformed header %q", line)
	}
	body := line[:len(line)-1]
	sp := -1
	for i, c := range body {
		if c == ' ' {
			if sp >= 0 {
				return 0, 0, fmt.Errorf("malformed header %q", line)
			}
			sp = i
		}
	}
	if sp <= 0 || sp == len(body)-1 {
		return 0, 0, fmt.Errorf("malformed header %q", line)
	}
	if headLen, err = parseLen(body[:sp]); err != nil {
		return 0, 0, err
	}
	if rawLen, err = parseLen(body[sp+1:]); err != nil {
		return 0, 0, err
	}
	return headLen, rawLen, nil
}

func parseLen(b []byte) (int64, error) {
	if len(b) == 0 || len(b) > 10 {
		return 0, fmt.Errorf("bad length %q", b)
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("bad length %q", b)
		}
		n = n*10 + int64(c-'0')
	}
	if n > maxLenField {
		return 0, fmt.Errorf("length %d above the ceiling", n)
	}
	return n, nil
}

// Remove deletes a fully reingested dump: only after every one of its items was
// confirmed (SPEC §12.2).
//
// The directory is fsynced after the unlink. Without it a power loss right
// after the reingestion can bring the name back, and the next boot delivers
// the whole dump a second time under the same uuids -- deduplicated by (uuid,
// step) where the destination can, a duplicate everywhere else (the
// JetStream window long gone, core NATS, a Redis stream). An error wrapping
// ErrNotDurable means the file WAS removed and only that durability is in
// doubt; the caller logs it at WARN.
func (s *Store) Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Quarantine renames a damaged file aside instead of deleting it. The intact
// records have already been reingested; what remains is unrecoverable by this
// program but may still be worth a human's attention, and deleting evidence of
// a crash is not this program's call.
//
// A non-empty name means the file WAS quarantined, whatever the error says:
// the rename happened, and the error, if any, is the directory fsync that
// followed (errors.Is(err, ErrNotDurable)), which only means the new name may
// not survive a power loss. The caller logs that at WARN and still reports
// the quarantine -- the ERROR with the discarded byte counts -- under the new
// name. Read as "not quarantined", it logged "cannot quarantine" with a path
// that no longer existed and dropped the count of what was lost (SPEC §12.2).
// An empty name means the file is still where it was.
func (s *Store) Quarantine(path string) (string, error) {
	dst := path + corruptExt
	if err := os.Rename(path, dst); err != nil {
		return "", err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return dst, err
	}
	return dst, nil
}

// Count reports how many files are pending and how many messages they hold, for
// the relplog_emergency_files and relplog_emergency_events_pending gauges. It
// decodes the heads, so the count is of messages that will really come back,
// but skips the raw lines without allocating them.
func (s *Store) Count() (files int, events int64, err error) {
	paths, err := s.List()
	if err != nil {
		return 0, 0, err
	}
	for _, p := range paths {
		n, err := countFile(p)
		if err != nil {
			continue
		}
		files++
		events += n
	}
	return files, events, nil
}

func countFile(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	res := &ReadResult{}
	if err := readFrom(bufio.NewReaderSize(f, 1<<16), fi.Size(), res, true); err != nil {
		return 0, err
	}
	return int64(len(res.Items)), nil
}

// syncDir fsyncs a directory so a newly created (or renamed) filename is
// durable.
//
// Its error is returned, not swallowed. Without the directory fsync a crash
// right after the dump can leave the file's data on disk and its NAME not --
// written and unreachable -- while the process exits believing the messages
// were saved. A filesystem that refuses fsync on a directory cannot give the
// dump its guarantee, and the honest outcome is the one of SPEC §12.3: the
// dump failed, the count goes to reason="emergency_failed", exit 3.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		// Also not durable, not undone: Remove and Quarantine call this AFTER
		// the unlink or the rename succeeded, and reporting "cannot remove"
		// for a file that is gone would be false (go-relplog-nats-server,
		// independent review, 2026-10-13). In Write it is still a failed dump.
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
// Quarantine happened, and only its durability is in doubt. Write's error may wrap it too, but
// there the fsync decides the dump: the file is removed and the dump has
// failed (SPEC §12.3).
var ErrNotDurable = errors.New("emergency: the directory entry may not be durable")

// dirSync is (*os.File).Sync, behind a variable only so the tests can make a
// directory fsync fail: no portable filesystem does that on demand.
var dirSync = func(d *os.File) error { return d.Sync() }

// sanitize keeps an agent id usable inside a filename. A canonical AgentUUID
// passes through unchanged ([0-9a-f-]), so the name is literally the one of
// SPEC §12.1; anything else -- a path separator above all -- becomes '_'.
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
