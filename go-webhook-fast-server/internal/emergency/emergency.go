// Package emergency writes and reingests the emergency dump of SPEC §9.
//
// The dump is a property of the RING, not of the sink. It fires whenever the
// process is leaving with a non-empty memory ring: a fatal sink failure, or a
// shutdown drain that ran out of time. With ring=sqlite the items are already
// on disk, and with ring=none there is never a backlog, so neither needs it.
package emergency

import (
	"bufio"
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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// File format, per SPEC §9.1:
//
//	<len_jlh>\n
//	<JLH bytes>
//	... repeated
//
// The header line is ASCII decimal; the envelope follows as raw bytes, exactly
// the bytes the client saw confirmed. The v1 format had TWO lengths on the
// header line (old envelope, body); a file in that format is refused whole as
// legacy (SPEC §9.2), never half-read.
const (
	filePrefix  = "emg-"
	fileSuffix  = ".bin"
	corruptExt  = ".corrupt"
	legacyExt   = ".legacy"
	maxLenField = 1 << 31 // sanity ceiling on a declared length
	// tmpSuffix marks a dump still being written, under a hidden name the
	// listing never matches (SPEC §9.1).
	tmpSuffix = ".tmp"
)

// ErrLegacy is returned by Read for a dump of the pre-LogHub format.
var ErrLegacy = errors.New("emergency: dump in the pre-LogHub format (two lengths per record)")

// Store reads and writes dumps in one directory.
type Store struct {
	dir     string
	agent   string // the AgentUUID: whose dump a file is (SPEC §9.1)
	abandon func() bool
}

func NewStore(dir, agentUUID string) *Store { return &Store{dir: dir, agent: agentUUID} }

// SetAbandon installs the check a Write in progress consults: true means the
// process was aborted and counted the dump's events lost, so the dump gives up
// and removes its temporary file instead of publishing a file the next boot
// would reingest -- counted lost AND delivered (SPEC §10.3). Checked before
// the first record, every 1024 records and before the publish; a dump that
// publishes between the last check and the abort is both counted and kept, an
// overcount, never a loss (port of the syslog/RELP family, 2026-10-13).
func (s *Store) SetAbandon(f func() bool) { s.abandon = f }

func (s *Store) abandoned() bool { return s.abandon != nil && s.abandon() }

// ErrAbandoned is Write giving up because the process was aborted.
var ErrAbandoned = errors.New("emergency: dump abandoned: the process was aborted")

// Dir reports the configured directory.
func (s *Store) Dir() string { return s.dir }

// Write dumps items to a new file and returns its path.
//
// Durability here is the whole point, so the file is fsynced AND the directory
// entry is fsynced. Without the second one, a crash right after this call can
// leave a file whose name never made it to disk -- the data would be written
// and unreachable.
//
// The file is written under a hidden temporary name and linked to the final
// one only once complete and synced (SPEC §9.1). Written under the final name,
// a process killed mid-write left a file that, cut at a record boundary, read
// clean -- and was removed after being delivered in part (the review of
// go-syslog-nats-server, ported from go-webhook-nats-server).
func (s *Store) Write(items []*envelope.Item) (string, error) {
	if len(items) == 0 {
		return "", nil
	}
	if s.abandoned() {
		return "", ErrAbandoned
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}

	name := fmt.Sprintf("%s%s-%d%s", filePrefix, sanitize(s.agent), time.Now().UnixNano(), fileSuffix)
	path := filepath.Join(s.dir, name)
	tmp := filepath.Join(s.dir, "."+name+tmpSuffix)

	// O_EXCL: never append to or truncate an existing dump, which would be
	// another instance's pending data.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", tmp, err)
	}

	w := bufio.NewWriterSize(f, 1<<20)
	var written int
	for i, it := range items {
		if i&1023 == 1023 && s.abandoned() {
			f.Close()
			return "", s.abort(tmp, ErrAbandoned)
		}
		if it == nil {
			continue
		}
		if _, err := fmt.Fprintf(w, "%d\n", len(it.Env)); err != nil {
			f.Close()
			return "", s.abort(tmp, err)
		}
		if _, err := w.Write(it.Env); err != nil {
			f.Close()
			return "", s.abort(tmp, err)
		}
		written++
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
	// Best effort here, unlike Remove and Quarantine: the dump is complete,
	// synced and under its final name, and discarding it for the directory
	// fsync would lose it (SPEC §9.1).
	_ = syncDir(s.dir)
	return path, nil
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

// abort removes a partially written dump: a truncated file that nobody knows is
// truncated is worse than no file, because reingestion would silently deliver
// half a batch and delete the rest. path is the temporary name.
func (s *Store) abort(path string, cause error) error {
	_ = os.Remove(path)
	return fmt.Errorf("emergency: writing %s: %w", path, cause)
}

// List returns the dump files in chronological order. The name carries a
// nanosecond timestamp, so lexicographic order over a fixed-width suffix is
// chronological order.
func (s *Store) List() ([]string, error) {
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
		if !e.Type().IsRegular() || !strings.HasPrefix(n, filePrefix) || !strings.HasSuffix(n, fileSuffix) {
			continue
		}
		out = append(out, filepath.Join(s.dir, n))
	}
	sort.Slice(out, func(i, j int) bool { return tsOf(out[i]) < tsOf(out[j]) })
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

// tsOf extracts the nanosecond stamp from a dump filename, so ordering does not
// depend on how the agent uuid sorts.
func tsOf(path string) int64 {
	base := strings.TrimSuffix(filepath.Base(path), fileSuffix)
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
	// Truncated is set when the file ended mid-record. The intact prefix is
	// still returned: partial recovery beats none.
	Truncated bool
	// TruncatedBytes is how much unusable tail was discarded.
	TruncatedBytes int64
}

// Read parses one dump file.
//
// This decoder reads from disk after a crash, which makes it the second place
// in this server that parses untrusted structured input. It never trusts a
// declared length: a corrupt header claiming 4 GiB must produce an error, not
// an allocation.
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
	size := fi.Size()

	res := &ReadResult{}
	r := bufio.NewReaderSize(f, 1<<20)
	var consumed int64

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF && line == "" {
			return res, nil // clean end
		}
		if err != nil && err != io.EOF {
			return res, err
		}
		if err == io.EOF {
			// A header line with no terminator: the file was cut here.
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return res, nil
		}

		n, legacy, perr := parseHeader(line)
		if legacy && consumed == 0 {
			return nil, ErrLegacy
		}
		if perr != nil {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return res, nil
		}
		// The declared length must fit in what is left of the file. This is
		// the check that turns a corrupt header into a truncation report
		// instead of a multi-gigabyte allocation.
		// TruncatedBytes counts from the start of the record, its header line
		// included: that is what is lost.
		remaining := size - consumed - int64(len(line))
		if int64(n) > remaining {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return res, nil
		}

		env := make([]byte, n)
		if _, err := io.ReadFull(r, env); err != nil {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return res, nil
		}
		// A record whose JLH is not the shape this server writes is damage,
		// not an event: everything from here on is treated as truncated.
		// ParseHead checks the header and only scans the payload's braces, so
		// json.Valid reads the whole of it -- free at boot, and the one thing
		// that tells "{]" or "{,,,}" from an object a destination can store.
		h, _, herr := envelope.ParseHead(env)
		if herr != nil || !json.Valid(env) {
			res.Truncated = true
			res.TruncatedBytes = size - consumed
			return res, nil
		}
		consumed += int64(len(line)) + int64(n)

		// Buffers here come from make, not the pool: they outlive any pool
		// class and reingestion is a boot-time path where allocation is free.
		res.Items = append(res.Items, envelope.NewItem(h.UUID.String(), env, -1))
	}
}

// parseHeader reads "<len>\n". Two lengths is the v1 format: legacy. Two
// fields that are not both lengths are damage, not an old version.
func parseHeader(line string) (n int, legacy bool, err error) {
	f := strings.Fields(strings.TrimSpace(line))
	if len(f) == 2 {
		a, aerr := strconv.Atoi(f[0])
		b, berr := strconv.Atoi(f[1])
		if aerr == nil && berr == nil && a >= 0 && b >= 0 && a <= maxLenField && b <= maxLenField {
			return 0, true, fmt.Errorf("legacy header %q", line)
		}
		return 0, false, fmt.Errorf("malformed header %q", line)
	}
	if len(f) != 1 {
		return 0, false, fmt.Errorf("malformed header %q", line)
	}
	n, err = strconv.Atoi(f[0])
	if err != nil || n < 0 || n > maxLenField {
		return 0, false, fmt.Errorf("bad length %q", f[0])
	}
	return n, false, nil
}

// Remove deletes a fully reingested dump.
//
// The directory is fsynced after the unlink. Without it a power loss right
// after the reingestion can bring the name back, and the next boot delivers
// the whole dump a second time under the same uuids -- deduplicated where the
// destination can, a duplicate everywhere else (a Redis stream, NATS past its
// duplicate_window, the SQLite and ClickHouse inbox). An error wrapping
// ErrNotDurable means the file WAS removed and only that durability is in
// doubt; the caller logs it at WARN (SPEC §9.2; port of the syslog/RELP
// family, 2026-10-13).
func (s *Store) Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Quarantine renames a truncated file aside instead of deleting it. The intact
// prefix has already been reingested; what remains is unrecoverable by this
// program but may still be worth a human's attention, and deleting evidence of
// a crash is not this program's call.
//
// A non-empty name means the file WAS quarantined, whatever the error says:
// the rename happened, and the error, if any, is the directory fsync that
// followed (errors.Is(err, ErrNotDurable)), which only means the new name may
// not survive a power loss -- and the next boot would then deliver the intact
// prefix again. The caller logs that at WARN and still reports the quarantine
// under the new name. An empty name means the file is still where it was.
func (s *Store) Quarantine(path string) (string, error) {
	return renameAside(path, path+corruptExt)
}

// SetAsideLegacy renames a dump of the pre-LogHub format to .legacy (SPEC
// §9.2): it is not reingested -- no destination understands that envelope any
// more -- and it is not deleted, so the previous version can still reingest it.
// The name and the error mean what they mean for Quarantine.
func (s *Store) SetAsideLegacy(path string) (string, error) {
	return renameAside(path, path+legacyExt)
}

// renameAside is the rename of Quarantine and SetAsideLegacy, with the
// directory fsync after it.
func renameAside(path, dst string) (string, error) {
	if err := os.Rename(path, dst); err != nil {
		return "", err
	}
	return dst, syncDir(filepath.Dir(dst))
}

// Count reports how many files are pending and how many events they hold.
//
// Every listed file counts, readable or not: one the boot could not read stays
// in place and is still awaiting reingestion, and leaving it out hid it from
// whs_emergency_files. Its events are unknown and not counted (SPEC §11.1).
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
// Quarantine and SetAsideLegacy happened, and only its durability is in doubt.
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
