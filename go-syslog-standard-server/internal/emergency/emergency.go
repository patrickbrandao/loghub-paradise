// Package emergency writes and reingests the emergency dump of SPEC §11.
//
// The dump is a property of the RING, not of the sink. It fires whenever the
// process is leaving with a non-empty memory ring: a fatal sink failure, or a
// shutdown drain that ran out of time. With ring=sqlite the items are already on
// disk, and with ring=none there is never a backlog, so neither needs it.
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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

// File format, per SPEC §11.1:
//
//	<len_head> <len_raw>\n
//	<head bytes>
//	<raw line bytes>
//	... repeated
//
// The header line is ASCII decimal; the two payloads follow as raw bytes. This is
// deliberately NOT line-delimited JSON: a syslog line is arbitrary bytes, and
// escaping it to fit a text format would cost a full re-encode of every pending
// message at the worst possible moment.
//
// What is dumped is exactly the pair of SPEC §3.2 -- item head plus raw line --
// and nothing else, because nothing else is needed: every other envelope field is
// derived from those two (invariant §19.13).
//
// That is the whole difference from the sibling service, and it removes a trap
// that cost a real bug there. Its dump stored a FINISHED ENVELOPE, so the
// queryable columns of the destination had to be rebuilt by parsing escaped JSON
// on the way back in; forgetting it wrote rows with an empty path and ts = 0,
// invisible to the very query those columns exist for, with an intact envelope
// beside them hiding the loss. Here the head IS the metadata and comes back
// whole.
const (
	filePrefix  = "emg-"
	fileSuffix  = ".bin"
	corruptExt  = ".corrupt"
	maxLenField = 1 << 31 // sanity ceiling on a declared length
)

// Store reads and writes dumps in one directory.
type Store struct {
	dir    string
	nodeID string
}

func NewStore(dir, nodeID string) *Store { return &Store{dir: dir, nodeID: nodeID} }

// Dir reports the configured directory.
func (s *Store) Dir() string { return s.dir }

// Write dumps items to a new file and returns its path.
//
// Durability here is the whole point, so the file is fsynced AND the directory
// entry is fsynced. Without the second one, a crash right after this call can
// leave a file whose name never made it to disk -- the data would be written and
// unreachable.
func (s *Store) Write(items []*envelope.Item) (string, error) {
	if len(items) == 0 {
		return "", nil
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}

	name := fmt.Sprintf("%s%s-%d%s", filePrefix, sanitize(s.nodeID), time.Now().UnixNano(), fileSuffix)
	path := filepath.Join(s.dir, name)

	// O_EXCL: never append to or truncate an existing dump, which would be
	// another instance's pending data.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return "", fmt.Errorf("emergency: cannot create %s: %w", path, err)
	}

	w := bufio.NewWriterSize(f, 1<<20)
	head := make([]byte, 0, 256)
	for _, it := range items {
		if it == nil {
			continue
		}
		head = envelope.EncodeHead(head[:0], &it.Head)
		if _, err := fmt.Fprintf(w, "%d %d\n", len(head), len(it.Raw)); err != nil {
			f.Close()
			return "", s.abort(path, err)
		}
		if _, err := w.Write(head); err != nil {
			f.Close()
			return "", s.abort(path, err)
		}
		if _, err := w.Write(it.Raw); err != nil {
			f.Close()
			return "", s.abort(path, err)
		}
	}

	if err := w.Flush(); err != nil {
		f.Close()
		return "", s.abort(path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", s.abort(path, err)
	}
	if err := f.Close(); err != nil {
		return "", s.abort(path, err)
	}
	if err := syncDir(s.dir); err != nil {
		return "", s.abort(path, err)
	}
	return path, nil
}

// abort removes a partially written dump: a truncated file that nobody knows is
// truncated is worse than no file, because reingestion would silently deliver
// half a batch and delete the rest.
func (s *Store) abort(path string, cause error) error {
	_ = os.Remove(path)
	return fmt.Errorf("emergency: writing %s: %w", path, cause)
}

// List returns the dump files in chronological order. The name carries a
// nanosecond timestamp, so ordering is by that stamp and not by how the node id
// happens to sort.
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
		if e.IsDir() || !strings.HasPrefix(n, filePrefix) || !strings.HasSuffix(n, fileSuffix) {
			continue
		}
		out = append(out, filepath.Join(s.dir, n))
	}
	sort.Slice(out, func(i, j int) bool { return tsOf(out[i]) < tsOf(out[j]) })
	return out, nil
}

// tsOf extracts the nanosecond stamp from a dump filename.
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
	// Undecodable counts records whose head would not decode. They are skipped
	// rather than ending the file: the records after them are intact.
	Undecodable int
}

// Read parses one dump file.
//
// This decoder reads from disk after a crash, which makes it the second place in
// this server that parses untrusted structured input, and one of the mandatory
// fuzz targets (SPEC §17.2). It never trusts a declared length: a corrupt header
// claiming 4 GiB must produce a truncation report, not an allocation.
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
	if err := readFrom(bufio.NewReaderSize(f, 1<<20), fi.Size(), res); err != nil {
		return res, err
	}
	return res, nil
}

// readFrom is Read without the file, so the fuzz target can drive it from a
// byte slice.
func readFrom(r *bufio.Reader, size int64, res *ReadResult) error {
	var consumed int64

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF && line == "" {
			return nil // clean end
		}
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			// A header line with no terminator: the file was cut here.
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
		// The declared lengths must fit in what is left of the file. This is the
		// check that turns a corrupt header into a truncation report instead of
		// a multi-gigabyte allocation.
		remaining := size - consumed - int64(len(line))
		if int64(headLen)+int64(rawLen) > remaining {
			res.Truncated = true
			res.TruncatedBytes = remaining
			return nil
		}

		head := make([]byte, headLen)
		if _, err := io.ReadFull(r, head); err != nil {
			res.Truncated = true
			res.TruncatedBytes = remaining
			return nil
		}
		raw := make([]byte, rawLen)
		if _, err := io.ReadFull(r, raw); err != nil {
			res.Truncated = true
			res.TruncatedBytes = remaining - int64(headLen)
			return nil
		}
		consumed += int64(len(line)) + int64(headLen) + int64(rawLen)

		h, derr := envelope.DecodeHead(head)
		if derr != nil {
			// This record is unusable, but the ones after it are not: the
			// lengths located them exactly. Skipping is strictly better than
			// abandoning the rest of the file.
			res.Undecodable++
			continue
		}

		// Buffers here come from make, not the pool: they outlive any pool class
		// and reingestion is a boot-time path where allocation is free. The
		// class of -1 is what stops Release from filing them anywhere.
		res.Items = append(res.Items, envelope.NewItem(h, raw, -1))
	}
}

func parseHeader(line string) (headLen, rawLen int, err error) {
	f := strings.Fields(strings.TrimSpace(line))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("malformed header %q", line)
	}
	headLen, err = strconv.Atoi(f[0])
	if err != nil || headLen < 0 || headLen > maxLenField {
		return 0, 0, fmt.Errorf("bad head length %q", f[0])
	}
	rawLen, err = strconv.Atoi(f[1])
	if err != nil || rawLen < 0 || rawLen > maxLenField {
		return 0, 0, fmt.Errorf("bad raw length %q", f[1])
	}
	return headLen, rawLen, nil
}

// Remove deletes a fully reingested dump.
func (s *Store) Remove(path string) error { return os.Remove(path) }

// Quarantine renames a truncated file aside instead of deleting it. The intact
// prefix has already been reingested; what remains is unrecoverable by this
// program but may still be worth a human's attention, and deleting evidence of a
// crash is not this program's call.
func (s *Store) Quarantine(path string) (string, error) {
	dst := path + corruptExt
	if err := os.Rename(path, dst); err != nil {
		return "", err
	}
	return dst, nil
}

// Count reports how many files are pending and how many messages they hold, for
// the syslog_emergency_* gauges.
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

// syncDir fsyncs a directory so a newly created filename is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	// Some filesystems refuse fsync on a directory. That is not a reason to
	// discard a dump that is otherwise written and synced.
	_ = d.Sync()
	return nil
}

// sanitize keeps a node id usable inside a filename.
func sanitize(s string) string {
	if s == "" {
		return "node"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s) && i < 64; i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
