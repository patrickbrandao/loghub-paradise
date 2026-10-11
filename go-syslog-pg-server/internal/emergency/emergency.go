// Package emergency writes and reingests the emergency dump of SPEC §12.
//
// The dump fires when the process is leaving with a non-empty ring (or a batch
// the workers were still holding): a FATAL sink, or a shutdown drain that ran
// out of time. The ring lives in memory, so without the dump those messages
// would die with the process.
//
// The file is JSON Lines: one finished JLH envelope per line, exactly the bytes
// the worker would have written to loghub.inbox (SPEC §12.1). That makes a
// dump readable with jq and usable by any LogHub tool, not only by this
// server. The price is paid at dump time -- every pending item is converted
// before it is written -- and it is paid once, by a process that is leaving
// anyway.
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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlh"
)

const (
	filePrefix = "emg-"
	fileSuffix = ".jsonl"
	corruptExt = ".corrupt"
	tmpSuffix  = ".tmp"

	// MaxLine is the longest line the reader accepts: the worst JLH of SPEC
	// §3.9 at the largest syslog.max_message_bytes the configuration allows
	// (12 × 16 MiB + 8 KiB). It is the ceiling of every configuration, not of
	// the running one, on purpose: a dump written by a run with a larger
	// max_message_bytes must still come back after the operator lowered it.
	// The reader never allocates by it: a line grows with what is actually
	// read, and a longer one is discarded without being held.
	MaxLine = 12*(16<<20) + 8<<10

	// syslogType is the only JLH type this server writes (SPEC §3.1).
	syslogType = "syslog"
)

// Store reads and writes dumps in one directory.
type Store struct {
	dir   string
	agent string
}

// NewStore builds a store for emergency.dir. agentUUID is the instance's
// AgentUUID, which names the files it writes (SPEC §12.1).
func NewStore(dir, agentUUID string) *Store { return &Store{dir: dir, agent: agentUUID} }

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

// WriteResult reports what one dump did.
type WriteResult struct {
	Path string
	// Written is the number of envelopes in the file.
	Written int
	// Refused is the number of malformed messages left out because
	// syslog.accept_malformed is false (SPEC §6.6): the worker would have
	// refused them too, so the dump does not resurrect them. The caller counts
	// them as events_dropped{reason="malformed"}.
	Refused int
}

// Write converts items to JLH and dumps them, one envelope per line, to a new
// file (SPEC §12.1).
//
// The conversion is the worker's, with the frozen conversion config, so a line
// is byte for byte the envelope the sink would have received (SPEC §3.5). The
// converter is the caller's and is not safe for concurrent use; the dump runs
// once, on the shutdown path, after the workers stopped.
//
// Durability here is the whole point. The lines go to a hidden temporary name
// (.emg-…jsonl.tmp, which List never sees), the file is fsynced, linked to its
// final name (renamed, on a file system without hard links: publish) and the
// directory is fsynced. Written under the final name, a
// dump cut at a line boundary -- a 1 MiB buffer flush ending on '\n', then a
// SIGKILL, an OOM, a power loss before the fsync -- would read back as a clean
// file, be delivered in part and then REMOVED, the tail lost with no count;
// and another instance sharing the directory could take a file still being
// written. Without the directory fsync, a crash right after this call can
// leave a name that never made it to disk. Any failure removes the temporary
// file (SPEC §12.1, §12.3).
func (s *Store) Write(items []*item.Item, conv *convert.Converter, acceptMalformed bool) (WriteResult, error) {
	var res WriteResult
	if len(items) == 0 {
		return res, nil
	}
	if err := os.MkdirAll(s.dir, 0o750); err != nil {
		return res, fmt.Errorf("emergency: cannot create %s: %w", s.dir, err)
	}

	name := FileName(s.agent, time.Now())
	path := filepath.Join(s.dir, name)
	tmp := filepath.Join(s.dir, "."+name+tmpSuffix)
	// O_EXCL: never append to or truncate an existing dump, which would be
	// another instance's -- or an earlier incident's -- pending data.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return res, fmt.Errorf("emergency: cannot create %s: %w", tmp, err)
	}

	w := bufio.NewWriterSize(f, 1<<20)
	var line []byte
	for _, it := range items {
		if it == nil {
			continue
		}
		var cr convert.Result
		line, cr = conv.Append(line[:0], &it.Head, it.Raw)
		if !acceptMalformed && cr.Format == convert.FormatMalformed {
			res.Refused++
			continue
		}
		// A JLH never holds a raw newline: the writer escapes every control
		// character (SPEC §3.3). So '\n' is a safe separator, and the line
		// is exactly the envelope.
		line = append(line, '\n')
		if _, err := w.Write(line); err != nil {
			f.Close()
			return WriteResult{}, s.abort(tmp, err)
		}
		res.Written++
	}
	if res.Written == 0 {
		// Everything was refused: an empty file would only be a file to
		// reingest and remove.
		f.Close()
		_ = os.Remove(tmp)
		return res, nil
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return WriteResult{}, s.abort(tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return WriteResult{}, s.abort(tmp, err)
	}
	if err := f.Close(); err != nil {
		return WriteResult{}, s.abort(tmp, err)
	}
	if err := publish(tmp, path); err != nil {
		return WriteResult{}, s.abort(tmp, err)
	}
	if err := syncDir(s.dir); err != nil {
		_ = os.Remove(path)
		return WriteResult{}, s.abort(tmp, err)
	}
	res.Path = path
	return res, nil
}

// link is os.Link, replaceable by the tests: a file system without hard links
// is not something a test can count on having.
var link = os.Link

// publish gives the complete temporary file its final name, never over an
// existing dump: the stamp makes a collision unlikely, and Link refuses one
// where Rename would silently replace it (SPEC §12.1).
//
// A file system without hard links -- vfat, exFAT, the object-store FUSE
// mounts -- refuses Link whatever the names, and every dump failed there, the
// whole ring lost, where writing in place had worked. There the final name is
// checked free and the file renamed: the nanosecond stamp in the name is what
// keeps the window between the two harmless. A Link refused because the name
// exists is no reason to try. Either way the directory fsync that follows
// decides the dump, the renamed one included (syncDir): go-webhook-nats-server
// made that fsync best effort, and this server keeps it fatal on purpose.
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

// FileName is emg-<agent_uuid>-<unixnano>.jsonl (SPEC §12.1).
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
// (SPEC §12.1). A temporary that is the same file as the dump under its final
// name is a twin -- the process died between the link and the removal -- and
// is removed: the dump itself is what reingestion reads. Any other is an
// interrupted dump; it is returned, for the boot to report, and never read or
// removed here. Without this nothing told the operator it was there (the
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

// Record is one envelope read back from a dump: the line itself and the
// columns of loghub.inbox its header carries (SPEC §3.7.1).
//
// The columns are PARSED from the header and checked against the payload
// position the sink will cut, never rebuilt from anything else: the line is the
// only thing the file holds, and what goes to the queue must be what the line
// says.
type Record struct {
	UUID  uuidv7.UUID
	From  string
	Space string
	Level uint8
	SID   uint64
	Event jlh.Event
	// JLH is the whole line without its '\n', in a buffer of its own.
	JLH []byte
}

// ReadResult reports what came out of one file.
type ReadResult struct {
	// Records is filled by Read only; a Reader hands them out chunk by chunk.
	Records []Record
	// Truncated is set when the file ended without the '\n' of its last line:
	// the dump was cut. The intact lines before it are still returned.
	Truncated bool
	// TruncatedBytes is the size of that unterminated tail.
	TruncatedBytes int64
	// Invalid counts complete lines that are not an envelope this server
	// writes (ParseLine) or are longer than MaxLine. They are skipped rather
	// than ending the file: the newline located the next one exactly.
	Invalid int
	// InvalidBytes is their size, newlines included.
	InvalidBytes int64
}

// Damaged reports whether anything in the file could not be recovered. The app
// then renames the file to .corrupt (Quarantine) after the intact records are
// confirmed, instead of removing it, and logs the byte counts at ERROR
// (SPEC §12.2).
func (r ReadResult) Damaged() bool { return r.Truncated || r.Invalid > 0 }

// Reader streams one dump file, a chunk of records at a time.
//
// A dump is NOT read whole. Its lines are finished envelopes, up to twelve
// times the raw lines the ring held (SPEC §3.9): a full 512 MiB ring of
// control-heavy lines dumps gigabytes, and a boot that loaded the file would be
// OOM-killed at step 9, with every transport closed, on every restart -- the
// file is only removed after it is delivered. Streaming bounds the reingestion
// to one chunk plus one line.
//
// This decoder reads from disk after a crash, which makes it a parser of
// untrusted structured input and one of the mandatory fuzz targets
// (SPEC §18.3, FuzzRead).
type Reader struct {
	br      *bufio.Reader
	c       io.Closer
	maxLine int
	res     ReadResult
	pending *Record // read by the last Next, past its byte budget
	eof     bool
}

// Open starts reading one dump file.
func (s *Store) Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return newReader(f, f, MaxLine), nil
}

func newReader(r io.Reader, c io.Closer, maxLine int) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 1<<20), c: c, maxLine: maxLine}
}

// Next returns the next records: at most maxRecords, and no more than maxBytes
// of JLH unless a single record is larger (it then comes alone) -- the chunk
// rule of SPEC §10.1. An empty result with a nil error is the end of the file;
// Result then says what was skipped.
func (r *Reader) Next(maxRecords, maxBytes int) ([]Record, error) {
	if maxRecords < 1 {
		maxRecords = 1
	}
	var out []Record
	size := 0
	for len(out) < maxRecords {
		var rec Record
		if r.pending != nil {
			rec, r.pending = *r.pending, nil
		} else {
			var ok bool
			var err error
			if rec, ok, err = r.next(); err != nil {
				return nil, err
			} else if !ok {
				break
			}
		}
		if len(out) > 0 && size+len(rec.JLH) > maxBytes {
			r.pending = &rec
			break
		}
		out = append(out, rec)
		size += len(rec.JLH)
	}
	return out, nil
}

// next reads lines until one is a valid record or the file ends.
func (r *Reader) next() (Record, bool, error) {
	for !r.eof {
		line, size, terminated, err := readLine(r.br, r.maxLine)
		if err != nil {
			return Record{}, false, err
		}
		if size == 0 {
			r.eof = true // clean end
			break
		}
		if !terminated {
			// The last line has no newline: the dump was cut mid-write, or a
			// later byte was lost. Its envelope may even parse -- a payload
			// cut exactly at a '}' -- but only Write ends a line, so it was
			// never finished.
			r.res.Truncated = true
			r.res.TruncatedBytes = size
			r.eof = true
			break
		}
		if line == nil {
			// Longer than maxLine: discarded while reading, never held.
			r.res.Invalid++
			r.res.InvalidBytes += size
			continue
		}
		rec, perr := ParseLine(line[:len(line)-1])
		if perr != nil {
			r.res.Invalid++
			r.res.InvalidBytes += size
			continue
		}
		return rec, true, nil
	}
	return Record{}, false, nil
}

// Result reports what the reading skipped so far; complete once Next has
// returned the end of the file.
func (r *Reader) Result() ReadResult { return r.res }

// Close releases the file.
func (r *Reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

// Read parses one dump file whole. It is for tests and tools; the boot streams
// with Open and Next.
func (s *Store) Read(path string) (*ReadResult, error) {
	r, err := s.Open(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return readAll(r)
}

func readAll(r *Reader) (*ReadResult, error) {
	var recs []Record
	for {
		chunk, err := r.Next(1024, 1<<30)
		if err != nil {
			res := r.Result()
			return &res, err
		}
		if len(chunk) == 0 {
			res := r.Result()
			res.Records = recs
			return &res, nil
		}
		recs = append(recs, chunk...)
	}
}

// readLine reads up to and including the next '\n'. line is a fresh buffer of
// exactly the line's bytes, or nil when the line passed maxLine -- the rest of
// it is then consumed without being stored. size counts every byte consumed.
// terminated is false only at the end of the input.
func readLine(r *bufio.Reader, maxLine int) (line []byte, size int64, terminated bool, err error) {
	over := false
	for {
		frag, ferr := r.ReadSlice('\n')
		size += int64(len(frag))
		if !over {
			if len(line)+len(frag) > maxLine+1 { // +1: the newline is not content
				over, line = true, nil
			} else {
				line = append(line, frag...)
			}
		}
		switch {
		case ferr == nil:
			if over {
				return nil, size, true, nil
			}
			return line, size, true, nil
		case errors.Is(ferr, bufio.ErrBufferFull):
			continue
		case errors.Is(ferr, io.EOF):
			return line, size, false, nil
		default:
			return nil, size, false, ferr
		}
	}
}

// wireEnvelope is the JLH as this server writes it: nine keys, nothing else
// (SPEC §3.1). Pointers tell a missing key from a zero.
type wireEnvelope struct {
	UUID    *string         `json:"uuid"`
	From    *string         `json:"from"`
	Space   *string         `json:"space"`
	Type    *string         `json:"type"`
	Level   *uint8          `json:"level"`
	SID     *uint64         `json:"sid"`
	Step    *uint8          `json:"step"`
	Event   *wireEvent      `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

type wireEvent struct {
	UTS *int64  `json:"uts"`
	Sub *int64  `json:"sub"`
	ISO *string `json:"iso"`
}

// ErrLine wraps every refusal of ParseLine.
var ErrLine = errors.New("emergency: not an envelope of this server")

// maxDepth is the deepest nesting a line may have. The converter writes four
// levels at most (envelope, payload, structured-data, one SD element); the
// ceiling only has to stop a corrupt line from reaching a parser's own limit.
const maxDepth = 8

// ParseLine reads one dumped envelope and refuses anything this server could
// not have written (SPEC §12.2). The checks are those of the JLH and of the
// loghub.inbox columns and CHECK constraints the line feeds, and one more: the
// payload the sink will cut out of the line (jlh.Payload) must be the payload
// the JSON parser saw. A line where the two disagree would put one object in
// the queue while its header says another.
//
// The line is reingested WITHOUT conversion, so this function is the only gate
// between a file on disk and the queue. A line the queue refuses is not a
// lost line but a lost CHUNK: the refusal is ClassLogic and drops every
// envelope written in the same INSERT (SPEC §10.2). So nothing that passes may
// be refused by the queue's CHECK constraints or by jsonb itself.
//
// Every refusal wraps ErrLine.
func ParseLine(line []byte) (Record, error) {
	var rec Record
	fail := func(why string) (Record, error) { return Record{}, fmt.Errorf("%w: %s", ErrLine, why) }

	// encoding/json replaces invalid UTF-8 silently; the queue stores what
	// the line says, so the line must already be what jsonb will accept.
	if !utf8.Valid(line) {
		return fail("invalid UTF-8")
	}
	if why := scanLine(line); why != "" {
		return fail(why)
	}
	// The payload is cut by position, as the sink will cut it, and only the
	// header is decoded: decoding the whole line would copy the payload two or
	// three times over, and a line may approach MaxLine.
	p, ok := jlh.Payload(line)
	if !ok {
		return fail("payload is not the last key, as this server writes it")
	}
	head := line[:len(line)-len(p)-1]
	hb := make([]byte, 0, len(head)+3)
	hb = append(append(hb, head...), "{}}"...)
	var w wireEnvelope
	dec := json.NewDecoder(bytes.NewReader(hb))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return fail(err.Error())
	}
	if dec.More() {
		return fail("trailing data")
	}
	if w.UUID == nil || w.From == nil || w.Space == nil || w.Type == nil || w.Level == nil ||
		w.SID == nil || w.Step == nil || w.Event == nil || w.Payload == nil ||
		w.Event.UTS == nil || w.Event.Sub == nil || w.Event.ISO == nil {
		return fail("missing key")
	}

	u, err := uuidv7.FromString(*w.UUID)
	if err != nil || !u.IsValid() || u.String() != *w.UUID {
		return fail("uuid is not a lowercase UUIDv7")
	}
	// from is the queue's from_uuid, whose CHECK demands version 7: the
	// AgentUUID go-loghub-ident makes always is one.
	f, err := uuidv7.FromString(*w.From)
	if !ident.ValidFrom(*w.From) || err != nil || !f.IsValid() {
		return fail("from is not a lowercase UUIDv7")
	}
	if err := ident.ValidateSpace(*w.Space); err != nil {
		return fail("invalid space")
	}
	if *w.Type != syslogType || *w.Step != 0 {
		return fail("not type=syslog at step 0")
	}
	if *w.Level > 7 {
		return fail("level is not a syslog severity")
	}
	if *w.SID > jlh.MaxSID {
		return fail("sid above 2^53-1")
	}
	ev := jlh.Event{UTS: *w.Event.UTS, Sub: *w.Event.Sub}
	if ev.UTS < 0 || ev.UTS > jlh.MaxUTS || ev.Sub < 0 || ev.Sub > 999999999 {
		return fail("event out of range")
	}
	if string(ev.AppendISO(nil)) != *w.Event.ISO {
		return fail("event.iso does not match uts and sub")
	}
	// The uuid and the event are one clock sample (SPEC §3.1). A line where
	// they disagree was not written by this program.
	if t, ok := u.TimestampWithLevel(uuidv7.Level1); !ok || t.UnixMilli() != ev.Time().UnixMilli() {
		return fail("uuid instant does not match the event")
	}
	// The header must be byte for byte the one jlh.AppendHeader writes for
	// these values: key order, number spelling, no whitespace, no duplicate
	// key. encoding/json keeps the LAST of two equal keys and says nothing,
	// so without this a line could carry a second header field, or a second
	// payload before the real one, that no reader of the file would see the
	// same way.
	hdr := jlh.Header{UUID: u, From: *w.From, Space: *w.Space, Type: syslogType,
		Level: *w.Level, SID: *w.SID, Step: 0, Event: ev}
	if canon := jlh.AppendHeader(make([]byte, 0, len(head)), &hdr); !bytes.Equal(canon, head) {
		return fail("the header is not the one this server writes")
	}
	// inbox_syslog_ck: a type=syslog row must carry payload.message, which the
	// converter always writes (SPEC §3.2).
	if why := checkPayload(p); why != "" {
		return fail(why)
	}

	rec = Record{
		UUID:  u,
		From:  *w.From,
		Space: *w.Space,
		Level: *w.Level,
		SID:   *w.SID,
		Event: ev,
		JLH:   line,
	}
	return rec, nil
}

// checkPayload proves the payload is one JSON object whose top-level key
// "message" exists -- the last one, where jsonb keeps the last of equal keys --
// and is a string. It walks the tokens instead of unmarshalling into a map,
// which would copy every value; only the skipped values are buffered, one at a
// time. The key match is exact: encoding/json matches struct fields without
// regard to case, and jsonb's ? does not.
func checkPayload(p []byte) string {
	dec := json.NewDecoder(bytes.NewReader(p))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return "payload is not an object"
	}
	message := false
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return "payload is not valid JSON"
		}
		key, _ := t.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return "payload is not valid JSON"
		}
		if key == "message" {
			message = len(v) > 0 && v[0] == '"'
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return "payload is not valid JSON"
	}
	if _, err := dec.Token(); err != io.EOF {
		return "trailing data after the payload"
	}
	if !message {
		return "payload has no message string"
	}
	return ""
}

// maxNumberDigits is the longest integer the JLH writer emits: a uint64.
const maxNumberDigits = 20

// scanLine checks, in one pass over the bytes, three properties of the JLH
// writer that encoding/json does not:
//
//   - every \u escape is one the writer emits -- \u0000 to \u001f, control
//     characters (SPEC §3.3). That rules out lone surrogates, which
//     encoding/json turns into U+FFFD in silence and jsonb refuses (22P05);
//   - every number is a plain integer of at most twenty digits, the only kind
//     the converter writes (priority, codes, ports, sizes, uts, sid). jsonb
//     refuses 1e1000000 or 1e-16384 with 22003, and accepting one here would
//     lose its whole chunk;
//   - the nesting stays within maxDepth.
//
// A refusal by the queue is a ClassLogic refusal of the WHOLE chunk, so each
// of these would cost every neighbour of the bad line. It returns the reason
// of a refusal, or "".
func scanLine(line []byte) string {
	depth, inString := 0, false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if inString {
			switch c {
			case '"':
				inString = false
			case '\\':
				if i+1 >= len(line) {
					return "dangling escape"
				}
				i++
				if line[i] != 'u' {
					continue
				}
				if i+4 >= len(line) || line[i+1] != '0' || line[i+2] != '0' ||
					(line[i+3] != '0' && line[i+3] != '1') || !isLowerHex(line[i+4]) {
					return `a \u escape the JLH writer never emits`
				}
				i += 4
			}
			continue
		}
		switch {
		case c == '-' || c >= '0' && c <= '9':
			j := i
			if c == '-' {
				j++
			}
			d := j
			for d < len(line) && line[d] >= '0' && line[d] <= '9' {
				d++
			}
			if d == j || d-j > maxNumberDigits ||
				d < len(line) && (line[d] == '.' || line[d] == 'e' || line[d] == 'E') {
				return "a number the JLH writer never emits"
			}
			i = d - 1
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			if depth++; depth > maxDepth {
				return "nested too deep"
			}
		case '}', ']':
			depth--
		}
	}
	return ""
}

func isLowerHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' }

// Remove deletes a fully reingested dump: only after every one of its records
// was confirmed (SPEC §12.2).
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
		return err
	}
	defer d.Close()
	if err := dirSync(d); err != nil {
		return fmt.Errorf("fsync of directory %s: %w", dir, err)
	}
	return nil
}

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
