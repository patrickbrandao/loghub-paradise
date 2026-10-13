package emergency

import (
	"bytes"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/jlhcheck"
)

const agent = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

var base = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// mkItem builds a valid item whose index is recoverable from its clock sample.
// It cycles through the three transports, so a dump holds every payload shape:
// udp with addresses, tcp behind a PROXY header (x-proxy), unix with none.
func mkItem(i int, line string) *item.Item {
	t := base.Add(time.Duration(i) * time.Microsecond)
	h := item.Head{
		UUID:  jlh.NewUUID(t),
		From:  agent,
		Space: "com.acme",
		Size:  len(line),
	}
	h.SetTime(t)
	switch i % 3 {
	case 0:
		h.Transport, h.Framing = item.TransportUDP, item.FramingDatagram
		h.SrcIP, h.SrcPort = netip.MustParseAddr("10.0.0.3"), 59922
		h.DstIP, h.DstPort = netip.MustParseAddr("10.0.0.10"), 1514
	case 1:
		h.Transport, h.Framing = item.TransportTCP, item.FramingOctetCounting
		h.SrcIP, h.SrcPort = netip.MustParseAddr("2001:db8::7"), 40000
		h.DstIP, h.DstPort = netip.MustParseAddr("10.0.0.10"), 1514
		h.PeerIP, h.PeerPort = netip.MustParseAddr("127.0.0.1"), 41000
		h.Size = len(line) + 1000 // truncated on the way in
	default:
		h.Transport, h.Framing = item.TransportUnix, item.FramingDatagram
	}
	return item.New(h, []byte(line), -1)
}

// conv is the app's converter: every envelope carries x-transport "relp"
// (SPEC §3.2, D9), and the dump gate must take what it writes.
func conv() *convert.Converter {
	cfg := convert.DefaultConfig()
	cfg.XTransport = "relp"
	return convert.New(cfg)
}

// lines are the varied raw lines the round trip tests dump: RFC 5424 with SD,
// RFC 3164, NUL and invalid UTF-8 (raw in base64, \u0000 escapes), an embedded
// newline (escaped, never a line break of the file), and a malformed one.
var lines = []string{
	`<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v\"q"] hello`,
	`<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`,
	"linha com bytes crus \x00\xff\xfe e nova linha \n no meio",
	"<13>1 - - app - - - {\"json\":[1,2,{\"x\":\"\\u0041\"}]}",
	"no pri at all, malformed",
}

func dump(t testing.TB, s *Store, items []*item.Item) WriteResult {
	t.Helper()
	res, err := s.Write(items, conv(), true)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// SPEC §12.1: each line is the envelope the worker would have written, byte for
// byte -- the same converter, the same frozen config -- and a valid JLH.
func TestWriteIsOneJLHPerLine(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	var items []*item.Item
	for i, l := range lines {
		items = append(items, mkItem(i, l))
	}
	res := dump(t, s, items)
	if res.Written != len(items) || res.Refused != 0 {
		t.Fatalf("WriteResult = %+v", res)
	}
	b, err := os.ReadFile(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(b, []byte("\n")) {
		t.Fatal("the file does not end with a newline")
	}
	got := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	if len(got) != len(items) {
		t.Fatalf("%d lines for %d items: an envelope held a raw newline", len(got), len(items))
	}
	c := conv()
	for i, it := range items {
		want, _ := c.Append(nil, &it.Head, it.Raw)
		if got[i] != string(want) {
			t.Errorf("line %d is not the worker's envelope:\n got  %s\n want %s", i, got[i], want)
		}
		if err := jlhcheck.Validate([]byte(got[i])); err != nil {
			t.Errorf("line %d: oracle: %v", i, err)
		}
	}
}

// What Read returns is what the queue columns need, parsed from each line's
// own header, and the line itself untouched.
func TestReadReturnsTheColumns(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	var items []*item.Item
	for i, l := range lines {
		items = append(items, mkItem(i, l))
	}
	res := dump(t, s, items)
	rr, err := s.Read(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if rr.Damaged() || len(rr.Records) != len(items) {
		t.Fatalf("clean file: %+v", rr)
	}
	c := conv()
	for i, rec := range rr.Records {
		h := &items[i].Head
		want, cr := c.Append(nil, h, items[i].Raw)
		if rec.UUID != h.UUID || rec.From != h.From || rec.Space != h.Space ||
			rec.Event != (jlh.Event{UTS: h.UTS, Sub: h.Sub}) {
			t.Errorf("record %d head = %+v, want the item's", i, rec)
		}
		if int(rec.Level) != cr.Severity() || rec.SID != cr.SID {
			t.Errorf("record %d level/sid = %d/%d, want %d/%d", i, rec.Level, rec.SID, cr.Severity(), cr.SID)
		}
		if !bytes.Equal(rec.JLH, want) {
			t.Errorf("record %d JLH changed on the way back", i)
		}
	}
}

// syslog.accept_malformed=false: the worker would refuse a malformed message,
// so the dump must not resurrect it (SPEC §6.6, §12.1). Everything refused
// leaves no file at all.
func TestMalformedLeftOutWhenNotAccepted(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	items := []*item.Item{mkItem(0, lines[0]), mkItem(1, "no pri, malformed"), mkItem(2, lines[1])}
	res, err := s.Write(items, conv(), false)
	if err != nil {
		t.Fatal(err)
	}
	if res.Written != 2 || res.Refused != 1 {
		t.Fatalf("WriteResult = %+v, want 2 written and 1 refused", res)
	}
	rr, _ := s.Read(res.Path)
	if len(rr.Records) != 2 || rr.Records[0].UUID != items[0].Head.UUID || rr.Records[1].UUID != items[2].Head.UUID {
		t.Fatalf("records = %+v", rr.Records)
	}

	only, err := NewStore(t.TempDir(), agent).Write([]*item.Item{mkItem(3, "malformed")}, conv(), false)
	if err != nil || only.Path != "" || only.Written != 0 || only.Refused != 1 {
		t.Fatalf("all refused: %+v %v", only, err)
	}
}

func TestFileName(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	name := filepath.Base(p)
	if !strings.HasPrefix(name, "emg-"+agent+"-") || !strings.HasSuffix(name, ".jsonl") {
		t.Fatalf("name = %s", name)
	}
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, "emg-"+agent+"-"), ".jsonl")
	if len(stamp) != 19 {
		t.Fatalf("stamp %q is not nineteen digits; lexicographic order would not be temporal", stamp)
	}
	if got := FileName(agent, time.Unix(0, 1759700000000000000)); got != "emg-"+agent+"-1759700000000000000.jsonl" {
		t.Fatalf("FileName = %s", got)
	}
}

// SPEC §12.2: lexicographic order of name, which is temporal per agent.
func TestListIsLexicographicAndTemporal(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	var paths []string
	for i := 0; i < 3; i++ {
		paths = append(paths, dump(t, s, []*item.Item{mkItem(i, "x")}).Path)
		time.Sleep(2 * time.Millisecond)
	}
	// Noise the listing must ignore, the parent's .bin dumps included: this
	// server cannot read them.
	for _, n := range []string{"emg-x-1.jsonl.corrupt", "notes.txt", "emg-x-2.tmp", "emg-x-3.bin"} {
		os.WriteFile(filepath.Join(s.Dir(), n), []byte("x"), 0o600)
	}
	os.Mkdir(filepath.Join(s.Dir(), "emg-dir-3.jsonl"), 0o750)

	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("listed %d: %v", len(got), got)
	}
	for i := range paths {
		if got[i] != paths[i] {
			t.Fatalf("position %d is %s, want %s", i, filepath.Base(got[i]), filepath.Base(paths[i]))
		}
	}
}

// A truncated file must yield its intact lines and say how much it threw away.
// Losing the whole file because the tail was cut would discard messages an
// emitter was already told we had. The cut line is never delivered, even when
// what is left of it happens to parse: only Write ends a line.
func TestTruncatedFileYieldsItsPrefix(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "a"), mkItem(1, "b"), mkItem(2, "c")}).Path
	full, _ := os.ReadFile(p)

	for name, cut := range map[string]int{
		"mid-line":         len(full) - 10,
		"only the newline": len(full) - 1,
	} {
		t.Run(name, func(t *testing.T) {
			os.WriteFile(p, full[:cut], 0o600)
			res, err := s.Read(p)
			if err != nil {
				t.Fatal(err)
			}
			if !res.Truncated || len(res.Records) != 2 || res.Invalid != 0 {
				t.Fatalf("result = %+v", res)
			}
			last := bytes.LastIndexByte(full[:cut], '\n') + 1
			if res.TruncatedBytes != int64(cut-last) {
				t.Fatalf("TruncatedBytes = %d, want %d", res.TruncatedBytes, cut-last)
			}
			if !res.Damaged() {
				t.Fatal("a truncated file is damaged")
			}
		})
	}
}

// A complete line that is not an envelope is skipped, not the end of the file:
// the newline locates the next one exactly.
func TestInvalidLineIsSkipped(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "a"), mkItem(1, "b")}).Path
	full, _ := os.ReadFile(p)
	first := bytes.IndexByte(full, '\n') + 1
	junk := []byte("{\"uuid\":\"garbage\"}\n\n")
	mixed := append(append(append([]byte{}, full[:first]...), junk...), full[first:]...)
	os.WriteFile(p, mixed, 0o600)

	res, err := s.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 2 || res.Invalid != 2 || res.InvalidBytes != int64(len(junk)) || res.Truncated {
		t.Fatalf("result = %+v", res)
	}
	if !res.Damaged() {
		t.Fatal("a file with invalid lines is damaged")
	}
}

// A line longer than the ceiling is consumed without being held, and the lines
// around it still come back.
func TestOverlongLineIsDiscarded(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "a")}).Path
	good, _ := os.ReadFile(p)
	long := append(bytes.Repeat([]byte("x"), 5000), '\n')
	data := append(append(append([]byte{}, good...), long...), good...)

	res, err := readAll(newReader(bytes.NewReader(data), nil, len(good)+10))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Records) != 2 || res.Invalid != 1 || res.InvalidBytes != int64(len(long)) {
		t.Fatalf("result = %+v", res)
	}
}

// validLine is one envelope as Write produces it, for the refusal table.
func validLine(t testing.TB) string {
	t.Helper()
	it := mkItem(1, `<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] hello`)
	b, _ := conv().Append(nil, &it.Head, it.Raw)
	return string(b)
}

// ParseLine is the only gate between a file on disk and the queue columns
// (SPEC §12.2): each mutation is a line this server never writes, and some of
// them would make the queue refuse the whole chunk (inbox CHECKs, jsonb). The
// control case proves the table does not pass by refusing everything.
func TestParseLineRefusals(t *testing.T) {
	ok := validLine(t)
	if _, err := ParseLine([]byte(ok)); err != nil {
		t.Fatalf("control line refused: %v", err)
	}
	rec, _ := ParseLine([]byte(ok))
	u := rec.UUID.String()
	v4 := "4b8b5b6e-3f1a-4c2d-9e0f-1a2b3c4d5e6f"
	other := uuidv7.GenerateAt(uuidv7.Level3, base.Add(time.Hour)).String()

	sub := func(old, new string) string {
		if !strings.Contains(ok, old) {
			t.Fatalf("mutation anchor %q is not in the control line", old)
		}
		return strings.Replace(ok, old, new, 1)
	}
	cases := map[string]string{
		"empty":                   "",
		"not json":                "hello",
		"trailing data":           ok + " {}",
		"invalid utf-8":           sub(`hello`, "hel\xfflo"),
		"extra key":               sub(`{"uuid":`, `{"x":1,"uuid":`),
		"missing from":            sub(`"from":"`+agent+`",`, ``),
		"uppercase uuid":          sub(u, strings.ToUpper(u)),
		"uuid v4":                 sub(u, v4),
		"uuid of another instant": sub(u, other),
		"from v4":                 sub(`"from":"`+agent, `"from":"`+v4),
		"bad space":               sub(`"space":"com.acme"`, `"space":"com..acme"`),
		"type webhook":            sub(`"type":"syslog"`, `"type":"webhook"`),
		"step 1":                  sub(`"step":0`, `"step":1`),
		"level 8":                 sub(`"level":5`, `"level":8`),
		"level negative":          sub(`"level":5`, `"level":-1`),
		"sid above 2^53":          strings.Replace(ok, `"sid":`+itoa(rec.SID), `"sid":9007199254740992`, 1),
		"iso mismatch":            sub(`T12:00:00.000Z`, `T12:00:01.000Z`),
		"sub out of range":        sub(`"sub":1000`, `"sub":1000000000`),
		"payload not last":        sub(`"step":0,`, `"step":0,"payload":{"message":"x"},`),
		"duplicate level":         sub(`"level":5,`, `"level":5,"level":5,`),
		"whitespace":              sub(`"type":"syslog"`, `"type": "syslog"`),
		"sid with a plus":         strings.Replace(ok, `"sid":`, `"sid":+`, 1),
		"payload without message": sub(`,"message":"hello"`, ``),
		"lone surrogate":          sub(`hello`, `hel\ud800lo`),
		"escape never written":    sub(`hello`, `hel\u0041lo`),
		"uppercase escape":        strings.Replace(ok, `}}`, `,"x":"\u001F"}}`, 1),
		"too deep":                strings.Replace(ok, `}}`, `,"x":[[[[[[[[1]]]]]]]]}}`, 1),
		// Review A #2: jsonb refuses these with 22003, which would drop the
		// whole chunk; the converter only writes plain integers.
		"huge exponent":         strings.Replace(ok, `}}`, `,"n":1e1000000}}`, 1),
		"tiny exponent":         strings.Replace(ok, `}}`, `,"n":1e-16384}}`, 1),
		"fraction":              strings.Replace(ok, `}}`, `,"n":1.5}}`, 1),
		"21 digits":             strings.Replace(ok, `}}`, `,"n":123456789012345678901}}`, 1),
		"bare minus":            strings.Replace(ok, `}}`, `,"n":-}}`, 1),
		"MESSAGE in upper case": sub(`,"message":"hello"`, `,"MESSAGE":"hello"`),
		"message not a string":  sub(`,"message":"hello"`, `,"message":1`),
		// jsonb keeps the last of two equal keys: the last "message" counts.
		"last message number": ok[:len(ok)-2] + `,"message":2}}`,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if line == ok {
				t.Fatal("the mutation changed nothing")
			}
			_, err := ParseLine([]byte(line))
			if !errors.Is(err, ErrLine) {
				t.Fatalf("accepted (err=%v):\n%s", err, line)
			}
		})
	}
}

func itoa(n uint64) string { return string(jlhAppendUint(nil, n)) }

func jlhAppendUint(dst []byte, n uint64) []byte {
	if n >= 10 {
		dst = jlhAppendUint(dst, n/10)
	}
	return append(dst, byte('0'+n%10))
}

// What the converter writes for hostile input -- NUL, controls, invalid UTF-8,
// every printable ASCII -- always passes the gate: a dump of real traffic is
// never quarantined by its own reader.
func TestParseLineAcceptsWhatTheConverterWrites(t *testing.T) {
	var ascii []byte
	for c := 0; c < 0x80; c++ {
		ascii = append(ascii, byte(c))
	}
	for i, raw := range []string{
		string(ascii),
		"<13>1 - h a p m [x@1 k=\"\\\"\\\\\\]\"] \x00\x01\x1f \xff\xfe",
		"<0>Oct  1 00:00:00 host tag: message",
	} {
		it := mkItem(i, raw)
		b, _ := conv().Append(nil, &it.Head, it.Raw)
		if _, err := ParseLine(b); err != nil {
			t.Errorf("%d: %v\n%s", i, err, b)
		}
	}
}

// Quarantine keeps the evidence and takes it out of the next boot's way.
func TestQuarantineKeepsTheEvidence(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	dst, err := s.Quarantine(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(dst, ".jsonl.corrupt") {
		t.Fatalf("quarantined as %s", dst)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("quarantined file is gone: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("the original still exists")
	}
	if list, _ := s.List(); len(list) != 0 {
		t.Fatalf("List returned a quarantined file: %v", list)
	}
}

func TestRemove(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	if err := s.Remove(p); err != nil {
		t.Fatal(err)
	}
	if list, _ := s.List(); len(list) != 0 {
		t.Fatalf("listed after Remove: %v", list)
	}
}

// O_EXCL: a dump never appends to, or truncates, a file that already exists.
func TestWriteNeverOverwrites(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	first := mkItem(0, "first")
	p := dump(t, s, []*item.Item{first}).Path
	if _, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640); !os.IsExist(err) {
		t.Fatalf("O_EXCL open of an existing dump: %v", err)
	}
	res, _ := s.Read(p)
	if len(res.Records) != 1 || res.Records[0].UUID != first.Head.UUID {
		t.Fatal("dump changed")
	}
}

// A directory fsync that fails fails the dump. Swallowed, a crash right after
// could leave the data on disk and its name not, while the process exits
// believing the messages were saved (SPEC §12.1, §12.3). The partial file is
// removed, as on every other write error.
func TestWriteFailsWhenTheDirectoryFsyncFails(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	orig := dirSync
	t.Cleanup(func() { dirSync = orig })
	dirSync = func(*os.File) error { return errors.New("input/output error") }

	res, err := s.Write([]*item.Item{mkItem(0, "x")}, conv(), true)
	if err == nil || res.Path != "" || res.Written != 0 {
		t.Fatalf("Write = %+v %v, want an error", res, err)
	}
	if !strings.Contains(err.Error(), "input/output error") {
		t.Fatalf("err = %v, want the fsync cause", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a failed dump left %d files behind", len(entries))
	}

	// Quarantine and Remove report it too; the rename and the unlink already
	// happened.
	dirSync = orig
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	q := dump(t, s, []*item.Item{mkItem(1, "y")}).Path
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if dst, err := s.Quarantine(p); err == nil || dst == "" {
		t.Fatalf("Quarantine = %q %v, want the path and the fsync error", dst, err)
	}
	if err := s.Remove(q); err == nil {
		t.Fatal("Remove swallowed the directory fsync error")
	}
}

// A file system without hard links (vfat, exFAT, the object-store FUSE
// mounts) refuses Link whatever the names: every dump failed there, the whole
// ring lost, where writing in place had worked. The fallback renames after
// checking the final name is free (SPEC §12.1). It never renames over an
// existing dump, a Link refused because the name exists is not a reason to
// try, and the directory fsync still decides the renamed dump as it decides a
// linked one (SPEC §12.3).
func TestWriteOnAFileSystemWithoutHardLinks(t *testing.T) {
	defer func(orig func(string, string) error) { link = orig }(link)
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: syscall.ENOTSUP} }

	dir := t.TempDir()
	s := NewStore(dir, agent)
	first := mkItem(0, lines[0])
	res := dump(t, s, []*item.Item{first, mkItem(1, lines[1])})
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(res.Path) {
		t.Fatalf("directory holds %v, want only the dump", entries)
	}
	got, err := s.Read(res.Path)
	if err != nil || len(got.Records) != 2 || got.Damaged() || got.Records[0].UUID != first.Head.UUID {
		t.Fatalf("the renamed dump reads %+v, err %v", got, err)
	}

	// The directory fsync still decides: failed, the renamed dump goes too.
	orig := dirSync
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if _, err := s.Write([]*item.Item{mkItem(2, "x")}, conv(), true); err == nil {
		t.Error("a renamed dump whose directory fsync failed was reported saved")
	}
	dirSync = orig
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("a failed renamed dump left %d entries, want only the first dump", len(entries))
	}

	// The fallback never replaces a dump that has the final name.
	tmp := filepath.Join(dir, ".emg-x-1.jsonl.tmp")
	final := filepath.Join(dir, "emg-x-1.jsonl")
	if err := os.WriteFile(tmp, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publish(tmp, final); err == nil {
		t.Error("the fallback renamed over an existing dump")
	}
	if b, _ := os.ReadFile(final); string(b) != "old\n" {
		t.Errorf("the existing dump now holds %q", b)
	}
	// A Link refused because the name exists is final: no rename is tried.
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: os.ErrExist} }
	other := filepath.Join(dir, "emg-x-2.jsonl")
	if err := publish(tmp, other); !errors.Is(err, os.ErrExist) {
		t.Errorf("a Link refused for an existing name fell back to a rename: %v", err)
	}
	if _, err := os.Stat(other); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s exists after a refused Link: %v", other, err)
	}
}

// SPEC §12.1: a temporary that is the same file as a dump under its final
// name is the twin of a crash between link and removal, and goes; any other is
// an interrupted dump, reported and left alone.
func TestLeftovers(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "emergency"), agent)
	if left, err := s.Leftovers(); err != nil || len(left) != 0 {
		t.Fatalf("Leftovers of a missing directory = %v, %v", left, err)
	}
	path := dump(t, s, []*item.Item{mkItem(0, lines[0])}).Path
	twin := filepath.Join(s.Dir(), "."+filepath.Base(path)+tmpSuffix)
	if err := os.Link(path, twin); err != nil {
		t.Fatal(err)
	}
	cut := filepath.Join(s.Dir(), "."+FileName(agent, base.Add(time.Hour))+tmpSuffix)
	if err := os.WriteFile(cut, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A temporary whose final name holds ANOTHER file is not a twin.
	otherFinal := filepath.Join(s.Dir(), FileName(agent, base.Add(2*time.Hour)))
	other := filepath.Join(s.Dir(), "."+filepath.Base(otherFinal)+tmpSuffix)
	if err := os.WriteFile(other, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(otherFinal, []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	left, err := s.Leftovers()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(left)
	want := []string{cut, other}
	sort.Strings(want)
	if !reflect.DeepEqual(left, want) {
		t.Errorf("Leftovers = %v, want %v", left, want)
	}
	if _, err := os.Stat(twin); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the twin is still there: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the dump went with its twin: %v", err)
	}
}

func TestEmptyWriteCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	res, err := NewStore(dir, agent).Write(nil, conv(), true)
	if err != nil || res.Path != "" {
		t.Fatalf("Write(nil) = %+v %v", res, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("an empty dump created %d files", len(entries))
	}
}

func TestMissingDirIsNotAnError(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "nope"), agent)
	list, err := s.List()
	if err != nil || len(list) != 0 {
		t.Fatalf("List on a missing dir = %v %v", list, err)
	}
}

// SPEC §12.3: the directory is created at boot, and an unwritable one is found
// then, not during the incident.
func TestPrepare(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "emergency")
	s := NewStore(dir, agent)
	if err := s.Prepare(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("dir not created: %v", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("the probe was left behind: %v", entries)
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes anywhere")
	}
	ro := filepath.Join(t.TempDir(), "ro")
	os.Mkdir(ro, 0o500)
	if err := NewStore(ro, agent).Prepare(); err == nil {
		t.Fatal("Prepare accepted a read-only directory")
	}
}

func TestSanitize(t *testing.T) {
	for in, want := range map[string]string{
		"":      "agent",
		agent:   agent, // a canonical AgentUUID passes through unchanged
		"a/b:c": "a_b_c",
		"ok_1":  "ok_1",
		"../x":  "___x",
	} {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

// FuzzRead is mandatory (SPEC §18.3). The reader takes a file a crashing disk
// may have half written, so it must tolerate any byte sequence without
// panicking, account for every byte, and return only records the gate accepts
// again unchanged: what comes back is what goes to the queue.
func FuzzRead(f *testing.F) {
	s := NewStore(f.TempDir(), agent)
	res, err := s.Write([]*item.Item{
		mkItem(0, `<165>1 2026-09-12T14:33:01Z h a 1 - - msg`),
		mkItem(1, "\x00\xff raw"),
		mkItem(2, "unix"),
	}, conv(), true)
	if err != nil {
		f.Fatal(err)
	}
	valid, _ := os.ReadFile(res.Path)
	f.Add(valid)
	f.Add(valid[:len(valid)/2])
	f.Add([]byte(""))
	f.Add([]byte("\n"))
	f.Add([]byte("{}\n"))
	f.Add([]byte(`{"uuid":"\ud800"}` + "\n"))
	f.Add(append([]byte("[[[[[[[[[[\n"), valid...))
	f.Add(bytes.Repeat([]byte("x"), 300))

	const maxLine = 4096
	f.Fuzz(func(t *testing.T, data []byte) {
		res, err := readAll(newReader(bytes.NewReader(data), nil, maxLine))
		if err != nil {
			t.Fatalf("reading an in-memory input returned %v", err)
		}
		var kept int64
		for _, rec := range res.Records {
			if bytes.IndexByte(rec.JLH, '\n') >= 0 || len(rec.JLH) > maxLine {
				t.Fatal("a record holds a newline or passes the line ceiling")
			}
			again, err := ParseLine(rec.JLH)
			if err != nil || again.UUID != rec.UUID || again.SID != rec.SID || again.Level != rec.Level {
				t.Fatalf("a returned record does not pass the gate again: %v", err)
			}
			if _, ok := jlh.Payload(rec.JLH); !ok {
				t.Fatal("a returned record has no payload the sink can cut")
			}
			kept += int64(len(rec.JLH)) + 1
		}
		// Every byte is either a kept line, an invalid one or the cut tail.
		if kept+res.InvalidBytes+res.TruncatedBytes != int64(len(data)) {
			t.Fatalf("accounted %d+%d+%d bytes of %d", kept, res.InvalidBytes, res.TruncatedBytes, len(data))
		}
		// Chunked reading returns the same records in the same order.
		r := newReader(bytes.NewReader(data), nil, maxLine)
		n := 0
		for {
			chunk, err := r.Next(2, 600)
			if err != nil {
				t.Fatal(err)
			}
			if len(chunk) == 0 {
				break
			}
			for _, rec := range chunk {
				if n >= len(res.Records) || rec.UUID != res.Records[n].UUID {
					t.Fatal("chunked reading disagrees with whole reading")
				}
				n++
			}
		}
		if n != len(res.Records) || r.Result().Invalid != res.Invalid || r.Result().Truncated != res.Truncated {
			t.Fatalf("chunked reading: %d records, %+v; whole: %d, %+v", n, r.Result(), len(res.Records), res)
		}
	})
}

// Review A #1: the boot streams a dump. Next honours both chunk limits, an
// oversized record comes alone, nothing is skipped or reordered, and the end
// is an empty chunk with the skip counts in Result.
func TestNextChunksTheFile(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	var items []*item.Item
	for i := 0; i < 7; i++ {
		items = append(items, mkItem(i, strings.Repeat("x", 10+i)))
	}
	items = append(items, mkItem(7, strings.Repeat("big", 400)))
	p := dump(t, s, items).Path
	whole, _ := s.Read(p)

	r, err := s.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	small := len(whole.Records[0].JLH)
	var sizes []int
	var got []Record
	for {
		chunk, err := r.Next(3, 2*small+small/2)
		if err != nil {
			t.Fatal(err)
		}
		if len(chunk) == 0 {
			break
		}
		sizes = append(sizes, len(chunk))
		got = append(got, chunk...)
	}
	if len(got) != len(items) {
		t.Fatalf("read %d records of %d", len(got), len(items))
	}
	for i := range got {
		if got[i].UUID != items[i].Head.UUID {
			t.Fatalf("record %d out of order", i)
		}
	}
	// Two records fit the byte budget, never three; the big one goes alone.
	for i, n := range sizes {
		if n > 2 {
			t.Fatalf("chunk %d holds %d records: the byte budget was ignored (sizes %v)", i, n, sizes)
		}
	}
	if last := sizes[len(sizes)-1]; last != 1 {
		t.Fatalf("the oversized record did not come alone: sizes %v", sizes)
	}
	if r.Result().Damaged() {
		t.Fatalf("clean file reported damaged: %+v", r.Result())
	}
}

// Review A #4: a dump is written under a hidden temporary name and renamed
// when complete, so List never sees a file still being written; a failure
// leaves neither name behind.
func TestWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p := dump(t, s, []*item.Item{mkItem(0, "a")}).Path
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(p) {
		t.Fatalf("directory holds %v, want only the final dump", entries)
	}
	// A left-over temporary file, from a dump cut short, is never listed.
	os.WriteFile(filepath.Join(dir, "."+FileName(agent, time.Now())+tmpSuffix), []byte("{}\n"), 0o600)
	if list, _ := s.List(); len(list) != 1 || list[0] != p {
		t.Fatalf("List = %v", list)
	}
}
