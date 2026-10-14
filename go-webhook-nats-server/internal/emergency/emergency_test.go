package emergency

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
)

// testAgent is the AgentUUID the dumps are named after (SPEC §9.1). It is also
// the envelope's from, as it is in the server.
const testAgent = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// jlh builds a whole webhook envelope around body, the bytes a memory
// ring holds and the dump writes.
func jlh(body []byte) (id string, env []byte) {
	now := time.Now()
	h := envelope.Header{UUID: envelope.NewUUID(now), From: testAgent, Space: "default",
		Type: envelope.Type, Level: envelope.Level, SID: 1, Event: envelope.EventAt(now)}
	r := &envelope.Request{Mode: envelope.ModeFast, Src: netip.MustParseAddr("203.0.113.9"),
		Dst: netip.MustParseAddr("127.0.0.1"), SPort: 5555, DPort: 8080,
		PeerAddr: netip.MustParseAddr("203.0.113.9"), PeerPort: 5555, SrcSource: "tcp",
		Proto: "HTTP/1.1", Method: "POST", Host: "example.com", Path: "/x", RawQuery: "a=1",
		Header: http.Header{"Content-Type": {"application/json"}}, HostHeader: "example.com",
		ContentType: "application/json", Body: body}
	return h.UUID.String(), envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(body))
}

func item(body []byte) *envelope.Item {
	id, env := jlh(body)
	return envelope.NewItem(id, env, -1)
}

// maxLine is the line ceiling the tests give the store: the largest envelope
// of the default configuration (5 MiB body, 64 KiB headers, 8 KiB URI), as
// the app derives it (SPEC §9.2).
var maxLine = envelope.MaxEventBytes(5<<20, 64<<10, 8<<10)

func newStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(t.TempDir(), testAgent, maxLine)
}

// write is Write for the tests that expect every item to make it in.
func write(t *testing.T, s *Store, items []*envelope.Item) string {
	t.Helper()
	path, skipped, err := s.Write(items)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if skipped != 0 {
		t.Fatalf("Write left %d items out", skipped)
	}
	return path
}

// lines joins envelopes the way the dump writes them.
func lines(envs ...[]byte) []byte {
	var b []byte
	for _, e := range envs {
		b = append(b, e...)
		b = append(b, '\n')
	}
	return b
}

// dumpFile writes raw bytes as a dump the store will list.
func dumpFile(t *testing.T, s *Store, data []byte) string {
	t.Helper()
	p := filepath.Join(s.Dir(), fmt.Sprintf("emg-%s-%d%s", testAgent, time.Now().UnixNano(), fileSuffix))
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// ------------------------------------------------------------------ the gate

// SPEC §9.2: a line goes to NATS without conversion, and NATS never reads Data.
// So a line that passes ParseHead -- a JLH in this server's header shape -- is
// still refused unless it is a JLH of THIS server: type webhook, level 9,
// step 0, a payload opened by mode fast. A go-syslog-* dump dropped in the
// directory has the same file name. Each spoiled line is checked to pass
// ParseHead, or its refusal would prove nothing about the gate.
func TestGateRefusesEnvelopesOfAnotherServer(t *testing.T) {
	_, good := jlh([]byte(`{"ok":1}`))
	spoil := map[string]func([]byte) []byte{
		"type syslog": func(e []byte) []byte {
			return bytes.Replace(e, []byte(`"type":"webhook"`), []byte(`"type":"syslog"`), 1)
		},
		"type webhook.request": func(e []byte) []byte {
			return bytes.Replace(e, []byte(`"type":"webhook"`), []byte(`"type":"webhook.request"`), 1)
		},
		"level 8": func(e []byte) []byte { return bytes.Replace(e, []byte(`"level":9`), []byte(`"level":8`), 1) },
		"step 1":  func(e []byte) []byte { return bytes.Replace(e, []byte(`"step":0`), []byte(`"step":1`), 1) },
		"mode bucket": func(e []byte) []byte {
			return bytes.Replace(e, []byte(`{"mode":"fast",`), []byte(`{"mode":"bucket",`), 1)
		},
		"payload without mode first": func(e []byte) []byte {
			return bytes.Replace(e, []byte(`{"mode":"fast",`), []byte(`{`), 1)
		},
	}
	// A beacon is a JLH with this server's header writer, and no webhook.
	now := time.Now()
	beacon := envelope.AppendHeader(nil, &envelope.Header{UUID: envelope.NewUUID(now), From: testAgent,
		Space: "default", Type: "beacon", Level: 9, SID: 1, Event: envelope.EventAt(now)})
	beacon = envelope.Close(append(beacon, `{"name":"x"}`...))

	bad := [][]byte{beacon}
	for name, f := range spoil {
		e := f(good)
		if bytes.Equal(e, good) {
			t.Fatalf("%s: the spoiler changed nothing", name)
		}
		if _, _, err := envelope.ParseHead(e); err != nil {
			t.Fatalf("%s: the spoiled line fails ParseHead (%v); the gate would not be what refuses it", name, err)
		}
		bad = append(bad, e)
	}
	if _, _, err := envelope.ParseHead(beacon); err != nil {
		t.Fatalf("the beacon fails ParseHead: %v", err)
	}

	s := newStore(t)
	data := lines(good)
	for _, b := range bad {
		data = append(data, lines(b, good)...)
	}
	res, err := s.Read(dumpFile(t, s, data))
	if err != nil {
		t.Fatal(err)
	}
	if res.DamagedLines != len(bad) {
		t.Errorf("%d damaged lines, want the %d of other servers", res.DamagedLines, len(bad))
	}
	if len(res.Items) != len(bad)+1 {
		t.Fatalf("%d items, want the %d good lines around the damage", len(res.Items), len(bad)+1)
	}
	for i, it := range res.Items {
		if !bytes.Equal(it.Env, good) {
			t.Errorf("item %d is not the good line", i)
		}
	}
}

// SPEC §9.1: the dump is written under a hidden temporary name and linked to
// the final one complete; nothing of the temporary is left behind, and a
// temporary a crash left is never listed, read or removed.
func TestWriteGoesThroughATemporaryName(t *testing.T) {
	s := newStore(t)
	path := write(t, s, []*envelope.Item{item([]byte("a")), item([]byte("b"))})
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("directory holds %v, want only %s", names, filepath.Base(path))
	}

	// A temporary a crash left behind, cut at a line boundary: it would read
	// clean, which is exactly why it must not be taken for a dump.
	_, env := jlh([]byte("half"))
	tmp := filepath.Join(s.Dir(), "."+fmt.Sprintf("emg-%s-%d%s", testAgent, time.Now().UnixNano(), fileSuffix)+tmpSuffix)
	if err := os.WriteFile(tmp, lines(env), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != path {
		t.Errorf("List = %v, want only %s", files, path)
	}
	if n, events, _ := s.Count(); n != 1 || events != 2 {
		t.Errorf("Count = %d files %d events, want the dump's 1 and 2", n, events)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Errorf("the leftover temporary was touched: %v", err)
	}
}

// A file system without hard links (vfat, exFAT, object-store FUSE mounts)
// refuses Link whatever the names. Every dump failed there, the whole ring
// lost; now the free name is checked and the file renamed. A Link refused
// because the name is taken still refuses, and so does the fallback when the
// final name exists: never over another dump.
func TestWriteOnAFileSystemWithoutHardLinks(t *testing.T) {
	defer func(orig func(string, string) error) { link = orig }(link)
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: syscall.ENOTSUP} }

	s := newStore(t)
	path := write(t, s, []*envelope.Item{item([]byte("a")), item([]byte("b"))})
	entries, _ := os.ReadDir(s.Dir())
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("directory holds %d entries, want only the dump", len(entries))
	}
	res, err := s.Read(path)
	if err != nil || len(res.Items) != 2 || res.Damaged() {
		t.Fatalf("the renamed dump reads %v items, err %v", res, err)
	}

	// The fallback never replaces a dump that has the final name.
	tmp := filepath.Join(s.Dir(), ".emg-x-1.jsonl.tmp")
	final := filepath.Join(s.Dir(), "emg-x-1.jsonl")
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
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: os.ErrExist} }
	if err := publish(tmp, filepath.Join(s.Dir(), "emg-x-2.jsonl")); !errors.Is(err, os.ErrExist) {
		t.Errorf("a Link refused for an existing name fell back to a rename: %v", err)
	}
}

// A directory that can be written but not read (0300) cannot be opened for
// its fsync. Write removed the complete, synced, linked dump for that.
func TestWriteKeepsTheDumpWhenTheDirectoryCannotBeOpened(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads any directory")
	}
	s := newStore(t)
	if err := os.MkdirAll(s.Dir(), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(s.Dir(), 0o300); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(s.Dir(), 0o750)
	path, _, err := s.Write([]*envelope.Item{item([]byte("a"))})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the dump is gone: %v", err)
	}
}

// Only regular files are dumps. A FIFO named like one blocked os.Open
// forever, and the boot with it; a symlink could point at /dev/zero.
func TestListSkipsWhatIsNotARegularFile(t *testing.T) {
	s := newStore(t)
	path := write(t, s, []*envelope.Item{item([]byte("a"))})
	fifo := filepath.Join(s.Dir(), fmt.Sprintf("emg-%s-1%s", testAgent, fileSuffix))
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	if err := os.Symlink("/dev/zero", filepath.Join(s.Dir(), fmt.Sprintf("emg-%s-2%s", testAgent, fileSuffix))); err != nil {
		t.Fatal(err)
	}
	files, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != path {
		t.Fatalf("List = %v, want only %s", files, path)
	}
}

// A temporary that is the same file as a dump under its final name is the
// twin of a crash between link and removal, and goes; any other is an
// interrupted dump, reported and left alone (SPEC §9.1).
func TestLeftovers(t *testing.T) {
	s := newStore(t)
	if left, err := s.Leftovers(); err != nil || len(left) != 0 {
		t.Fatalf("Leftovers of a missing directory = %v, %v", left, err)
	}
	path := write(t, s, []*envelope.Item{item([]byte("a"))})
	twin := filepath.Join(s.Dir(), "."+filepath.Base(path)+tmpSuffix)
	if err := os.Link(path, twin); err != nil {
		t.Fatal(err)
	}
	cut := filepath.Join(s.Dir(), fmt.Sprintf(".emg-%s-7%s%s", testAgent, fileSuffix, tmpSuffix))
	if err := os.WriteFile(cut, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A temporary whose final name holds ANOTHER file is not a twin.
	other := filepath.Join(s.Dir(), fmt.Sprintf(".emg-%s-8%s%s", testAgent, fileSuffix, tmpSuffix))
	if err := os.WriteFile(other, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.Dir(), fmt.Sprintf("emg-%s-8%s", testAgent, fileSuffix)), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}
	left, err := s.Leftovers()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(left)
	if want := []string{cut, other}; !reflect.DeepEqual(left, want) {
		t.Errorf("Leftovers = %v, want %v", left, want)
	}
	if _, err := os.Stat(twin); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the twin is still there: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the dump went with its twin: %v", err)
	}
}

// ------------------------------------------------------------------ round-trip

func TestRoundTrip(t *testing.T) {
	s := newStore(t)

	// A webhook body is arbitrary binary, and it travels inside the JLH as
	// escaped text or as base64. Neither ever puts a raw newline in the line:
	// a body full of them is the case that matters most for JSON Lines.
	bodies := [][]byte{
		[]byte("plain"),
		{0x00, 0x0a, 0xff, 0x0d, 0x1a},
		[]byte(""),
		bytes.Repeat([]byte("x"), 100_000),
		[]byte("line one\nline two\r\n\n"),
		[]byte("   separators JavaScript treats as newlines"),
	}
	want := make([]*envelope.Item, len(bodies))
	for i, b := range bodies {
		want[i] = item(b)
	}

	path := write(t, s, want)

	res, err := s.Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if res.Damaged() {
		t.Fatalf("a file we just wrote has %d damaged lines", res.DamagedLines)
	}
	if len(res.Items) != len(want) {
		t.Fatalf("got %d items, want %d", len(res.Items), len(want))
	}
	for i, got := range res.Items {
		if !bytes.Equal(got.Env, want[i].Env) {
			t.Errorf("item %d: the JLH differs", i)
		}
		if got.ID != want[i].ID {
			t.Errorf("item %d id = %q, want %q", i, got.ID, want[i].ID)
		}
		// The reingested envelope is delivered as is, so it must still be the
		// contract the destination expects, and carry the exact body.
		e, err := jlhcheck.Validate(got.Env)
		if err != nil {
			t.Errorf("item %d: %v", i, err)
			continue
		}
		if body, err := e.Payload.Body(); err != nil || !bytes.Equal(body, bodies[i]) {
			t.Errorf("item %d: body %q (%v), want %q", i, body, err, bodies[i])
		}
	}
}

// SPEC §9.1: the file IS JSON Lines -- one envelope per line, each line a JSON
// object, every line ended by a newline, nothing else -- so any line tool
// reads it.
func TestFileIsJSONLines(t *testing.T) {
	s := newStore(t)
	items := []*envelope.Item{item([]byte("a\nb")), item([]byte{0, 1, 2}), item(nil)}
	path := write(t, s, items)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(raw, []byte("\n")) {
		t.Fatal("the file does not end with a newline")
	}
	got := bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n"))
	if len(got) != len(items) {
		t.Fatalf("%d lines, want one per envelope (%d)", len(got), len(items))
	}
	for i, line := range got {
		if !bytes.Equal(line, items[i].Env) {
			t.Errorf("line %d is not the envelope byte for byte", i)
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(line, &obj); err != nil {
			t.Errorf("line %d is not a JSON object: %v", i, err)
		}
	}
}

// The file carries the JLH alone, so a reingested item's identity has to be
// rebuilt from the JLH: a sink fills its columns from what Read returns, and
// an item that came back with an empty id would collide on the queue's key.
// Here the item is written with no id at all, so only the envelope can supply
// it.
func TestReadRebuildsTheIdentityFromTheJLH(t *testing.T) {
	s := newStore(t)
	id, env := jlh([]byte(`{"n":1}`))

	path := write(t, s, []*envelope.Item{envelope.NewItem("", env, -1)})
	res, err := s.Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("got %d items, want 1", len(res.Items))
	}
	if got := res.Items[0].ID; got != id {
		t.Errorf("reingested id = %q, want the envelope's uuid %q", got, id)
	}
	if res.Items[0].Size() != int64(len(env)) {
		t.Errorf("Size = %d, want len(JLH) = %d", res.Items[0].Size(), len(env))
	}
}

func TestWriteEmptyIsNoFile(t *testing.T) {
	s := newStore(t)
	path, skipped, err := s.Write(nil)
	if err != nil || path != "" || skipped != 0 {
		t.Fatalf("Write(nil) = %q, %d, %v; want no file", path, skipped, err)
	}
	files, _ := s.List()
	if len(files) != 0 {
		t.Errorf("an empty dump created %d files", len(files))
	}
}

func TestWriteCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "nested")
	s := NewStore(dir, testAgent, maxLine)
	write(t, s, []*envelope.Item{item([]byte("x"))})
}

// SPEC §9.1, the framing defence: an envelope with a raw 0x0A would split its
// line and spoil the next. The serializer never makes one, so the item here is
// forged; the dump leaves it OUT, reports it, and writes every other item.
func TestWriteLeavesOutAnEnvelopeWithARawNewline(t *testing.T) {
	s := newStore(t)
	good1, good2 := item([]byte("a")), item([]byte("b"))
	_, env := jlh([]byte("c"))
	forged := append([]byte(nil), env...)
	forged[len(forged)-1] = '\n' // the closing brace of the envelope, now a newline
	bad := envelope.NewItem("forged", forged, -1)

	path, skipped, err := s.Write([]*envelope.Item{good1, bad, good2})
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 1 {
		t.Errorf("skipped = %d, want the one forged item", skipped)
	}
	res, err := s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Damaged() || len(res.Items) != 2 || res.Items[0].ID != good1.ID || res.Items[1].ID != good2.ID {
		t.Errorf("read back %d items (damaged %d), want exactly the two good ones in order",
			len(res.Items), res.DamagedLines)
	}

	// Nothing writable at all: no file, and the loss is still reported.
	path, skipped, err = s.Write([]*envelope.Item{bad})
	if err != nil || path != "" || skipped != 1 {
		t.Errorf("only forged items: path %q skipped %d err %v; want no file and 1 skipped", path, skipped, err)
	}
}

// SPEC §9.1: emg-<agent_uuid>-<unixnano>.jsonl. The AgentUUID says whose dump
// a file is when several instances share a volume, and it must survive
// intact: a uuid is already filename-safe, hyphens included.
func TestFileNameCarriesTheAgentUUID(t *testing.T) {
	s := newStore(t)
	before := time.Now().UnixNano()
	path := write(t, s, []*envelope.Item{item([]byte("x"))})
	after := time.Now().UnixNano()

	name := filepath.Base(path)
	m := regexp.MustCompile(`^emg-(.+)-([0-9]+)\.jsonl$`).FindStringSubmatch(name)
	if m == nil {
		t.Fatalf("file name %q is not emg-<agent_uuid>-<unixnano>.jsonl", name)
	}
	if m[1] != testAgent {
		t.Errorf("agent part = %q, want the AgentUUID %q", m[1], testAgent)
	}
	ns, _ := strconv.ParseInt(m[2], 10, 64)
	if ns < before || ns > after {
		t.Errorf("timestamp %d outside the write [%d, %d]", ns, before, after)
	}
	if got := tsOf(path, fileSuffix); got != ns {
		t.Errorf("tsOf = %d, want %d: the hyphens of the uuid must not confuse it", got, ns)
	}
}

// ------------------------------------------------------------------- ordering

func TestListIsChronological(t *testing.T) {
	s := newStore(t)
	var paths []string
	for i := 0; i < 5; i++ {
		paths = append(paths, write(t, s, []*envelope.Item{item([]byte("y"))}))
	}
	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(paths) {
		t.Fatalf("List returned %d files, want %d", len(got), len(paths))
	}
	for i := range paths {
		if got[i] != paths[i] {
			t.Errorf("position %d: got %s, want %s", i, filepath.Base(got[i]), filepath.Base(paths[i]))
		}
	}
}

// Ordering must come from the timestamp, not from how the agent uuid happens
// to sort, or two instances writing into a shared volume would interleave
// wrongly.
func TestListOrdersByTimestampNotAgentUUID(t *testing.T) {
	dir := t.TempDir()
	// The first agent sorts after the second lexicographically but writes first.
	first := write(t, NewStore(dir, "ffffffff-07bc-70b4-ae05-c57e7541d8a8", maxLine), []*envelope.Item{item(nil)})
	second := write(t, NewStore(dir, "00000000-07bc-70b4-ae05-c57e7541d8a8", maxLine), []*envelope.Item{item(nil)})

	got, err := NewStore(dir, testAgent, maxLine).List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Errorf("order = %v, want [%s %s]",
			basenames(got), filepath.Base(first), filepath.Base(second))
	}
}

func basenames(paths []string) []string {
	out := make([]string, len(paths))
	for i, p := range paths {
		out[i] = filepath.Base(p)
	}
	return out
}

func TestListIgnoresForeignFiles(t *testing.T) {
	s := newStore(t)
	write(t, s, []*envelope.Item{item(nil)})
	for _, name := range []string{"README", "emg-x.txt", "other.jsonl", "emg-old.jsonl.corrupt",
		"emg-old.bin", "emg-old.bin.legacy"} {
		if err := os.WriteFile(filepath.Join(s.Dir(), name), []byte("junk"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Errorf("List returned %v, want only the one real dump", basenames(got))
	}
}

// SPEC §9.2: the binary dumps of go-webhook-fast-server are never read, only
// reported -- Foreign lists them, List never does.
func TestForeignListsOnlyTheFastBinaryDumps(t *testing.T) {
	s := newStore(t)
	write(t, s, []*envelope.Item{item(nil)})
	for _, name := range []string{"emg-a-2.bin", "emg-a-1.bin", "emg-b.bin.corrupt", "other.bin"} {
		if err := os.WriteFile(filepath.Join(s.Dir(), name), []byte("15\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Foreign()
	if err != nil {
		t.Fatal(err)
	}
	if b := basenames(got); len(b) != 2 || b[0] != "emg-a-1.bin" || b[1] != "emg-a-2.bin" {
		t.Errorf("Foreign = %v, want the two emg-*.bin in time order", b)
	}
	if l, _ := s.List(); len(l) != 1 {
		t.Errorf("List = %v, want only the .jsonl dump", basenames(l))
	}
}

// --------------------------------------------------------------------- damage
//
// SPEC §9.2: a damaged line is skipped and the next one read. The newline is
// the framing, so damage never spreads past the line it is on.

// A crash in the middle of the dump leaves a last line with no newline. The
// lines before it are delivered; the tail counts as damage, with its bytes.
func TestCutShortFileRecoversTheIntactLines(t *testing.T) {
	s := newStore(t)
	_, a := jlh([]byte("one"))
	_, b := jlh([]byte("two"))
	_, c := jlh([]byte("three"))
	data := lines(a, b)
	data = append(data, c[:len(c)/2]...)
	path := dumpFile(t, s, data)

	res, err := s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 || !bytes.Equal(res.Items[0].Env, a) || !bytes.Equal(res.Items[1].Env, b) {
		t.Fatalf("recovered %d items, want the two intact lines", len(res.Items))
	}
	if res.DamagedLines != 1 || res.DamagedBytes != int64(len(c)/2) {
		t.Errorf("damage = %d lines, %d bytes; want 1 line of %d bytes", res.DamagedLines, res.DamagedBytes, len(c)/2)
	}

	// A whole envelope that only lacks its newline is still a cut line: the
	// writer always ends a line, so a missing newline means the write never
	// finished.
	path = dumpFile(t, s, append(lines(a), b...))
	res, err = s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 1 || res.DamagedLines != 1 {
		t.Errorf("unterminated last line: %d items, %d damaged; want 1 and 1", len(res.Items), res.DamagedLines)
	}
}

// The improvement over the length-prefixed format: damage in the MIDDLE
// costs only its own line, and every line after it is still delivered.
func TestDamagedLineInTheMiddleIsSkipped(t *testing.T) {
	s := newStore(t)
	_, a := jlh([]byte("first"))
	_, c := jlh([]byte("last"))
	_, b := jlh([]byte("middle"))

	noBrace := b[:len(b)-1]                                                      // the payload never closes
	badJSON := bytes.Replace(b, []byte(`"params":{}`), []byte(`"params":{]`), 1) // ParseHead passes, json.Valid does not
	notOurs := []byte(`{"uuid":"x","hello":"world"}`)                            // JSON, not an envelope of this server
	// What json.Valid accepts and the serializer never writes (independent
	// review of go-webhook-pg-server, 2026-10-07): a raw byte that is not
	// UTF-8 inside a string, and an escaped lone surrogate. There, jsonb
	// refused them and every boot exited 3; here NATS would publish them.
	rawFF := bytes.Replace(b, []byte(`"path":"/x"`), []byte("\"path\":\"/x\xff\""), 1)
	surrogate := bytes.Replace(b, []byte(`"path":"/x"`), []byte(`"path":"/x\ud800"`), 1)
	if bytes.Equal(rawFF, b) || bytes.Equal(surrogate, b) || !json.Valid(rawFF) || !json.Valid(surrogate) {
		t.Fatal("fixture: the jsonb-refused lines are not what they claim")
	}
	cases := map[string][]byte{
		"truncated envelope": noBrace,
		"invalid utf-8":      rawFF,
		"lone surrogate":     surrogate,
		"invalid json":       badJSON,
		"foreign object":     notOurs,
		"binary garbage":     {0x00, 0xff, 0x7f},
		"a bare number":      []byte("12345"),
	}
	if bytes.Equal(badJSON, b) {
		t.Fatal("fixture: the invalid-json line was not altered")
	}
	for name, mid := range cases {
		t.Run(name, func(t *testing.T) {
			path := dumpFile(t, s, lines(a, mid, c))
			res, err := s.Read(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Items) != 2 || !bytes.Equal(res.Items[0].Env, a) || !bytes.Equal(res.Items[1].Env, c) {
				t.Fatalf("recovered %d items, want the lines around the damage", len(res.Items))
			}
			if res.DamagedLines != 1 || res.DamagedBytes != int64(len(mid)+1) {
				t.Errorf("damage = %d lines, %d bytes; want 1 line of %d bytes (newline included)",
					res.DamagedLines, res.DamagedBytes, len(mid)+1)
			}
		})
	}
}

// The escapes the serializer does write pass, and nothing else: an escaped
// backslash followed by "u" is text, not an escape.
func TestOurShapeEscapes(t *testing.T) {
	for in, want := range map[string]bool{
		`"a\u0000b"`:     true,
		`"\u001f"`:       true,
		`"\u2028\u2029"`: true,
		`"\\ud800"`:      true, // an escaped backslash, then the text ud800
		`"\n\t\r\"\\"`:   true,
		`"\/"`:           false, // valid JSON, never written
		`"\b"`:           false,
		`"\f"`:           false,
		`"\u000a"`:       false, // written as \n
		`"\u0041"`:       false, // 'A' is written raw
		`"\u00e9"`:       false, // so is every rune from U+0080
		`"\u001F"`:       false, // the writer's hex is lowercase
		`"\ud800"`:       false,
		`"\u0100"`:       false,
		`"\u20AC"`:       false,
		`"\`:             false,
		`"\u00`:          false,
		// SPEC §9.2 (§18.1 item 10): a raw U+2028/U+2029 is a second spelling
		// of the escape the writer always uses; its neighbours stay text.
		"\"a\xe2\x80\xa8b\"":       false,
		"\"\xe2\x80\xa9\"":         false,
		"\"\xe2\x80\xa7\"":         true, // U+2027, raw: an ordinary rune
		"\"\xc3\xa9\xe2\x82\xac\"": true,
	} {
		sc := shapeScanner{b: []byte(in)}
		_, got := sc.str()
		if got != want {
			t.Errorf("str(%s) = %v, want %v", in, got, want)
		}
	}
}

// SPEC §9.2: json.Valid admits much the writer never writes, and NATS never
// reads Data. Each probe passes ParseHead, utf8.Valid and json.Valid -- so
// the shape is what refuses it -- and each would have been published,
// confirmed and the file removed (the independent review, 2026-10-10).
func TestGateRefusesWhatTheWriterNeverWrites(t *testing.T) {
	_, good := jlh([]byte(`{"ok":1}`))
	payloadOf := func(e []byte) []byte {
		_, p, err := envelope.ParseHead(e)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	withPayload := func(p string) []byte {
		e := bytes.Replace(good, payloadOf(good), []byte(p), 1)
		return e
	}
	gp := string(payloadOf(good))
	deep := strings.Repeat(`[`, 9000) + strings.Repeat(`]`, 9000)
	var many []string
	for i := range 20 {
		many = append(many, fmt.Sprintf(`"k%d":"%d"`, i, i))
	}
	manyKeys := strings.Join(many, ",")
	probes := map[string][]byte{
		"duplicate mode": withPayload(`{"mode":"fast","mode":"bucket"` + gp[len(`{"mode":"fast"`):]),
		"duplicate network": withPayload(strings.Replace(gp, `,"method":`,
			`,"network":{"ip":{"version":4,"src":"10.6.6.6","dst":"10.0.0.1"}},"method":`, 1)),
		"duplicate body":     withPayload(strings.Replace(gp, `"}}`, `","body":"forged"}}`, 1)),
		"payload of one key": withPayload(`{"mode":"fast","x":1}`),
		"keys out of order": withPayload(strings.Replace(strings.Replace(gp, `"method":`, `"m3thod":`, 1),
			`"schema":`, `"method":`, 1)),
		"huge exponent":       withPayload(strings.Replace(gp, `"length":8`, `"length":1e999999`, 1)),
		"a fraction":          withPayload(strings.Replace(gp, `"length":8`, `"length":8.5`, 1)),
		"negative zero":       withPayload(strings.Replace(gp, `"length":8`, `"length":-0`, 1)),
		"a 400-digit integer": withPayload(strings.Replace(gp, `"length":8`, `"length":`+strings.Repeat("9", 400), 1)),
		"null":                withPayload(strings.Replace(gp, `"params":{}`, `"params":null`, 1)),
		"true":                withPayload(strings.Replace(gp, `"params":{}`, `"params":true`, 1)),
		"9000 levels":         withPayload(strings.Replace(gp, `"params":{}`, `"params":`+deep, 1)),
		// Past 16 keys the duplicate check is a map, not a scan.
		"duplicate among many query keys": withPayload(strings.Replace(gp, `"query":{"a":"1"}`,
			`"query":{`+manyKeys+`,"k3":"forged"}`, 1)),
		"escaped letter": withPayload(strings.Replace(gp, `"path":"/x"`, `"path":"/\u0078"`, 1)),
		"escaped slash":  withPayload(strings.Replace(gp, `"path":"/x"`, `"path":"\/x"`, 1)),
		"escaped é":      withPayload(strings.Replace(gp, `"path":"/x"`, `"path":"/\u00e9"`, 1)),
		// A raw U+2028, and the same key in its two spellings, which a
		// byte-wise duplicate check took for two keys (§18.1 item 10).
		"raw U+2028": withPayload(strings.Replace(gp, `"path":"/x"`, "\"path\":\"/\xe2\x80\xa8\"", 1)),
		"same key in two spellings": withPayload(strings.Replace(gp, `"query":{"a":"1"}`,
			`"query":{"a\u2028":"1",`+"\"a\xe2\x80\xa8\":\"2\"}", 1)),
	}
	// The uuid of another instant than the event's.
	h, _, _ := envelope.ParseHead(good)
	other := envelope.NewUUID(h.Event.Time().Add(time.Millisecond)).String()
	probes["uuid of another instant"] = bytes.Replace(good, []byte(h.UUID.String()), []byte(other), 1)

	for name, e := range probes {
		if bytes.Equal(e, good) {
			t.Fatalf("%s: the probe changed nothing", name)
		}
		if _, _, err := envelope.ParseHead(e); err != nil {
			t.Fatalf("%s: fails ParseHead (%v); the shape would not be what refuses it", name, err)
		}
		if !utf8.Valid(e) || !json.Valid(e) {
			t.Fatalf("%s: not valid JSON; json.Valid would refuse it already", name)
		}
		if _, err := checkLine(e); err == nil {
			t.Errorf("%s: the gate let it through", name)
		}
	}
	if _, err := checkLine(good); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// The other side of the gate: whatever the writer produces from hostile input
// passes -- control bytes, U+2028, invalid UTF-8, a binary body, repeated and
// numerous query keys, numerous headers (past the gate's switch from a linear
// scan to a map), IPv6, the fingerprints.
func TestGateAdmitsEverythingTheWriterWrites(t *testing.T) {
	var ctl []byte
	for c := 0; c < 0x20; c++ {
		ctl = append(ctl, byte(c))
	}
	text := string(ctl) + "\"\\\u2028\u2029é\xff\xfe€"
	hdr := http.Header{"Content-Type": {"text/plain"}, "X-Ctl": {text}}
	for i := range 40 {
		hdr.Add(fmt.Sprintf("X-H%02d", i), fmt.Sprintf("v%d", i))
		hdr.Add("X-Multi", strconv.Itoa(i))
	}
	var q []string
	for i := range 40 {
		q = append(q, fmt.Sprintf("k%d=%d", i, i), "rep=x"+strconv.Itoa(i))
	}
	q = append(q, "%FF=a", "%FE=b", "ctl="+strings.Repeat("%01", 3), "plus=a+b")
	for _, body := range [][]byte{nil, []byte(text), {0, 1, 2, 0xff}, bytes.Repeat([]byte{'x'}, 70000)} {
		now := time.Now()
		h := envelope.Header{UUID: envelope.NewUUID(now), From: testAgent, Space: "acme-1",
			Type: envelope.Type, Level: envelope.Level, SID: 1 << 52, Event: envelope.EventAt(now)}
		r := &envelope.Request{Mode: envelope.ModeFast, Src: netip.MustParseAddr("2001:db8::1"),
			Dst: netip.MustParseAddr("::1"), SPort: 65535, DPort: 443, SecProxy: true,
			FP2: text, FP3: "771,4865-4866", FP4: "t13d1516h2_8daaf6152771_02713d6af862",
			PeerAddr: netip.MustParseAddr("10.0.0.1"), PeerPort: 1, SrcSource: "proxy-v2",
			Proto: "HTTP/2.0", Method: "PATCH", Host: "[2001:db8::1]", Path: "/" + text,
			RawQuery: strings.Join(q, "&"), Header: hdr, HostHeader: text, ContentType: text, Body: body}
		env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(body))
		if _, err := checkLine(env); err != nil {
			_, p, _ := envelope.ParseHead(env)
			t.Errorf("body of %d bytes: the gate refused the writer's own envelope (%v):\n%.600s", len(body), err, p)
		}
	}
}

// Empty lines are nothing written and nothing lost: skipped without counting
// as damage (SPEC §9.2).
func TestEmptyLinesAreNotDamage(t *testing.T) {
	s := newStore(t)
	_, a := jlh([]byte("x"))
	_, b := jlh([]byte("y"))
	data := append([]byte("\n\n"), lines(a)...)
	data = append(data, '\n')
	data = append(data, lines(b)...)
	data = append(data, '\n')
	res, err := s.Read(dumpFile(t, s, data))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 || res.Damaged() {
		t.Errorf("%d items, %d damaged; want 2 and 0", len(res.Items), res.DamagedLines)
	}
}

// SPEC §9.2: a line above the ceiling -- the largest envelope the current
// configuration can produce -- is skipped WITHOUT being held in memory, and
// the reader resynchronises at the next newline. Without the ceiling, a
// corrupt file with no newline is one allocation the size of the file.
func TestOverlongLineIsSkippedAndResynchronised(t *testing.T) {
	_, a := jlh([]byte("before"))
	_, b := jlh([]byte("after"))
	ceiling := int64(len(a) + len(b)) // both fit; the junk does not
	dir := t.TempDir()
	s := NewStore(dir, testAgent, ceiling)

	junk := bytes.Repeat([]byte("j"), int(ceiling)*3+70_000) // longer than the reader's buffer, too
	path := dumpFile(t, s, lines(a, junk, b))
	res, err := s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 2 || !bytes.Equal(res.Items[0].Env, a) || !bytes.Equal(res.Items[1].Env, b) {
		t.Fatalf("recovered %d items, want the lines around the over-long one", len(res.Items))
	}
	if res.DamagedLines != 1 || res.DamagedBytes != int64(len(junk)+1) {
		t.Errorf("damage = %d lines, %d bytes; want 1 line of %d", res.DamagedLines, res.DamagedBytes, len(junk)+1)
	}

	// An envelope exactly at the ceiling is accepted: the ceiling counts the
	// line without its newline.
	s = NewStore(dir, testAgent, int64(len(a)))
	res, err = s.Read(dumpFile(t, s, lines(a)))
	if err != nil || len(res.Items) != 1 || res.Damaged() {
		t.Errorf("a line exactly at the ceiling: %d items, damaged %d, err %v", len(res.Items), res.DamagedLines, err)
	}
	// And one byte below it, refused.
	s = NewStore(dir, testAgent, int64(len(a))-1)
	res, err = s.Read(dumpFile(t, s, lines(a)))
	if err != nil || len(res.Items) != 0 || res.DamagedLines != 1 {
		t.Errorf("a line one byte over: %d items, damaged %d, err %v", len(res.Items), res.DamagedLines, err)
	}
}

// A file with no newline at all and no ceiling-sized line still ends: the
// whole content is one damaged line.
func TestFileWithoutNewlineIsOneDamagedLine(t *testing.T) {
	s := newStore(t)
	data := bytes.Repeat([]byte("z"), 200_000)
	res, err := s.Read(dumpFile(t, s, data))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Items) != 0 || res.DamagedLines != 1 || res.DamagedBytes != int64(len(data)) {
		t.Errorf("%d items, %d damaged lines, %d bytes; want 0, 1, %d",
			len(res.Items), res.DamagedLines, res.DamagedBytes, len(data))
	}
}

// ------------------------------------------------------------------ handling

func TestQuarantine(t *testing.T) {
	s := newStore(t)
	path := write(t, s, []*envelope.Item{item(nil)})
	dst, err := s.Quarantine(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(dst, corruptExt) {
		t.Errorf("quarantined to %q, want a %s suffix", dst, corruptExt)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("the original file should have been renamed away")
	}
	if _, err := os.Stat(dst); err != nil {
		t.Errorf("the quarantined file should exist: %v", err)
	}
	// A quarantined file must not come back through List.
	if got, _ := s.List(); len(got) != 0 {
		t.Errorf("List returned %v after quarantine", basenames(got))
	}
}

func TestRemove(t *testing.T) {
	s := newStore(t)
	path := write(t, s, []*envelope.Item{item(nil)})
	if err := s.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.List(); len(got) != 0 {
		t.Errorf("List returned %v after Remove", basenames(got))
	}
}

func TestCount(t *testing.T) {
	s := newStore(t)
	if f, e, err := s.Count(); err != nil || f != 0 || e != 0 {
		t.Errorf("empty store: files=%d events=%d err=%v", f, e, err)
	}
	write(t, s, []*envelope.Item{item(nil), item(nil)})
	write(t, s, []*envelope.Item{item(nil)})
	f, e, err := s.Count()
	if err != nil || f != 2 || e != 3 {
		t.Errorf("files=%d events=%d err=%v; want 2 files and 3 events", f, e, err)
	}
}

func TestListOnMissingDirectory(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "does-not-exist"), testAgent, maxLine)
	got, err := s.List()
	if err != nil || len(got) != 0 {
		t.Errorf("List on a missing directory = %v, %v; want empty and no error", got, err)
	}
}

func TestSanitize(t *testing.T) {
	cases := map[string]string{
		testAgent:     testAgent, // a uuid is kept as is: SPEC §9.1 names the file after it
		"gws-prod-a1": "gws-prod-a1",
		"":            "agent",
		"a/b\\c":      "a_b_c",
		"../x":        "___x",
		"ok_123":      "ok_123",
	}
	for in, want := range cases {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
	if got := sanitize(strings.Repeat("a", 200)); len(got) != 64 {
		t.Errorf("sanitize truncates to 64 chars, got %d", len(got))
	}
}

// FuzzRead is mandatory per SPEC §15.2. This decoder reads from disk right
// after a crash: a loose parser turns a corrupt file into a panic at boot, at
// the exact moment the operator most needs the process to come back -- or
// worse, delivers a line that is not an envelope.
func FuzzRead(f *testing.F) {
	// Real dumps first, so the mutator starts from the shape that matters:
	// whole JLH lines with text and base64 bodies, around damage.
	_, env1 := jlh([]byte(`{"a":1}`))
	_, env2 := jlh([]byte{0x00, 0xff, '\n'})
	seeds := [][]byte{
		lines(env1),
		lines(env1, env2),
		append(lines(env1), env2[:len(env2)/2]...),
		lines(env1, []byte("garbage"), env2),
		lines(env1, []byte(""), env2),
		[]byte(""),
		[]byte("\n"),
		[]byte("{}\n"),
		[]byte("15\n" + string(env1)), // the fast's binary format
		[]byte("\x00\x01\x02"),
		[]byte("\r\n" + string(env1) + "\r\n"),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// In memory: a file per input, fsynced on the way back out, held the
		// mutator to a few runs a second on macOS (independent review,
		// 2026-10-07). Read is Open plus readFrom; the I/O is not what is
		// being fuzzed. A small ceiling, so the over-long path is reached by
		// the mutator and not only by inputs of megabytes.
		maxLine := int64(len(env1) + len(env2))
		res, err := readFrom(bytes.NewReader(data), maxLine)
		if err != nil {
			t.Fatalf("reading from memory cannot fail: %v", err)
		}

		// Invariant 1: every recovered item is fully formed -- a JLH this
		// server would have written, valid JSON and UTF-8, its own uuid as the
		// id, no newline inside. A half-read item would be delivered as a real
		// event.
		var total int64
		for i, it := range res.Items {
			if it == nil {
				t.Fatalf("item %d is nil", i)
			}
			h, payload, err := envelope.ParseHead(it.Env)
			if err != nil || !json.Valid(it.Env) || !utf8.Valid(it.Env) {
				t.Fatalf("item %d was recovered without being a valid JLH: %v", i, err)
			}
			// The gate of SPEC §9.2: NATS publishes what passes without reading
			// it, so only this server's own envelopes may come out.
			if h.Type != envelope.Type || h.Level != envelope.Level || h.Step != 0 ||
				!bytes.HasPrefix(payload, []byte(payloadPrefix)) {
				t.Fatalf("item %d is not an envelope of this server: type %q level %d step %d",
					i, h.Type, h.Level, h.Step)
			}
			if it.ID != h.UUID.String() {
				t.Fatalf("item %d id %q, envelope uuid %s", i, it.ID, h.UUID)
			}
			// An oracle the gate does not share: the JLH schema and a
			// decoder that sees every duplicate key, at any depth.
			if err := jlhcheck.ValidateEnvelopeSchema(it.Env); err != nil {
				t.Fatalf("item %d passed the gate and fails the JLH envelope checks: %v", i, err)
			}
			if bytes.IndexByte(it.Env, '\n') >= 0 {
				t.Fatalf("item %d carries a raw newline", i)
			}
			total += int64(len(it.Env)) + 1
		}

		// Invariant 2: every byte of the input is accounted for, exactly once:
		// in an item and its newline, in a damaged line, or in an empty line.
		if got := total + res.DamagedBytes + int64(res.EmptyLines); got != int64(len(data)) {
			t.Fatalf("items %d + damage %d + empty %d bytes, from %d bytes of input",
				total, res.DamagedBytes, res.EmptyLines, len(data))
		}

		// Invariant 3: what was recovered round-trips through the writer's
		// encoding and the reader unchanged, with no damage.
		if len(res.Items) > 0 {
			var buf bytes.Buffer
			if err := encode(&buf, res.Items); err != nil {
				t.Fatal(err)
			}
			res2, err := readFrom(&buf, maxLine)
			if err != nil {
				t.Fatal(err)
			}
			if res2.Damaged() || len(res2.Items) != len(res.Items) {
				t.Fatalf("round-trip changed the record count: %d then %d (damaged %d)",
					len(res.Items), len(res2.Items), res2.DamagedLines)
			}
			for i := range res.Items {
				if !bytes.Equal(res.Items[i].Env, res2.Items[i].Env) {
					t.Fatalf("round-trip changed record %d", i)
				}
			}
		}
	})
}

// FuzzGateAdmitsTheWriter is the other half of the gate's contract: a gate
// stricter than the writer would quarantine real events, answered 200. Every
// envelope envelope.AppendWebhook writes -- whatever the path, query, header
// values, content type and body -- passes checkLine.
func FuzzGateAdmitsTheWriter(f *testing.F) {
	f.Add("/x", "a=1&a=2&b", "v", "application/json", []byte(`{"ok":1}`), uint16(443), true)
	f.Add("/\x00 ", "%FF=%FE&%00", "\x01\"\\", "", []byte{0, 0xff, '\n'}, uint16(0), false)
	f.Add("", "", "", "text/plain", []byte(nil), uint16(65535), true)
	f.Fuzz(func(t *testing.T, path, query, value, ctype string, body []byte, port uint16, v6 bool) {
		now := time.Now()
		h := envelope.Header{UUID: envelope.NewUUID(now), From: testAgent, Space: "default",
			Type: envelope.Type, Level: envelope.Level, SID: 1, Event: envelope.EventAt(now)}
		src := netip.MustParseAddr("203.0.113.9")
		if v6 {
			src = netip.MustParseAddr("2001:db8::9")
		}
		r := &envelope.Request{Mode: envelope.ModeFast, Src: src, Dst: netip.MustParseAddr("127.0.0.1"),
			SPort: port, DPort: 8080, SecProxy: v6, FP2: value, FP3: value, FP4: value,
			PeerAddr: src, PeerPort: port, SrcSource: value, Proto: "HTTP/1.1", Method: value,
			Host: value, URLPort: port, Path: path, RawQuery: query,
			Header: http.Header{"X-A": {value, value}, tokenOf(value): {path}}, HostHeader: value,
			ContentType: ctype, Body: body}
		env := envelope.AppendWebhook(nil, &h, r, envelope.PlanBody(body))
		if bytes.IndexByte(env, '\n') >= 0 {
			return // the dump leaves it out before the gate (TestWriteLeavesOutAnEnvelopeWithARawNewline)
		}
		if _, err := checkLine(env); err != nil {
			t.Fatalf("the gate refused the writer's own envelope: %v\n%.400s", err, env)
		}
	})
}

// tokenOf keeps the HTTP token characters of s (RFC 9110 §5.6.2): a header
// name is one, net/http refuses any other before the handler sees it, and the
// writer emits names without escaping them.
func tokenOf(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0 {
			b = append(b, c)
		}
	}
	return "X-" + string(b)
}
