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

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/jlhcheck"
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

func newStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(t.TempDir(), testAgent)
}

// ------------------------------------------------------------------ round-trip

func TestRoundTrip(t *testing.T) {
	s := newStore(t)

	// A webhook body is arbitrary binary, and it now travels inside the JLH:
	// as escaped text or as base64. The format must survive both, and anything
	// that looks like a header line, which is the reason it is length-prefixed
	// rather than line-delimited JSON.
	bodies := [][]byte{
		[]byte("plain"),
		{0x00, 0x0a, 0xff, 0x0d, 0x1a},
		[]byte(""),
		bytes.Repeat([]byte("x"), 100_000),
		[]byte("12 34\nlooks like a header line"),
		[]byte("12\nlooks like a header line too"),
	}
	want := make([]*envelope.Item, len(bodies))
	for i, b := range bodies {
		want[i] = item(b)
	}

	path, err := s.Write(want)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	res, err := s.Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if res.Truncated {
		t.Fatal("a file we just wrote should not read as truncated")
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
		// contract the destinations expect, and carry the exact body.
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

// The file carries the JLH alone, so a reingested item's identity has to be
// rebuilt from the JLH. Before LogHub this was the ItemMeta rebuild: a sink
// fills its columns from what Read returns, and an item that came back with an
// empty id would collide with every other one on the destination's key. Here
// the item is written with no id at all, so only the envelope can supply it.
func TestReadRebuildsTheIdentityFromTheJLH(t *testing.T) {
	s := newStore(t)
	id, env := jlh([]byte(`{"n":1}`))

	path, err := s.Write([]*envelope.Item{envelope.NewItem("", env, -1)})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
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
	path, err := s.Write(nil)
	if err != nil || path != "" {
		t.Fatalf("Write(nil) = %q, %v; want no file", path, err)
	}
	files, _ := s.List()
	if len(files) != 0 {
		t.Errorf("an empty dump created %d files", len(files))
	}
}

func TestWriteCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "nested")
	s := NewStore(dir, testAgent)
	if _, err := s.Write([]*envelope.Item{item([]byte("x"))}); err != nil {
		t.Fatalf("Write should create the directory: %v", err)
	}
}

// write is Write for the tests that expect it to succeed.
func write(t *testing.T, s *Store, items []*envelope.Item) string {
	t.Helper()
	path, err := s.Write(items)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return path
}

// SPEC §9.1: the dump is written under a hidden temporary name and linked to
// the final one complete; nothing of the temporary is left behind, and a
// temporary a crash left is never listed, read or removed.
func TestWriteGoesThroughATemporaryName(t *testing.T) {
	// The final name appears only by the link, from a hidden temporary that
	// is already complete: written in place, a dump cut at a record boundary
	// would read clean.
	defer func(orig func(string, string) error) { link = orig }(link)
	var linked []string
	link = func(o, n string) error {
		if _, err := os.Lstat(n); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the final name exists before the link: %v", err)
		}
		if !strings.HasPrefix(filepath.Base(o), ".") || !strings.HasSuffix(o, tmpSuffix) {
			t.Errorf("linked from %s, not a hidden temporary", filepath.Base(o))
		}
		if res, err := NewStore(filepath.Dir(o), testAgent).Read(o); err != nil ||
			len(res.Items) != 2 || res.Truncated {
			t.Errorf("the temporary is not the complete dump at the link: %v %v", res, err)
		}
		linked = append(linked, n)
		return os.Link(o, n)
	}
	s := newStore(t)
	path := write(t, s, []*envelope.Item{item([]byte("a")), item([]byte("b"))})
	if len(linked) != 1 || linked[0] != path {
		t.Errorf("linked %v, want once to %s", linked, path)
	}
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

	// A temporary a crash left behind, cut at a record boundary: it would
	// read clean, which is exactly why it must not be taken for a dump.
	_, env := jlh([]byte("half"))
	tmp := filepath.Join(s.Dir(), "."+fmt.Sprintf("emg-%s-%d%s", testAgent, time.Now().UnixNano(), fileSuffix)+tmpSuffix)
	if err := os.WriteFile(tmp, append([]byte(fmt.Sprintf("%d\n", len(env))), env...), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0] != path {
		t.Errorf("List = %v, want only %s", files, path)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Errorf("the leftover temporary was touched: %v", err)
	}
}

// A file system without hard links (vfat, exFAT, object-store FUSE mounts)
// refuses Link whatever the names. Every dump would fail there, the whole
// ring lost; the free name is checked and the file renamed. A Link refused
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
	if err != nil || len(res.Items) != 2 || res.Truncated {
		t.Fatalf("the renamed dump reads %v items, err %v", res, err)
	}

	// The fallback never replaces a dump that has the final name.
	tmp := filepath.Join(s.Dir(), ".emg-x-1.bin.tmp")
	final := filepath.Join(s.Dir(), "emg-x-1.bin")
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
	if err := publish(tmp, filepath.Join(s.Dir(), "emg-x-2.bin")); !errors.Is(err, os.ErrExist) {
		t.Errorf("a Link refused for an existing name fell back to a rename: %v", err)
	}
}

// A directory that can be written but not read (0300) cannot be opened for
// its fsync. Write removed the complete, synced dump for that.
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
	path, err := s.Write([]*envelope.Item{item([]byte("a"))})
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

// SPEC §9.1: emg-<agent_uuid>-<unixnano>.bin. The AgentUUID says whose dump a
// file is when several instances share a volume, and it must survive intact:
// a uuid is already filename-safe, hyphens included.
func TestFileNameCarriesTheAgentUUID(t *testing.T) {
	s := newStore(t)
	before := time.Now().UnixNano()
	path, err := s.Write([]*envelope.Item{item([]byte("x"))})
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().UnixNano()

	name := filepath.Base(path)
	m := regexp.MustCompile(`^emg-(.+)-([0-9]+)\.bin$`).FindStringSubmatch(name)
	if m == nil {
		t.Fatalf("file name %q is not emg-<agent_uuid>-<unixnano>.bin", name)
	}
	if m[1] != testAgent {
		t.Errorf("agent part = %q, want the AgentUUID %q", m[1], testAgent)
	}
	ns, _ := strconv.ParseInt(m[2], 10, 64)
	if ns < before || ns > after {
		t.Errorf("timestamp %d outside the write [%d, %d]", ns, before, after)
	}
	if got := tsOf(path); got != ns {
		t.Errorf("tsOf = %d, want %d: the hyphens of the uuid must not confuse it", got, ns)
	}
}

// ------------------------------------------------------------------- ordering

func TestListIsChronological(t *testing.T) {
	s := newStore(t)
	var paths []string
	for i := 0; i < 5; i++ {
		p, err := s.Write([]*envelope.Item{item([]byte("y"))})
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
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
	first, err := NewStore(dir, "ffffffff-07bc-70b4-ae05-c57e7541d8a8").Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(dir, "00000000-07bc-70b4-ae05-c57e7541d8a8").Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}

	got, err := NewStore(dir, testAgent).List()
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
	if _, err := s.Write([]*envelope.Item{item(nil)}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"README", "emg-x.txt", "other.bin", "emg-old.bin.corrupt", "emg-old.bin.legacy"} {
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

// ----------------------------------------------------------------- truncation

// SPEC §9.2: a file cut short is processed up to the last intact record and
// then quarantined. Partial recovery beats none, and a crash must never turn
// into a panic at the next boot.
func TestTruncatedFileRecoversThePrefix(t *testing.T) {
	s := newStore(t)
	items := []*envelope.Item{
		item([]byte("first")),
		item([]byte("second")),
		item([]byte("third")),
	}
	path, err := s.Write(items)
	if err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Cut at several points; every one must recover a prefix and report the
	// cut. The last cut is one byte short: only the final brace is missing,
	// which the declared length alone already catches.
	cuts := []int{}
	for _, frac := range []float64{0.35, 0.5, 0.75, 0.95} {
		cuts = append(cuts, int(float64(len(full))*frac))
	}
	cuts = append(cuts, len(full)-1)
	for _, cut := range cuts {
		p := filepath.Join(t.TempDir(), "emg-t-1"+fileSuffix)
		if err := os.WriteFile(p, full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}

		st := NewStore(filepath.Dir(p), testAgent)
		res, err := st.Read(p)
		if err != nil {
			t.Fatalf("cut at %d/%d: Read returned an error instead of a truncation: %v", cut, len(full), err)
		}
		if !res.Truncated {
			t.Errorf("cut at %d/%d: Truncated not reported", cut, len(full))
		}
		if len(res.Items) >= len(items) {
			t.Errorf("cut at %d/%d: recovered %d items from a truncated file", cut, len(full), len(res.Items))
		}
		// Whatever was recovered must be byte-exact, not partially filled.
		var kept int64
		for i, got := range res.Items {
			if !bytes.Equal(got.Env, items[i].Env) || got.ID != items[i].ID {
				t.Errorf("cut at %d/%d: item %d is corrupt", cut, len(full), i)
			}
			kept += int64(len(strconv.Itoa(len(got.Env)))+1) + int64(len(got.Env))
		}
		// What is discarded is the tail after the last good record, the cut
		// record's header line included.
		if tail := int64(cut) - kept; res.TruncatedBytes != tail {
			t.Errorf("cut at %d/%d: TruncatedBytes = %d, want the %d-byte tail", cut, len(full), res.TruncatedBytes, tail)
		}
	}
}

// SPEC §9.2: every record's header is checked with the reader the destinations
// use (envelope.ParseHead). A record whose length is fine but whose JLH is not
// the shape this server writes is damage, not an event: it and everything after
// it count as truncated, so the file is quarantined rather than delivered.
func TestRecordWithAnInvalidJLHCountsAsTruncated(t *testing.T) {
	_, good1 := jlh([]byte("one"))
	_, good2 := jlh([]byte("two"))
	_, base := jlh([]byte("bad"))

	bad := map[string][]byte{
		"v1 envelope":        []byte(`{"id":"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f","ts":1757116800,"method":"POST"}`),
		"level out of range": bytes.Replace(base, []byte(`"level":9`), []byte(`"level":16`), 1),
		"not JSON":           []byte("this is not an envelope at all"),
		"empty record":       {},
		// The header is fine and the braces balance, so only json.Valid
		// tells this payload is no JSON.
		"payload not JSON": append(append([]byte{}, base[:bytes.Index(base, []byte(`"payload":`))+len(`"payload":`)]...), `{]}`...),
	}
	record := func(env []byte) []byte { return append([]byte(fmt.Sprintf("%d\n", len(env))), env...) }

	dir := t.TempDir()
	st := NewStore(dir, testAgent)

	// The control: the two good records alone read back clean, so whatever
	// fails below fails because of the bad record and nothing else.
	ctl := filepath.Join(dir, "emg-control-1"+fileSuffix)
	if err := os.WriteFile(ctl, append(record(good1), record(good2)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if res, err := st.Read(ctl); err != nil || res.Truncated || len(res.Items) != 2 {
		t.Fatalf("control: %v, %+v; want two clean items", err, res)
	}

	for name, env := range bad {
		t.Run(name, func(t *testing.T) {
			if _, _, err := envelope.ParseHead(env); err == nil && json.Valid(env) {
				t.Fatal("test setup: the bad record is a valid JLH")
			}
			first := record(good1)
			data := append(append(append([]byte{}, first...), record(env)...), record(good2)...)
			p := filepath.Join(t.TempDir(), "emg-bad-1"+fileSuffix)
			if err := os.WriteFile(p, data, 0o600); err != nil {
				t.Fatal(err)
			}
			res, err := st.Read(p)
			if err != nil {
				t.Fatalf("Read: %v; want a truncation, not an error", err)
			}
			if !res.Truncated {
				t.Error("an invalid JLH was not reported as damage")
			}
			if len(res.Items) != 1 || !bytes.Equal(res.Items[0].Env, good1) {
				t.Errorf("recovered %d items, want only the intact prefix", len(res.Items))
			}
			// Everything after the last good record, the bad record's own
			// header line included.
			if tail := int64(len(data) - len(first)); res.TruncatedBytes != tail {
				t.Errorf("TruncatedBytes = %d, want the %d-byte tail", res.TruncatedBytes, tail)
			}

			// The same record first: nothing is recovered, and it is still a
			// truncation rather than an error.
			p2 := filepath.Join(t.TempDir(), "emg-bad-2"+fileSuffix)
			if err := os.WriteFile(p2, append(record(env), record(good2)...), 0o600); err != nil {
				t.Fatal(err)
			}
			res, err = st.Read(p2)
			if err != nil || !res.Truncated || len(res.Items) != 0 {
				t.Errorf("bad first record: %v, %+v; want truncated with no items", err, res)
			}
		})
	}
}

// A corrupt header claiming a huge length must produce a truncation report, not
// a multi-gigabyte allocation.
func TestCorruptLengthDoesNotAllocate(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "emg-x-1"+fileSuffix)
	if err := os.WriteFile(p, []byte("2000000000\nshort"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := NewStore(dir, testAgent).Read(p)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !res.Truncated {
		t.Error("an impossible declared length should read as truncated")
	}
	if len(res.Items) != 0 {
		t.Errorf("recovered %d items from a corrupt header", len(res.Items))
	}
}

func TestGarbageHeaders(t *testing.T) {
	cases := map[string]string{
		"not a number":   "abc\npayload",
		"three fields":   "1 2 3\npayload",
		"negative":       "-1\npayload",
		"empty line":     "\npayload",
		"no newline":     "12",
		"binary":         "\x00\x01\x02\x03",
		"zero length":    "0\n",
		"beyond the end": "100\nshort",
		"overflow":       "99999999999999999999\nx",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "emg-x-1"+fileSuffix)
			if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			res, err := NewStore(dir, testAgent).Read(p)
			if err != nil {
				t.Fatalf("Read returned an error instead of a truncation: %v", err)
			}
			if !res.Truncated {
				t.Error("garbage was not reported as truncated")
			}
			if len(res.Items) != 0 {
				t.Errorf("recovered %d items from garbage", len(res.Items))
			}
		})
	}
}

// ------------------------------------------------------------------- legacy

// v1Dump is a file in the pre-LogHub format: "<len_env> <len_body>\n" then the
// old envelope and the body.
func v1Dump(records int) []byte {
	var b bytes.Buffer
	for i := 0; i < records; i++ {
		env := fmt.Sprintf(`{"id":"019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e%02d","ts":1757116800,"method":"POST"}`, i)
		body := []byte{byte(i), 0x00, '\n', 0xff}
		fmt.Fprintf(&b, "%d %d\n", len(env), len(body))
		b.WriteString(env)
		b.Write(body)
	}
	return b.Bytes()
}

// SPEC §9.2: a dump with TWO lengths on the header is the v1 format. It is
// refused whole -- never half-read, never mixed into the LogHub ring --, and
// SetAsideLegacy renames it to .legacy so the previous version can still
// reingest it.
func TestLegacyDumpIsRefusedWhole(t *testing.T) {
	s := newStore(t)

	// The control: a dump of this version is not legacy.
	cur, err := s.Write([]*envelope.Item{item([]byte("now"))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(cur); err != nil {
		t.Fatalf("control: a current dump reads as %v", err)
	}

	p := filepath.Join(s.Dir(), "emg-node-a-1757116800000000000"+fileSuffix)
	data := v1Dump(3)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(p)
	if !errors.Is(err, ErrLegacy) {
		t.Fatalf("Read = %+v, %v; want ErrLegacy", res, err)
	}
	if res != nil {
		t.Errorf("a legacy file returned %d items; it must be refused whole", len(res.Items))
	}

	// Count must not count it as pending events of this version. Left in
	// place -- the boot could not set it aside -- it is still a file for
	// the operator, as an unreadable one is (SPEC §11.1).
	if files, events, err := s.Count(); err != nil || files != 2 || events != 1 {
		t.Errorf("Count = %d files, %d events, %v; want both files and only the current dump's event",
			files, events, err)
	}

	dst, err := s.SetAsideLegacy(p)
	if err != nil {
		t.Fatal(err)
	}
	if dst != p+legacyExt {
		t.Errorf("set aside to %q, want %q", dst, p+legacyExt)
	}
	if b, err := os.ReadFile(dst); err != nil || !bytes.Equal(b, data) {
		t.Errorf("the legacy file was not kept byte for byte: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Error("the legacy file is still under its .bin name")
	}
	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != cur {
		t.Errorf("List = %v after SetAsideLegacy, want only the current dump", basenames(got))
	}
}

// Legacy is a property of the FILE, decided by its first header: a file cannot
// be half v1 and half LogHub, so a two-length header after valid records is
// damage, recovered up to it like any other.
func TestTwoLengthsAfterAValidRecordIsDamage(t *testing.T) {
	s := newStore(t)
	_, env := jlh([]byte("ok"))
	data := append([]byte(fmt.Sprintf("%d\n", len(env))), env...)
	data = append(data, v1Dump(1)...)
	p := filepath.Join(s.Dir(), "emg-mixed-1"+fileSuffix)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(p)
	if err != nil {
		t.Fatalf("Read: %v; want a truncation", err)
	}
	if !res.Truncated || len(res.Items) != 1 || !bytes.Equal(res.Items[0].Env, env) {
		t.Errorf("got truncated=%v with %d items; want the one valid record and a truncation",
			res.Truncated, len(res.Items))
	}
}

// SPEC §9.2 says legacy is TWO LENGTHS on the header, and parseHeader's own
// comment says "two numbers". Two fields that are not lengths are damage, and
// must be quarantined as .corrupt like any other garbage -- not set aside as
// .legacy with an ERROR telling the operator to reingest them with the previous
// version, which would fail there too.
func TestTwoNonLengthsAreNotLegacy(t *testing.T) {
	for name, content := range map[string]string{
		"two words":     "abc def\npayload",
		"two negatives": "-1 -1\npayload",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "emg-x-1"+fileSuffix)
			if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			res, err := NewStore(dir, testAgent).Read(p)
			if errors.Is(err, ErrLegacy) {
				t.Fatal("regressão de §9.2: parseHeader (emergency.go) flags any two-field header as legacy, " +
					"numbers or not; a damaged dump is renamed .legacy instead of .corrupt")
			}
			if err != nil || !res.Truncated || len(res.Items) != 0 {
				t.Errorf("Read = %+v, %v; want a truncation with no items", res, err)
			}
		})
	}
}

// ------------------------------------------------------------------ handling

func TestQuarantine(t *testing.T) {
	s := newStore(t)
	path, err := s.Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}
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
	path, err := s.Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := s.Write([]*envelope.Item{item(nil), item(nil)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]*envelope.Item{item(nil)}); err != nil {
		t.Fatal(err)
	}
	f, e, err := s.Count()
	if err != nil || f != 2 || e != 3 {
		t.Errorf("files=%d events=%d err=%v; want 2 files and 3 events", f, e, err)
	}
}

// SPEC §11.1: a dump that cannot be read stays where it is, awaiting
// reingestion, so it is a pending file -- its events unknown. Skipping it hid
// it from whs_emergency_files (independent review of the 2026-10-11 port).
func TestCountKeepsAnUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads any file")
	}
	s := newStore(t)
	if _, err := s.Write([]*envelope.Item{item(nil), item(nil)}); err != nil {
		t.Fatal(err)
	}
	locked, err := s.Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(locked); err == nil {
		t.Fatal("control: the locked dump is still readable")
	}
	f, e, err := s.Count()
	if err != nil || f != 2 || e != 2 {
		t.Errorf("files=%d events=%d err=%v; want 2 files (one unreadable) and the readable one's 2 events",
			f, e, err)
	}
}

func TestListOnMissingDirectory(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "does-not-exist"), testAgent)
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
// the exact moment the operator most needs the process to come back.
func FuzzRead(f *testing.F) {
	// Real dumps first, so the mutator starts from the shape that matters:
	// one-length headers over whole JLH records, text and base64 bodies.
	_, env1 := jlh([]byte(`{"a":1}`))
	_, env2 := jlh([]byte{0x00, 0xff, '\n'})
	rec := func(env []byte) string { return strconv.Itoa(len(env)) + "\n" + string(env) }
	seeds := []string{
		rec(env1),
		rec(env1) + rec(env2),
		rec(env1) + rec(env2)[:len(rec(env2))/2],
		rec(env1) + "3\nabc",
		string(v1Dump(2)),
		"",
		"\n",
		"0\n",
		"5\nhello",
		"12 13\n",
		"abc def\n",
		"-1\n",
		"2000000000\nx",
		"1\na",
		"1 1\na",
		"9999999999999999999\nx",
		"0\n0\n0\n",
		"\x00\x01\x02",
		"1" + strings.Repeat("9", 100) + "\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		p := filepath.Join(dir, "emg-fuzz-1"+fileSuffix)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Skip()
		}
		st := NewStore(dir, testAgent)

		res, err := st.Read(p)
		if err != nil {
			// An I/O error or a legacy file is acceptable; a panic is not.
			return
		}

		// Invariant 1: every recovered item is fully formed -- a JLH this
		// server would have written, with its own uuid as the id. A half-read
		// item would be delivered downstream as a real event.
		for i, it := range res.Items {
			if it == nil {
				t.Fatalf("item %d is nil", i)
			}
			h, _, err := envelope.ParseHead(it.Env)
			if err != nil {
				t.Fatalf("item %d was recovered with an invalid JLH: %v", i, err)
			}
			if it.ID != h.UUID.String() {
				t.Fatalf("item %d id %q, envelope uuid %s", i, it.ID, h.UUID)
			}
		}

		// Invariant 2: nothing recovered can exceed the file itself, which is
		// what proves no declared length was trusted blindly.
		var total int64
		for _, it := range res.Items {
			total += int64(len(it.Env))
		}
		if total > int64(len(data)) {
			t.Fatalf("recovered %d bytes from a %d byte file", total, len(data))
		}

		// Invariant 3: a clean read must round-trip. If the decoder accepted
		// this input as complete, re-encoding it must reproduce parseable
		// output -- otherwise "not truncated" is meaningless.
		if !res.Truncated && len(res.Items) > 0 {
			p2, err := st.Write(res.Items)
			if err != nil {
				t.Fatalf("re-writing a cleanly decoded file failed: %v", err)
			}
			res2, err := st.Read(p2)
			if err != nil {
				t.Fatalf("re-reading failed: %v", err)
			}
			if res2.Truncated || len(res2.Items) != len(res.Items) {
				t.Fatalf("round-trip changed the record count: %d then %d (truncated=%v)",
					len(res.Items), len(res2.Items), res2.Truncated)
			}
			for i := range res.Items {
				if !bytes.Equal(res.Items[i].Env, res2.Items[i].Env) {
					t.Fatalf("round-trip changed record %d", i)
				}
			}
		}
	})
}
