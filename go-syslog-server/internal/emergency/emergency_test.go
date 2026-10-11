package emergency

import (
	"bufio"
	"bytes"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
)

const agent = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

var base = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// mkItem builds a valid item whose index is recoverable from its clock sample.
// Odd items are TLS with every optional head key; even ones unix datagrams with
// none -- the two extremes of the codec.
func mkItem(i int, line string) *item.Item {
	t := base.Add(time.Duration(i) * time.Microsecond)
	h := item.Head{
		UUID:  uuidv7.GenerateAt(uuidv7.Level3, t),
		From:  agent,
		Space: "com.acme",
		Size:  len(line),
	}
	h.SetTime(t)
	if i%2 == 1 {
		h.Transport, h.Framing = item.TransportTLS, item.FramingOctetCounting
		h.SrcIP, h.SrcPort = netip.MustParseAddr("2001:db8::7"), 40000
		h.DstIP, h.DstPort = netip.MustParseAddr("10.0.0.10"), 6514
		h.PeerIP, h.PeerPort = netip.MustParseAddr("127.0.0.1"), 41000
		h.TLS = &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256",
			PeerSubject: "CN=fw01", PeerFingerprint: "sha-256:3B:8E"}
		h.Size = len(line) + 1000 // truncated on the way in
	} else {
		h.Transport, h.Framing = item.TransportUnix, item.FramingDatagram
	}
	return item.New(h, []byte(line), -1)
}

func idx(it *item.Item) int { return int(it.Head.Time().Sub(base) / time.Microsecond) }

// A reingested item must be INDISTINGUISHABLE from the original: same uuid,
// same clock sample, same transport, same addresses, same TLS session, same
// original size. That is what makes the envelope reproduce byte for byte in
// the drain worker (SPEC §3.5, §12.2).
func TestRoundTripIsExact(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	orig := []*item.Item{
		mkItem(0, `<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] hello`),
		mkItem(1, `<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`),
		mkItem(2, "linha com bytes crus \x00\xff\xfe e nova linha \n no meio"),
		mkItem(3, ""),
		mkItem(4, "12 34\n"), // a raw line that looks like a record header
	}
	path, err := s.Write(orig)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Damaged() {
		t.Fatalf("clean file reported damaged: %+v", res)
	}
	if len(res.Items) != len(orig) {
		t.Fatalf("read %d of %d", len(res.Items), len(orig))
	}
	for i, got := range res.Items {
		if !reflect.DeepEqual(got.Head, orig[i].Head) {
			t.Fatalf("item %d head changed\n got %+v\nwant %+v", i, got.Head, orig[i].Head)
		}
		if !bytes.Equal(got.Raw, orig[i].Raw) {
			t.Fatalf("item %d raw line changed\n got %q\nwant %q", i, got.Raw, orig[i].Raw)
		}
	}
}

// SPEC §12.1: emg-<agent_uuid>-<unixnano>.bin, with the AgentUUID literally.
func TestFileName(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(p)
	if !strings.HasPrefix(name, "emg-"+agent+"-") || !strings.HasSuffix(name, ".bin") {
		t.Fatalf("name = %s", name)
	}
	stamp := strings.TrimSuffix(strings.TrimPrefix(name, "emg-"+agent+"-"), ".bin")
	if len(stamp) != 19 {
		t.Fatalf("stamp %q is not nineteen digits; lexicographic order would not be temporal", stamp)
	}
	if got := FileName(agent, time.Unix(0, 1759700000000000000)); got != "emg-"+agent+"-1759700000000000000.bin" {
		t.Fatalf("FileName = %s", got)
	}
}

// SPEC §12.2: lexicographic order of name, which is temporal per agent.
func TestListIsLexicographicAndTemporal(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	var paths []string
	for i := 0; i < 3; i++ {
		p, err := s.Write([]*item.Item{mkItem(i, "x")})
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
		time.Sleep(2 * time.Millisecond)
	}
	// Noise the listing must ignore.
	for _, n := range []string{"emg-x-1.bin.corrupt", "notes.txt", "emg-x-2.tmp"} {
		os.WriteFile(filepath.Join(s.Dir(), n), []byte("x"), 0o600)
	}
	os.Mkdir(filepath.Join(s.Dir(), "emg-dir-3.bin"), 0o750)

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

// A truncated file must yield its intact prefix and say how much it threw away.
// Losing the whole file because the tail was cut would discard messages an
// emitter was already told we had.
func TestTruncatedFileYieldsItsPrefix(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	path, err := s.Write([]*item.Item{mkItem(0, "primeira"), mkItem(1, "segunda"), mkItem(2, "terceira")})
	if err != nil {
		t.Fatal(err)
	}
	full, _ := os.ReadFile(path)
	for _, cut := range []int{len(full) - 1, len(full) - 10, len(full) / 2, len(full) / 3, 5, 1} {
		if cut < 0 {
			continue
		}
		dir := t.TempDir()
		p2 := filepath.Join(dir, "cut.bin")
		if err := os.WriteFile(p2, full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		res, err := NewStore(dir, agent).Read(p2)
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		if !res.Truncated || !res.Damaged() {
			t.Fatalf("cut at %d was not reported as truncated", cut)
		}
		if len(res.Items) >= 3 {
			t.Fatalf("cut at %d still produced %d items", cut, len(res.Items))
		}
		for i, it := range res.Items {
			if idx(it) != i {
				t.Fatalf("cut at %d: item %d is %d", cut, i, idx(it))
			}
		}
		if res.TruncatedBytes <= 0 || res.TruncatedBytes > int64(cut) {
			t.Fatalf("cut at %d: truncated bytes = %d", cut, res.TruncatedBytes)
		}
	}
}

// A header claiming gigabytes must become a truncation report, never an
// allocation.
func TestAbsurdDeclaredLengthDoesNotAllocate(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "emg-a-1.bin")
	if err := os.WriteFile(p, []byte("1073741824 1073741824\nshort"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := NewStore(dir, agent).Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Items) != 0 || res.TruncatedBytes != 27 {
		t.Fatalf("res = %+v", res)
	}
}

// A file that never has a newline is not read into one string to find one.
func TestHeaderLineIsBounded(t *testing.T) {
	data := bytes.Repeat([]byte("9"), 1<<20)
	res := &ReadResult{}
	if err := readFrom(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), res, false); err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || res.TruncatedBytes != int64(len(data)) {
		t.Fatalf("res = %+v", res)
	}
}

// Headers this program did not write are corruption, not a looser dialect.
func TestParseHeaderIsStrict(t *testing.T) {
	for _, bad := range []string{"1 2", "1  2\n", " 1 2\n", "1 2 \n", "+1 2\n", "-1 2\n", "1 -2\n",
		"1 2\r\n", "a 2\n", "1\n", "\n", "99999999999 1\n", "2147483649 0\n"} {
		if _, _, err := parseHeader([]byte(bad)); err == nil {
			t.Errorf("parseHeader(%q) accepted", bad)
		}
	}
	h, r, err := parseHeader([]byte("12 0\n"))
	if err != nil || h != 12 || r != 0 {
		t.Fatalf("parseHeader(12 0) = %d %d %v", h, r, err)
	}
}

// One undecodable record must not cost the records after it: the lengths
// located them exactly.
func TestUndecodableRecordIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	good, err := s.Write([]*item.Item{mkItem(0, "boa")})
	if err != nil {
		t.Fatal(err)
	}
	goodBytes, _ := os.ReadFile(good)
	os.Remove(good)

	// Exactly 7 head bytes and 4 raw bytes, so the record boundary is honest
	// and only the HEAD is unusable -- here a head of another codec version.
	// A mismatched length would desynchronise the next header, which is a
	// different failure.
	bad := []byte(`7 4` + "\n" + `{"v":2}abcd`)
	p := filepath.Join(dir, "emg-a-2.bin")
	if err := os.WriteFile(p, append(bad, goodBytes...), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Undecodable != 1 || res.UndecodableBytes != int64(len(bad)) || res.Truncated {
		t.Fatalf("res = %+v, want 1 undecodable record of %d bytes", res, len(bad))
	}
	if !res.Damaged() {
		t.Fatal("a file with an undecodable record must be quarantined, not removed")
	}
	if len(res.Items) != 1 || string(res.Items[0].Raw) != "boa" {
		t.Fatalf("items = %v", res.Items)
	}
}

func TestQuarantineKeepsTheEvidence(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p, _ := s.Write([]*item.Item{mkItem(0, "x")})
	dst, err := s.Quarantine(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(dst, ".bin.corrupt") {
		t.Fatalf("quarantined as %s", dst)
	}
	if _, err := os.Stat(dst); err != nil {
		t.Fatalf("quarantined file is gone: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("the original still exists")
	}
	// A quarantined file must not be picked up by the next boot.
	list, _ := s.List()
	if len(list) != 0 {
		t.Fatalf("List returned a quarantined file: %v", list)
	}
}

func TestRemove(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	p, _ := s.Write([]*item.Item{mkItem(0, "x")})
	if err := s.Remove(p); err != nil {
		t.Fatal(err)
	}
	if list, _ := s.List(); len(list) != 0 {
		t.Fatalf("listed after Remove: %v", list)
	}
}

func TestCount(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	s.Write([]*item.Item{mkItem(0, "a"), mkItem(1, "b")})
	s.Write([]*item.Item{mkItem(2, "c")})
	// A damaged file counts only what will really come back.
	p, _ := s.Write([]*item.Item{mkItem(3, "d"), mkItem(4, "e")})
	full, _ := os.ReadFile(p)
	os.WriteFile(p, full[:len(full)-1], 0o600)

	files, events, err := s.Count()
	if err != nil {
		t.Fatal(err)
	}
	if files != 3 || events != 4 {
		t.Fatalf("files=%d events=%d, want 3 and 4", files, events)
	}
}

// O_EXCL: a dump never appends to, or truncates, a file that already exists.
func TestWriteNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	// Fill every name the next few nanoseconds could produce is impractical;
	// instead prove the flag directly on a name that exists.
	p, err := s.Write([]*item.Item{mkItem(0, "first")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640); !os.IsExist(err) {
		t.Fatalf("O_EXCL open of an existing dump: %v", err)
	}
	res, _ := s.Read(p)
	if len(res.Items) != 1 || string(res.Items[0].Raw) != "first" {
		t.Fatal("dump changed")
	}
}

// Review finding F9: a directory fsync that fails fails the dump. Swallowed,
// a crash right after could leave the data on disk and its name not, while the
// process exits believing the messages were saved (SPEC §12.1, §12.3). The
// partial file is removed, as on every other write error.
func TestWriteFailsWhenTheDirectoryFsyncFails(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	orig := dirSync
	t.Cleanup(func() { dirSync = orig })
	dirSync = func(*os.File) error { return errors.New("input/output error") }

	p, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err == nil || p != "" {
		t.Fatalf("Write = %q %v, want an error", p, err)
	}
	if !strings.Contains(err.Error(), "input/output error") {
		t.Fatalf("err = %v, want the fsync cause", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a failed dump left %d files behind", len(entries))
	}

	// Quarantine reports it too; the rename itself already happened.
	dirSync = orig
	p, err = s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if dst, err := s.Quarantine(p); err == nil || dst == "" {
		t.Fatalf("Quarantine = %q %v, want the path and the fsync error", dst, err)
	}
}

func TestEmptyWriteCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p, err := s.Write(nil)
	if err != nil || p != "" {
		t.Fatalf("Write(nil) = %q %v", p, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("an empty dump created %d files", len(entries))
	}
}

func TestMissingDirIsNotAnError(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "nope"), agent)
	list, err := s.List()
	if err != nil || len(list) != 0 {
		t.Fatalf("List on a missing dir = %v %v", list, err)
	}
	if files, events, err := s.Count(); err != nil || files != 0 || events != 0 {
		t.Fatalf("Count on a missing dir = %d %d %v", files, events, err)
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

// FuzzRead is mandatory (SPEC §18.3). The decoder reads a file a crashing disk
// may have half written, so it must tolerate any byte sequence without
// panicking and without trusting a declared length, and everything it returns
// must be an item the converter can take.
func FuzzRead(f *testing.F) {
	s := NewStore(f.TempDir(), agent)
	p, err := s.Write([]*item.Item{
		mkItem(0, `<165>1 2026-09-12T14:33:01Z h a 1 - - msg`),
		mkItem(1, "\x00\xff raw"),
	})
	if err != nil {
		f.Fatal(err)
	}
	valid, _ := os.ReadFile(p)
	f.Add(valid)
	f.Add(valid[:len(valid)/2])
	f.Add([]byte(""))
	f.Add([]byte("\n"))
	f.Add([]byte("0 0\n"))
	f.Add([]byte("-1 -1\n"))
	f.Add([]byte("99999999999999999999 0\n"))
	f.Add([]byte("1073741824 1073741824\nx"))
	f.Add([]byte("4 4\nabcdefgh"))
	f.Add(append([]byte("7 4\n{\"v\":2}abcd"), valid...))

	f.Fuzz(func(t *testing.T, data []byte) {
		res := &ReadResult{}
		err := readFrom(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), res, false)
		if err != nil {
			t.Fatalf("readFrom on an in-memory reader returned %v", err)
		}
		if res.TruncatedBytes < 0 || res.TruncatedBytes > int64(len(data)) {
			t.Fatalf("truncated bytes out of range: %d of %d", res.TruncatedBytes, len(data))
		}
		if res.UndecodableBytes < 0 || res.UndecodableBytes > int64(len(data)) {
			t.Fatalf("undecodable bytes out of range: %d", res.UndecodableBytes)
		}
		var raw int64
		for _, it := range res.Items {
			if !it.Head.UUID.IsValid() {
				t.Fatal("an item came back without a valid uuid")
			}
			// Whatever came back re-encodes to a head that decodes to the same
			// thing: it is an item the rest of the pipeline can carry.
			again, err := item.DecodeHead(item.EncodeHead(nil, &it.Head))
			if err != nil || !reflect.DeepEqual(again, it.Head) {
				t.Fatalf("returned head does not round trip: %v", err)
			}
			raw += int64(len(it.Raw))
		}
		if raw > int64(len(data)) {
			t.Fatalf("returned %d raw bytes from %d bytes of input", raw, len(data))
		}
		// The counting path agrees with the reading path.
		cres := &ReadResult{}
		_ = readFrom(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), cres, true)
		if len(cres.Items) != len(res.Items) || cres.Undecodable != res.Undecodable || cres.Truncated != res.Truncated {
			t.Fatalf("count path disagrees: %d/%d/%v vs %d/%d/%v", len(cres.Items), cres.Undecodable,
				cres.Truncated, len(res.Items), res.Undecodable, res.Truncated)
		}
	})
}
