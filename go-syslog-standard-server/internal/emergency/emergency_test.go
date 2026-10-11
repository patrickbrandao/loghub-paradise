package emergency

import (
	"bufio"
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
)

func item(i int, line string) *envelope.Item {
	h := envelope.Head{
		ID:        fmt.Sprintf("019823f1-7a2c-7b3d-8e4f-%012d", i),
		Transport: envelope.TransportRELP,
		Framing:   envelope.FramingRELP,
		Listener:  ":20514",
		PeerAddr:  netip.MustParseAddr("10.0.0.7"),
		PeerPort:  40000,
		PRI:       165,
		FromWire:  true,
		Truncated: i%2 == 0,
	}
	h.SetTime(time.Unix(1757116800, int64(i)*1000))
	return envelope.NewItem(h, []byte(line), -1)
}

// A reingested item must be INDISTINGUISHABLE from the original: same id, same
// reception instant, same transport, same peer, same effective PRI. That is what
// makes the envelope reproduce byte for byte in the drain worker.
func TestRoundTripIsExact(t *testing.T) {
	s := NewStore(t.TempDir(), "sls-prod-a1")
	orig := []*envelope.Item{
		item(0, `<165>1 2026-09-12T14:33:01Z web-01 nginx 1234 ID47 [a@1 k="v"] hello`),
		item(1, `<34>Oct 11 22:14:15 mymachine su[230]: 'su root' failed`),
		item(2, "linha com bytes crus \x00\xff\xfe e nova linha \n no meio"),
		item(3, ""),
	}
	path, err := s.Write(orig)
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated || res.Undecodable != 0 {
		t.Fatalf("clean file reported truncated=%v undecodable=%d", res.Truncated, res.Undecodable)
	}
	if len(res.Items) != len(orig) {
		t.Fatalf("read %d of %d", len(res.Items), len(orig))
	}
	for i, got := range res.Items {
		if got.Head != orig[i].Head {
			t.Fatalf("item %d head changed\n got %+v\nwant %+v", i, got.Head, orig[i].Head)
		}
		if !bytes.Equal(got.Raw, orig[i].Raw) {
			t.Fatalf("item %d raw line changed\n got %q\nwant %q", i, got.Raw, orig[i].Raw)
		}
	}
}

func TestListIsChronological(t *testing.T) {
	s := NewStore(t.TempDir(), "node")
	var paths []string
	for i := 0; i < 3; i++ {
		p, err := s.Write([]*envelope.Item{item(i, "x")})
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
		time.Sleep(2 * time.Millisecond)
	}
	got, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("listed %d", len(got))
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
	s := NewStore(t.TempDir(), "node")
	path, err := s.Write([]*envelope.Item{item(0, "primeira"), item(1, "segunda"), item(2, "terceira")})
	if err != nil {
		t.Fatal(err)
	}
	full, _ := os.ReadFile(path)
	for _, cut := range []int{len(full) - 1, len(full) - 10, len(full) / 2, len(full) / 3, 5, 1} {
		if cut < 0 {
			continue
		}
		p2 := filepath.Join(t.TempDir(), "cut.bin")
		if err := os.WriteFile(p2, full[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		s2 := NewStore(filepath.Dir(p2), "node")
		res, err := s2.Read(p2)
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		if cut < len(full) && !res.Truncated && len(res.Items) == 3 {
			t.Fatalf("cut at %d was not reported as truncated", cut)
		}
		for _, it := range res.Items {
			if it.Head.ID == "" {
				t.Fatalf("cut at %d produced an item with no id", cut)
			}
		}
	}
}

// A header claiming a gigabyte must become a truncation report, never an
// allocation.
func TestAbsurdDeclaredLengthDoesNotAllocate(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "emg-node-1.bin")
	if err := os.WriteFile(p, []byte("1073741824 1073741824\nshort"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := NewStore(dir, "node").Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Items) != 0 {
		t.Fatalf("res = %+v", res)
	}
}

// One undecodable record must not cost the records after it: the lengths located
// them exactly.
func TestUndecodableRecordIsSkipped(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "node")
	good, err := s.Write([]*envelope.Item{item(0, "boa")})
	if err != nil {
		t.Fatal(err)
	}
	goodBytes, _ := os.ReadFile(good)
	os.Remove(good)

	// Exactly 8 head bytes and 4 raw bytes, so the record boundary is honest
	// and only the HEAD is unusable. A mismatched length would desynchronise
	// the next header, which is a different failure.
	bad := []byte("8 4\nnot!jsonabcd")
	p := filepath.Join(dir, "emg-node-2.bin")
	if err := os.WriteFile(p, append(bad, goodBytes...), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := s.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	if res.Undecodable != 1 {
		t.Fatalf("undecodable = %d, want 1", res.Undecodable)
	}
	if len(res.Items) != 1 || string(res.Items[0].Raw) != "boa" {
		t.Fatalf("items = %v", res.Items)
	}
}

func TestQuarantineKeepsTheEvidence(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "node")
	p, _ := s.Write([]*envelope.Item{item(0, "x")})
	dst, err := s.Quarantine(p)
	if err != nil {
		t.Fatal(err)
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

func TestCount(t *testing.T) {
	s := NewStore(t.TempDir(), "node")
	s.Write([]*envelope.Item{item(0, "a"), item(1, "b")})
	s.Write([]*envelope.Item{item(2, "c")})

	files, events, err := s.Count()
	if err != nil {
		t.Fatal(err)
	}
	if files != 2 || events != 3 {
		t.Fatalf("files=%d events=%d, want 2 and 3", files, events)
	}
}

func TestEmptyWriteCreatesNothing(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, "node")
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
	s := NewStore(filepath.Join(t.TempDir(), "nope"), "node")
	list, err := s.List()
	if err != nil || len(list) != 0 {
		t.Fatalf("List on a missing dir = %v %v", list, err)
	}
}

func TestSanitizeNodeID(t *testing.T) {
	for in, want := range map[string]string{
		"":            "node",
		"sls-prod-a1": "sls_prod_a1",
		"a/b:c":       "a_b_c",
		"ok_1":        "ok_1",
	} {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

// FuzzRead is mandatory (SPEC §17.2). The decoder reads a file a crashing disk
// may have half written, so it must tolerate any byte sequence without panicking
// and without trusting a declared length.
func FuzzRead(f *testing.F) {
	s := NewStore(f.TempDir(), "node")
	p, err := s.Write([]*envelope.Item{
		item(0, `<165>1 2026-09-12T14:33:01Z h a 1 - - msg`),
		item(1, "\x00\xff raw"),
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

	f.Fuzz(func(t *testing.T, data []byte) {
		res := &ReadResult{}
		err := readFrom(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), res)
		if err != nil {
			return
		}
		if res.TruncatedBytes < 0 {
			t.Fatalf("negative truncated bytes: %d", res.TruncatedBytes)
		}
		for _, it := range res.Items {
			if it.Head.ID == "" {
				t.Fatal("an item came back with no id")
			}
			// A clamped priority must still be in range, or the destination
			// gets a row outside every index it has.
			if it.Head.PRI < 0 || it.Head.PRI > 191 {
				t.Fatalf("pri out of range: %d", it.Head.PRI)
			}
			_ = it.Head.Facility()
			_ = it.Head.Severity()
		}
	})
}
