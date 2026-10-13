package emergency

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
)

// The tests of this file come from go-relplog-server (publish_test.go there,
// the S §22.1 items 24 and 25 and the "A portar" list of the R entry), adapted
// to the JSON Lines dump of this server: a quarantine whose directory fsync
// failed IS a quarantine, Remove fsyncs the directory, and Count reads what the
// gauges of SPEC §12.2 are recounted from.

// SPEC §12.2 (S §22.1 item 24): when the rename to .corrupt happened and only
// the directory fsync failed, the file IS quarantined. Quarantine returns the
// new name with an error the caller can tell apart (ErrNotDurable), so the
// boot logs the fsync at WARN and still emits the ERROR with the discarded
// bytes under the new name, instead of "cannot quarantine" with a path that no
// longer exists.
func TestQuarantineWithAFailedDirectoryFsyncIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	orig := dirSync
	t.Cleanup(func() { dirSync = orig })
	dirSync = func(*os.File) error { return errors.New("input/output error") }

	dst, err := s.Quarantine(p)
	if dst != p+corruptExt {
		t.Fatalf("Quarantine = %q, %v; want the new name %s", dst, err, p+corruptExt)
	}
	if !errors.Is(err, ErrNotDurable) || !strings.Contains(err.Error(), "input/output error") {
		t.Fatalf("err = %v, want ErrNotDurable with the fsync cause", err)
	}
	if _, serr := os.Stat(dst); serr != nil {
		t.Fatalf("the quarantined file is not at its new name: %v", serr)
	}
	if _, serr := os.Stat(p); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the dump is still at its old name: %v", serr)
	}
	if list, _ := s.List(); len(list) != 0 {
		t.Fatalf("List returns a quarantined file: %v", list)
	}

	// Control: a rename that fails is no quarantine, and says so -- no name,
	// and not the durability error.
	dirSync = orig
	dst, err = s.Quarantine(filepath.Join(dir, "emg-gone-1.jsonl"))
	if dst != "" || err == nil || errors.Is(err, ErrNotDurable) {
		t.Fatalf("Quarantine of a missing file = %q, %v; want no name and a rename error", dst, err)
	}
}

// Remove fsyncs the directory after the unlink: without it a power loss can
// bring a reingested dump back and the next boot delivers it twice. A failed
// fsync still removed the file, and says so with ErrNotDurable.
func TestRemoveSyncsTheDirectory(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	orig := dirSync
	t.Cleanup(func() { dirSync = orig })
	synced := 0
	dirSync = func(d *os.File) error { synced++; return orig(d) }
	if err := s.Remove(p); err != nil {
		t.Fatal(err)
	}
	if synced != 1 {
		t.Fatalf("%d directory fsyncs after Remove, want 1", synced)
	}

	dirSync = orig
	p = dump(t, s, []*item.Item{mkItem(1, "y")}).Path
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if err := s.Remove(p); !errors.Is(err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", err)
	}
	if _, serr := os.Stat(p); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the dump is still there: %v", serr)
	}
}

// Ported from go-relplog-server (publish_test.go there), which the independent
// review of 2026-10-13 found missing here: the dump is written under its hidden
// temporary name and published only once complete. At the moment of the
// publish nothing the boot would reingest or count is visible, and the
// temporary already reads back whole; a refused publish leaves nothing.
func TestDumpIsPublishedOnlyWhenComplete(t *testing.T) {
	defer func(orig func(string, string) error) { link = orig }(link)
	dir := t.TempDir()
	s := NewStore(dir, agent)
	items := []*item.Item{mkItem(0, "first"), mkItem(1, "second"), mkItem(2, "third")}

	called := false
	link = func(tmp, final string) error {
		called = true
		if base := filepath.Base(tmp); !strings.HasPrefix(base, "."+filePrefix) || !strings.HasSuffix(base, fileSuffix+tmpSuffix) {
			t.Errorf("temporary name %s is not hidden", base)
		}
		if filepath.Base(tmp) != "."+filepath.Base(final)+tmpSuffix {
			t.Errorf("temporary %s does not name its final %s", tmp, final)
		}
		if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the final name exists before the publish: %v", err)
		}
		// Nothing the boot would reingest is visible yet...
		if list, _ := s.List(); len(list) != 0 {
			t.Errorf("List sees a dump still being written: %v", list)
		}
		if files, _, _ := s.Count(); files != 0 {
			t.Errorf("Count sees a dump still being written: %d", files)
		}
		// ...and the temporary is already complete.
		res, err := s.Read(tmp)
		if err != nil || res.Damaged() || len(res.Records) != len(items) {
			t.Errorf("the temporary at publish time reads %+v, %v", res, err)
		}
		return os.Link(tmp, final)
	}
	w, err := s.Write(items, conv(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("the dump was never published: it was written under its final name")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(w.Path) {
		t.Fatalf("directory holds %v, want only the dump", entries)
	}
	if left, err := s.Leftovers(); err != nil || len(left) != 0 {
		t.Fatalf("a complete dump left %v, %v", left, err)
	}
	res, err := s.Read(w.Path)
	if err != nil || res.Damaged() || len(res.Records) != 3 {
		t.Fatalf("the published dump reads %+v, %v", res, err)
	}

	// A failed publish leaves nothing: neither a final name nor the temporary.
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: os.ErrExist} }
	if w2, err := s.Write([]*item.Item{mkItem(3, "x")}, conv(), true); err == nil || w2.Path != "" {
		t.Fatalf("Write with a refused publish = %q, %v", w2.Path, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("a failed publish left %d entries, want only the first dump", len(entries))
	}
}

// Independent review of 2026-10-13: the directory fsync after the unlink starts
// with an open of the directory, which can fail on its own -- here a directory
// that can be written but not read. The file IS removed, and the error says so
// with ErrNotDurable; a bare error made the boot log "cannot remove ... it will
// be delivered again", which was false.
func TestRemoveWithAnUnopenableDirectoryIsRemoved(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens any directory")
	}
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p := dump(t, s, []*item.Item{mkItem(0, "x")}).Path
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	err := s.Remove(p)
	if !errors.Is(err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", err)
	}
	os.Chmod(dir, 0o700)
	if _, serr := os.Stat(p); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the dump is still there: %v", serr)
	}
}

// SPEC §12.2 (S §22.1 item 25): the gauges are recounted from the directory.
// Every listed file counts, an unreadable one included; its envelopes count
// only when a reingestion would deliver them -- a cut tail and an invalid line
// do not.
func TestCount(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	dump(t, s, []*item.Item{mkItem(0, "a"), mkItem(1, "b")})
	dump(t, s, []*item.Item{mkItem(2, "c")})
	// A damaged file: its last line loses the '\n', and a garbage line goes
	// in front.
	p := dump(t, s, []*item.Item{mkItem(3, "d"), mkItem(4, "e")}).Path
	full, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append([]byte("not an envelope\n"), full[:len(full)-1]...), 0o600); err != nil {
		t.Fatal(err)
	}
	// An unreadable file is still a pending file, with no known envelopes.
	locked := filepath.Join(dir, FileName(agent, base.Add(1e9)))
	if err := os.WriteFile(locked, full, 0o000); err != nil {
		t.Fatal(err)
	}
	wantFiles := 4
	if f, err := os.Open(locked); err == nil {
		f.Close() // running as root: the mode does not lock it
		wantFiles, full = 4, nil
	}

	files, events, err := s.Count()
	if err != nil {
		t.Fatal(err)
	}
	wantEvents := int64(4)
	if full == nil {
		wantEvents += 2
	}
	if files != wantFiles || events != wantEvents {
		t.Fatalf("files=%d events=%d, want %d and %d", files, events, wantFiles, wantEvents)
	}
	if f, e, err := NewStore(filepath.Join(dir, "absent"), agent).Count(); err != nil || f != 0 || e != 0 {
		t.Fatalf("Count of a missing directory = %d, %d, %v", f, e, err)
	}
}
