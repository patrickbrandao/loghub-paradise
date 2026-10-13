package emergency

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
)

// SPEC §12.1 (§22 pendência 23): a dump is written under a hidden temporary name
// and only gets the name the boot reingests once it is complete and fsynced.
// Written in place, a process killed after a flush that ended between two
// records left a file that read back clean, was delivered in part and then
// removed; and another instance sharing the directory could take it while it
// was being written. The test looks at the directory from inside the publish.
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
		if err != nil || res.Damaged() || len(res.Items) != len(items) {
			t.Errorf("the temporary at publish time reads %+v, %v", res, err)
		}
		return os.Link(tmp, final)
	}
	p, err := s.Write(items)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("the dump was never published: it was written under its final name")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(p) {
		t.Fatalf("directory holds %v, want only the dump", entries)
	}
	if left, err := s.Leftovers(); err != nil || len(left) != 0 {
		t.Fatalf("a complete dump left %v, %v", left, err)
	}
	res, err := s.Read(p)
	if err != nil || res.Damaged() || len(res.Items) != 3 || string(res.Items[2].Raw) != "third" {
		t.Fatalf("the published dump reads %+v, %v", res, err)
	}

	// A failed publish leaves nothing: neither a final name nor the temporary.
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: os.ErrExist} }
	if p2, err := s.Write([]*item.Item{mkItem(3, "x")}); err == nil || p2 != "" {
		t.Fatalf("Write with a refused publish = %q, %v", p2, err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("a failed publish left %d entries, want only the first dump", len(entries))
	}
}

// A file system without hard links (vfat, exFAT, the object-store FUSE
// mounts) refuses Link whatever the names: every dump would fail there, the
// whole ring lost, where writing in place had worked. The fallback renames
// after checking the final name is free (SPEC §12.1). It never renames over an
// existing dump, a Link refused because the name exists is not a reason to
// try, and the directory fsync still decides the renamed dump as it decides a
// linked one (SPEC §12.3). Ported from go-syslog-nats-server.
func TestWriteOnAFileSystemWithoutHardLinks(t *testing.T) {
	defer func(orig func(string, string) error) { link = orig }(link)
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: syscall.ENOTSUP} }

	dir := t.TempDir()
	s := NewStore(dir, agent)
	first := mkItem(0, "first")
	p, err := s.Write([]*item.Item{first, mkItem(1, "second")})
	if err != nil {
		t.Fatalf("a dump on a file system without hard links failed: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != filepath.Base(p) {
		t.Fatalf("directory holds %v, want only the dump", entries)
	}
	got, err := s.Read(p)
	if err != nil || len(got.Items) != 2 || got.Damaged() || got.Items[0].Head.UUID != first.Head.UUID {
		t.Fatalf("the renamed dump reads %+v, err %v", got, err)
	}

	// The directory fsync still decides: failed, the renamed dump goes too.
	orig := dirSync
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if _, err := s.Write([]*item.Item{mkItem(2, "x")}); err == nil {
		t.Error("a renamed dump whose directory fsync failed was reported saved")
	}
	dirSync = orig
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Errorf("a failed renamed dump left %d entries, want only the first dump", len(entries))
	}

	// The fallback never replaces a dump that has the final name.
	tmp := filepath.Join(dir, ".emg-x-1.bin.tmp")
	final := filepath.Join(dir, "emg-x-1.bin")
	if err := os.WriteFile(tmp, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := publish(tmp, final); err == nil {
		t.Error("the fallback renamed over an existing dump")
	}
	if b, _ := os.ReadFile(final); string(b) != "old" {
		t.Errorf("the existing dump now holds %q", b)
	}
	// A Link refused because the name exists is final: no rename is tried.
	link = func(o, n string) error { return &os.LinkError{Op: "link", Old: o, New: n, Err: os.ErrExist} }
	other := filepath.Join(dir, "emg-x-2.bin")
	if err := publish(tmp, other); !errors.Is(err, os.ErrExist) {
		t.Errorf("a Link refused for an existing name fell back to a rename: %v", err)
	}
	if _, err := os.Stat(other); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s exists after a refused Link: %v", other, err)
	}
}

// SPEC §12.1: a temporary that is the same file as a dump under its final
// name is the twin of a crash between link and removal, and goes; any other is
// an interrupted dump, reported and left alone -- never read, never removed.
func TestLeftovers(t *testing.T) {
	s := NewStore(filepath.Join(t.TempDir(), "emergency"), agent)
	if left, err := s.Leftovers(); err != nil || len(left) != 0 {
		t.Fatalf("Leftovers of a missing directory = %v, %v", left, err)
	}
	path, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
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
	// Not a dump temporary at all: ignored.
	if err := os.WriteFile(filepath.Join(s.Dir(), ".probe-x"), nil, 0o600); err != nil {
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
	for _, p := range want {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("an interrupted dump was removed: %v", err)
		}
	}
	if b, _ := os.ReadFile(cut); string(b) != "half" {
		t.Errorf("an interrupted dump was changed: %q", b)
	}
	// The interrupted ones are never reingested.
	list, _ := s.List()
	for _, p := range list {
		if strings.HasPrefix(filepath.Base(p), ".") {
			t.Errorf("List returned a temporary: %s", p)
		}
	}
}

// SPEC §12.2 (§22 pendência 24): when the rename to .corrupt happened and only
// the directory fsync failed, the file IS quarantined. Quarantine returns the
// new name with an error the caller can tell apart (ErrNotDurable), so the
// boot logs the fsync at WARN and still emits the ERROR with the discarded
// bytes under the new name, instead of "cannot quarantine" with a path that no
// longer exists.
func TestQuarantineWithAFailedDirectoryFsyncIsQuarantined(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
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
	dst, err = s.Quarantine(filepath.Join(dir, "emg-gone-1.bin"))
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
	p, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
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

	p, err = s.Write([]*item.Item{mkItem(1, "y")})
	if err != nil {
		t.Fatal(err)
	}
	dirSync = func(*os.File) error { return errors.New("input/output error") }
	if err := s.Remove(p); !errors.Is(err, ErrNotDurable) {
		t.Fatalf("err = %v, want ErrNotDurable", err)
	}
	if _, serr := os.Stat(p); !errors.Is(serr, os.ErrNotExist) {
		t.Fatalf("the dump is still there: %v", serr)
	}
}
