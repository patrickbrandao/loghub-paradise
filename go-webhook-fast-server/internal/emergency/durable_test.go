package emergency

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/envelope"
)

// The tests of this file come from the syslog/RELP family (durable_test.go and
// publish_test.go of go-syslog-server and go-syslog-pg-server, from the
// independent reviews of go-relplog-server and go-relplog-nats-server),
// ported 2026-10-13: Remove and Quarantine fsync the directory and say when
// only that fsync failed, and a dump the abort already counted gives up.

// failDirSync makes every directory fsync fail until the test ends.
func failDirSync(t *testing.T) {
	t.Helper()
	orig := dirSync
	t.Cleanup(func() { dirSync = orig })
	dirSync = func(*os.File) error { return errors.New("input/output error") }
}

// Remove fsyncs the directory after the unlink: without it a power loss can
// bring a reingested dump back and the next boot delivers it twice. A failed
// fsync still removed the file, and says so with ErrNotDurable.
func TestRemoveSyncsTheDirectory(t *testing.T) {
	s := newStore(t)
	p, err := s.Write([]*envelope.Item{item(nil)})
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

	dirSync = orig
	if p, err = s.Write([]*envelope.Item{item(nil)}); err != nil {
		t.Fatal(err)
	}
	failDirSync(t)
	if err := s.Remove(p); !errors.Is(err, ErrNotDurable) || !strings.Contains(err.Error(), "input/output error") {
		t.Fatalf("Remove = %v, want ErrNotDurable with the fsync cause", err)
	}
	if _, serr := os.Lstat(p); !os.IsNotExist(serr) {
		t.Fatalf("the dump is still there: %v", serr)
	}
	// Control: an unlink that fails is not a durability doubt.
	if err := s.Remove(p); err == nil || errors.Is(err, ErrNotDurable) {
		t.Fatalf("Remove of a missing file = %v, want the unlink's error", err)
	}
}

// The directory that cannot even be opened for its fsync, after the unlink
// succeeded, is ErrNotDurable too: the file IS gone, and "cannot remove" would
// be false (go-relplog-nats-server's independent review, 2026-10-13).
func TestRemoveWithAnUnopenableDirectoryIsNotDurable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens any directory")
	}
	s := newStore(t)
	p, err := s.Write([]*envelope.Item{item(nil)})
	if err != nil {
		t.Fatal(err)
	}
	// Write and search, no read: the unlink works, os.Open(dir) does not.
	if err := os.Chmod(s.Dir(), 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(s.Dir(), 0o700) })
	if err := s.Remove(p); !errors.Is(err, ErrNotDurable) {
		t.Fatalf("Remove = %v, want ErrNotDurable", err)
	}
	if _, serr := os.Lstat(p); !os.IsNotExist(serr) {
		t.Fatalf("the dump is still there: %v", serr)
	}
}

// When the rename happened and only the directory fsync failed, the file IS
// set aside: Quarantine and SetAsideLegacy return the new name with an error
// the caller can tell apart (ErrNotDurable), so the boot logs the fsync at
// WARN and still reports the file under its new name.
func TestRenameAsideWithAFailedDirectoryFsyncHappened(t *testing.T) {
	for _, tc := range []struct {
		name string
		ext  string
		op   func(*Store, string) (string, error)
	}{
		{"Quarantine", corruptExt, (*Store).Quarantine},
		{"SetAsideLegacy", legacyExt, (*Store).SetAsideLegacy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore(t)
			p, err := s.Write([]*envelope.Item{item(nil)})
			if err != nil {
				t.Fatal(err)
			}
			orig := dirSync
			synced := 0
			dirSync = func(d *os.File) error { synced++; return orig(d) }
			dst, err := tc.op(s, p)
			dirSync = orig
			if err != nil || dst != p+tc.ext || synced != 1 {
				t.Fatalf("%s = %q, %v with %d directory fsyncs; want %s, nil, 1", tc.name, dst, err, synced, p+tc.ext)
			}

			if p, err = s.Write([]*envelope.Item{item(nil)}); err != nil {
				t.Fatal(err)
			}
			failDirSync(t)
			dst, err = tc.op(s, p)
			if dst != p+tc.ext || !errors.Is(err, ErrNotDurable) {
				t.Fatalf("%s = %q, %v; want the new name and ErrNotDurable", tc.name, dst, err)
			}
			if _, serr := os.Stat(dst); serr != nil {
				t.Fatalf("the file is not at its new name: %v", serr)
			}
			if list, _ := s.List(); len(list) != 0 {
				t.Fatalf("List returns a file set aside: %v", basenames(list))
			}
			// Control: a rename that fails sets nothing aside, and says so --
			// no name, and not the durability error.
			dst, err = tc.op(s, filepath.Join(s.Dir(), "emg-gone-1.bin"))
			if dst != "" || err == nil || errors.Is(err, ErrNotDurable) {
				t.Fatalf("%s of a missing file = %q, %v; want no name and a rename error", tc.name, dst, err)
			}
		})
	}
}

// The dump itself keeps the directory fsync best effort (SPEC §9.1): a
// complete dump under its final name is not thrown away because the fsync that
// follows failed.
func TestWriteKeepsTheDumpWhenTheDirectoryFsyncFails(t *testing.T) {
	s := newStore(t)
	failDirSync(t)
	p, err := s.Write([]*envelope.Item{item(nil), item(nil)})
	if err != nil {
		t.Fatalf("Write = %v, want the dump kept", err)
	}
	if res, err := s.Read(p); err != nil || len(res.Items) != 2 {
		t.Fatalf("Read = %+v, %v", res, err)
	}
}

// SPEC §10.3: a dump the abort already counted as lost gives up and leaves
// nothing behind -- neither the final name, which the next boot would
// reingest (counted lost AND delivered), nor the temporary.
func TestWriteGivesUpWhenAbandoned(t *testing.T) {
	s := newStore(t)
	s.SetAbandon(func() bool { return true })
	if p, err := s.Write([]*envelope.Item{item(nil)}); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned", p, err)
	}
	// Abandoned after the records were written, before the publish.
	calls := 0
	s.SetAbandon(func() bool { calls++; return calls > 1 })
	if p, err := s.Write([]*envelope.Item{item(nil), item(nil)}); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned", p, err)
	}
	if calls != 2 {
		t.Fatalf("the abandon check ran %d times, want one before the first record and one before the publish", calls)
	}
	// Checked every 1024 records too: a dump the size of a ring is seconds of
	// writing, and the abort's os.Exit does not wait for it.
	many := make([]*envelope.Item, 3000)
	for i := range many {
		many[i] = item(nil)
	}
	calls = 0
	s.SetAbandon(func() bool { calls++; return false })
	p, err := s.Write(many)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("the abandon check ran %d times for 3000 records, want 4 (start, 1024, 2048, publish)", calls)
	}
	if err := s.Remove(p); err != nil {
		t.Fatal(err)
	}
	calls = 0
	s.SetAbandon(func() bool { calls++; return calls == 2 })
	if p, err := s.Write(many); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned at the check after 1024 records", p, err)
	}
	if ents, _ := os.ReadDir(s.Dir()); len(ents) != 0 {
		t.Fatalf("an abandoned dump left %d entries: %s", len(ents), ents[0].Name())
	}
	// Not abandoned: written as always.
	s.SetAbandon(func() bool { return false })
	if _, err := s.Write([]*envelope.Item{item(nil)}); err != nil {
		t.Fatal(err)
	}
}
