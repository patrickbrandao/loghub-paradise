package emergency

import (
	"errors"
	"os"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// The directory that cannot even be opened for its fsync, after the unlink
// succeeded, is ErrNotDurable too: the file IS gone, and "cannot remove" would
// be false (ported from go-relplog-nats-server, independent review of
// 2026-10-13).
func TestRemoveWithAnUnopenableDirectoryIsNotDurable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root opens any directory")
	}
	dir := t.TempDir()
	s := NewStore(dir, agent)
	p, err := s.Write([]*item.Item{mkItem(0, "x")})
	if err != nil {
		t.Fatal(err)
	}
	// Write and search, no read: the unlink works, os.Open(dir) does not.
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	err = s.Remove(p)
	if !errors.Is(err, ErrNotDurable) {
		t.Fatalf("Remove = %v, want ErrNotDurable", err)
	}
	if _, serr := os.Lstat(p); !os.IsNotExist(serr) {
		t.Fatalf("the dump is still there: %v", serr)
	}
}

// SPEC §13.4: a dump the abort already counted as lost gives up and leaves
// nothing behind -- neither the final name, which the next boot would
// reingest (counted lost AND delivered), nor the temporary (ported from
// go-relplog-nats-server, independent review of 2026-10-13).
func TestWriteGivesUpWhenAbandoned(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	s.SetAbandon(func() bool { return true })
	if p, err := s.Write([]*item.Item{mkItem(0, "x")}); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned", p, err)
	}
	// Abandoned after the records were written, before the publish.
	calls := 0
	s.SetAbandon(func() bool { calls++; return calls > 1 })
	if p, err := s.Write([]*item.Item{mkItem(1, "a"), mkItem(2, "b")}); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned", p, err)
	}
	if calls < 2 {
		t.Fatalf("the abandon check ran %d times, want one before the publish too", calls)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("an abandoned dump left %d entries: %s", len(ents), ents[0].Name())
	}
	// Not abandoned: written as always.
	s.SetAbandon(func() bool { return false })
	if _, err := s.Write([]*item.Item{mkItem(3, "c")}); err != nil {
		t.Fatal(err)
	}
}

// The abandon check also runs while the records are written, every 1024 of
// them, so a large dump an abort counted lost stops early instead of writing
// to the end (go-syslog-server review, 2026-10-13, M4).
func TestWriteChecksTheAbandonWhileWriting(t *testing.T) {
	s := NewStore(t.TempDir(), agent)
	calls := 0
	s.SetAbandon(func() bool { calls++; return false })
	items := make([]*item.Item, 2048)
	for i := range items {
		items[i] = mkItem(i, "x")
	}
	if _, err := s.Write(items); err != nil {
		t.Fatal(err)
	}
	// Before the first record, after records 1024 and 2048, before the publish.
	if calls != 4 {
		t.Fatalf("the abandon check ran %d times for 2048 records, want 4", calls)
	}
}
