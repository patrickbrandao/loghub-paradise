package emergency

import (
	"errors"
	"os"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
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

// The abandon check also runs every 1024 records, so a large dump the abort
// counted does not finish its writes and its fsync before giving up
// (independent review of the port, 2026-10-13: only the start and the
// publish were proven). Abandoned at the second check -- record 1023 of 3000
// --, the temporary is removed and the check before the publish never runs.
func TestWriteGivesUpInTheMiddleOfALargeDump(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir, agent)
	items := make([]*item.Item, 3000)
	for i := range items {
		items[i] = mkItem(i, "m")
	}
	calls := 0
	s.SetAbandon(func() bool { calls++; return calls >= 2 })
	if p, err := s.Write(items); !errors.Is(err, ErrAbandoned) || p != "" {
		t.Fatalf("Write = %q, %v; want ErrAbandoned", p, err)
	}
	if calls != 2 {
		t.Fatalf("the abandon check ran %d times, want 2: the start and record 1023", calls)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("an abandoned dump left %d entries: %s", len(ents), ents[0].Name())
	}
	// Control: never abandoned, the start, two in the middle and the publish.
	calls = 0
	s.SetAbandon(func() bool { calls++; return false })
	if _, err := s.Write(items); err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("a full write of 3000 records ran %d checks, want 4", calls)
	}
}
