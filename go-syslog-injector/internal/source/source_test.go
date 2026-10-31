package source

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// nextOrHang calls Next with a deadline: the bug this guards against is a
// --loop that reopens a document-less file forever.
func nextOrHang(t *testing.T, f *File) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := f.Next(1, time.Now())
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Next spins: --loop on a file without documents never ends")
		return nil
	}
}

func TestLoopBlankFileEnds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "blank.txt")
	if err := os.WriteFile(p, []byte("\n\r\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := NewFile(p, true, Header{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := nextOrHang(t, f); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
	if f.Stats()["file_empty_lines"] == 0 {
		t.Fatalf("stats %v", f.Stats())
	}
}

func TestLoopFileEmptiedBetweenPassesEnds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "one.txt")
	if err := os.WriteFile(p, []byte("doc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := NewFile(p, true, Header{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := nextOrHang(t, f); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := nextOrHang(t, f); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}
