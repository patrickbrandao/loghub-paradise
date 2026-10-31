package source

import (
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/pcap/pcaptest"
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
	if err := os.WriteFile(p, []byte("\n  \n\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := NewFile(p, true)
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
	f, err := NewFile(p, true)
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

// A capture rewritten without documents between passes ends --loop instead
// of being reopened forever (passSent, not the total).
func TestLoopPcapEmptiedBetweenPassesEnds(t *testing.T) {
	src, dst := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2")
	frame := func(payload string) []pcaptest.Frame {
		ip := pcaptest.IP(src, dst, 17, pcaptest.UDP(40000, 12201, []byte(payload)), 1)
		return []pcaptest.Frame{{TS: time.Unix(1, 0), Data: pcaptest.Link(pcaptest.Ethernet, ip)}}
	}
	p := filepath.Join(t.TempDir(), "c.pcap")
	if err := os.WriteFile(p, pcaptest.Classic(pcaptest.Ethernet, frame(`{"a":1}`)), 0o600); err != nil {
		t.Fatal(err)
	}
	pc, err := NewPcap(p, PcapOptions{UDP: true, TCP: true, Loop: true})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	next := func() error {
		done := make(chan error, 1)
		go func() { _, err := pc.Next(1, time.Now()); done <- err }()
		select {
		case err := <-done:
			return err
		case <-time.After(3 * time.Second):
			t.Fatal("Next spins on a capture without documents")
			return nil
		}
	}
	if err := next(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, pcaptest.Classic(pcaptest.Ethernet, frame("not gelf")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := next(); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}
