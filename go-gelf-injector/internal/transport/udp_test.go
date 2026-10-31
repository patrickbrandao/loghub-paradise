package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
)

// The chunks on the wire, byte by byte: magic, one id for the message,
// sequence 0..n-1, count n, every datagram at most the chunk size, and the
// payload the gzip of the document.
func TestUDPChunkWire(t *testing.T) {
	ln, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	d, err := Dialer(Options{Network: "udp", Target: ln.LocalAddr().String(), Timeout: time.Second,
		Compress: gelf.Gzip, ChunkSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	c, err := d(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	doc := make([]byte, 0, 2000)
	for i := 0; len(doc) < 2000; i++ {
		doc = append(doc, byte('a'+i*7%26), byte('0'+i*13%10))
	}
	var got []string
	if err := c.Send(1, &source.Message{Data: doc}, func(o string, _ bool) { got = append(got, o) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != Sent {
		t.Fatalf("outcomes %v", got)
	}
	var payload []byte
	var id []byte
	n := -1
	buf := make([]byte, 65536)
	for seq := 0; n < 0 || seq < n; seq++ {
		_ = ln.SetReadDeadline(time.Now().Add(time.Second))
		k, _, err := ln.ReadFrom(buf)
		if err != nil {
			t.Fatal(err)
		}
		b := buf[:k]
		if k > 100 || b[0] != 0x1e || b[1] != 0x0f || int(b[10]) != seq {
			t.Fatalf("chunk %d: % x", seq, b[:12])
		}
		if id == nil {
			id, n = append([]byte(nil), b[2:10]...), int(b[11])
		} else if !bytes.Equal(id, b[2:10]) || int(b[11]) != n {
			t.Fatalf("chunk %d: id or count changed", seq)
		}
		payload = append(payload, b[12:]...)
	}
	zr, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(zr)
	if !bytes.Equal(out, doc) {
		t.Fatal("reassembled payload differs")
	}
	// A message that fits the chunk size goes in one plain datagram.
	if err := c.Send(2, &source.Message{Data: []byte("{}")}, func(string, bool) {}); err != nil {
		t.Fatal(err)
	}
	k, _, _ := ln.ReadFrom(buf)
	if gelf.IsChunk(buf[:k]) || gelf.Detect(buf[:k]) != gelf.Gzip {
		t.Fatalf("small message: % x", buf[:k])
	}
	c.Close(context.Background())
}

func TestStreamNulFraming(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		b, _ := io.ReadAll(c)
		got <- b
	}()
	d, _ := Dialer(Options{Network: "tcp", Target: ln.Addr().String(), Timeout: time.Second})
	c, err := d(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var outs []string
	done := func(o string, _ bool) { outs = append(outs, o) }
	c.Send(1, &source.Message{Data: []byte(`{"a":1}`)}, done)
	c.Send(2, &source.Message{Data: []byte("{\"a\":\x00}")}, done)
	c.Send(3, &source.Message{Data: []byte(`{"b":2}`)}, done)
	c.Close(context.Background())
	if b := <-got; string(b) != "{\"a\":1}\x00{\"b\":2}\x00" {
		t.Fatalf("%q", b)
	}
	if len(outs) != 3 || outs[0] != NulByte || outs[1] != Sent || outs[2] != Sent {
		t.Fatalf("outcomes %v", outs)
	}
}

func TestCompressionRefusedOnTCP(t *testing.T) {
	if _, err := Dialer(Options{Network: "tcp", Target: "x:1", Compress: gelf.Gzip}); err == nil {
		t.Fatal("gzip over TCP accepted")
	}
}

// partialConn takes the first n bytes of a write and fails the rest.
type partialConn struct {
	net.Conn
	n int
}

func (p *partialConn) SetWriteDeadline(time.Time) error { return nil }
func (p *partialConn) Write(b []byte) (int, error) {
	return min(p.n, len(b)), errors.New("i/o timeout")
}

// A partial write: frames wholly written (NUL included) are sent, the cut
// one and those after it are write_error (review-gi #6).
func TestStreamPartialWrite(t *testing.T) {
	var got []string
	done := func(o string, _ bool) { got = append(got, o) }
	// "{}\0" = 3 bytes each: 7 bytes written = frames 1 and 2 whole, 3 cut.
	s := &streamConn{c: &partialConn{n: 7}, timeout: time.Second}
	for range 4 {
		if err := s.Send(0, &source.Message{Data: []byte("{}")}, done); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err == nil {
		t.Fatal("Flush hid the error")
	}
	if want := []string{Sent, Sent, WriteError, WriteError}; !slices.Equal(got, want) {
		t.Fatalf("outcomes %v, want %v", got, want)
	}
	// Exactly at a frame end: the frame is whole.
	got = nil
	s = &streamConn{c: &partialConn{n: 6}, timeout: time.Second}
	for range 3 {
		s.Send(0, &source.Message{Data: []byte("{}")}, done)
	}
	s.Flush()
	if want := []string{Sent, Sent, WriteError}; !slices.Equal(got, want) {
		t.Fatalf("outcomes %v, want %v", got, want)
	}
}
