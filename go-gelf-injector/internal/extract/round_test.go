package extract

import (
	"net/netip"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
)

// tcpdump -i any: the copies of one datagram are dropped, a sender's own
// repeats are not (SPEC §6.3.1, item 2). The repeats here keep the same IP
// identification (an IPv4 with DF may send 0 every time), the worst case:
// only the vantage tells them apart in auto.
func TestDatagramCopies(t *testing.T) {
	src := netip.MustParseAddrPort("127.0.0.1:40000")
	const out, in = 1<<8 | 4, 1<<8 | 0 // lo, outgoing / to us (SLL2)
	t0 := time.Unix(100, 0)
	at := func(us int) time.Time { return t0.Add(time.Duration(us) * time.Microsecond) }
	type dg struct {
		v    uint64
		ipid uint32
		doc  string
		us   int
	}
	a1 := `{"a":1}`
	cases := []struct {
		name string
		mode DedupMode
		in   []dg
		want int
	}{
		{"loopback out+in", DedupAuto, []dg{{out, 7, a1, 0}, {in, 7, a1, 3}}, 1},
		{"sent twice", DedupAuto, []dg{{out, 7, a1, 0}, {in, 7, a1, 3}, {out, 7, a1, 10}, {in, 7, a1, 12}}, 2},
		{"sent twice, copies late", DedupAuto, []dg{{out, 7, a1, 0}, {out, 7, a1, 1}, {in, 7, a1, 2}, {in, 7, a1, 3}}, 2},
		{"one copy lost", DedupAuto, []dg{{out, 7, a1, 0}, {in, 7, a1, 2}, {in, 7, a1, 9}}, 2},
		{"single vantage repeats", DedupAuto, []dg{{0, 7, a1, 0}, {0, 7, a1, 1}, {0, 7, a1, 2}}, 3},
		{"another IP id", DedupAuto, []dg{{out, 7, a1, 0}, {in, 8, a1, 1}}, 2},
		{"different documents", DedupAuto, []dg{{out, 7, a1, 0}, {in, 7, `{"a":2}`, 1}}, 2},
		{"past the window", DedupAuto, []dg{{out, 7, a1, 0}, {in, 7, a1, 60_000}}, 2},
		// on is the vantage rule since the review of 2026-11-07 (alignment
		// 8(b)): a repeat at the same vantage is a message, not a copy.
		{"on: same vantage repeats", DedupOn, []dg{{0, 7, a1, 0}, {0, 7, a1, 1}, {0, 8, a1, 2}}, 3},
		{"on: loopback out+in", DedupOn, []dg{{out, 7, a1, 0}, {in, 7, a1, 3}}, 1},
		{"on: past the window", DedupOn, []dg{{0, 7, a1, 0}, {0, 7, a1, 60_000}}, 2},
		{"off", DedupOff, []dg{{out, 7, a1, 0}, {in, 7, a1, 3}}, 2},
	}
	for _, c := range cases {
		var st Stats
		d := &Datagrams{St: &st, Strict: true, R: gelf.NewReassembler(), Dedup: c.mode}
		n := 0
		for _, x := range c.in {
			if _, ok := d.AddFrom(src, x.ipid, x.v, []byte(x.doc), at(x.us)); ok {
				n++
			}
		}
		if n != c.want || st.DatagramsDup != uint64(len(c.in)-c.want) {
			t.Errorf("%s: %d documents, %d copies dropped; want %d", c.name, n, st.DatagramsDup, c.want)
		}
	}
	// Another source port with the same bytes is another datagram.
	var st Stats
	d := &Datagrams{St: &st, Strict: true, R: gelf.NewReassembler()}
	d.AddFrom(src, 7, out, []byte(a1), t0)
	if _, ok := d.AddFrom(netip.MustParseAddrPort("127.0.0.1:40001"), 7, in, []byte(a1), t0); !ok {
		t.Fatal("other source taken as a copy")
	}
}

// The last frame without NUL of a closed direction is a document as the
// server takes it at EOF; of a direction the capture cut, only if it looks
// whole (SPEC §6.4, item 5).
func TestTailClosedVersusCut(t *testing.T) {
	for _, c := range []struct {
		tail         string
		closed, cut  int // documents emitted (of 2) when closed / cut
		lostWhenCut  uint64
		notGELFClose uint64
	}{
		{`{"b":2}`, 2, 2, 0, 0},
		{`{"b":2} `, 2, 2, 0, 0},
		{`{"b":"x"`, 2, 1, 1, 0}, // malformed on purpose at the end
		{`{"b":2}x`, 2, 1, 1, 0}, // trailing garbage inside the frame
		{`not gelf`, 1, 1, 1, 1}, // strict: not an object
	} {
		in := []byte("{\"a\":1}\x00" + c.tail)
		for _, chunk := range []int{1, 3, 100} {
			out, st := feedEnd(true, false, chunk, in, nil, true)
			if len(out) != c.closed || st.FramesNotGELF != c.notGELFClose || st.LostFrames != 0 {
				t.Fatalf("%q closed: %q %+v", c.tail, out, st)
			}
			out, st = feedEnd(true, false, chunk, in, nil, false)
			if len(out) != c.cut || st.LostFrames != c.lostWhenCut {
				t.Fatalf("%q cut: %q %+v", c.tail, out, st)
			}
		}
		// Not strict: always a document.
		if out, _ := feedEnd(false, false, 4, in, nil, false); len(out) != 2 {
			t.Fatalf("%q not strict: %q", c.tail, out)
		}
	}
}
