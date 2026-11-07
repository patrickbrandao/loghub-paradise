package pcap_test

import (
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-injector/internal/pcap/pcaptest"
)

// TestTCPEndClosed: a direction is closed only when the capture proves its
// end -- a FIN with every byte before it, or a RST right after the last
// byte; the end of the capture, or a RST ahead of a hole, is not (SPEC §6.4;
// from go-gelf-injector, 2026-11-07).
func TestTCPEndClosed(t *testing.T) {
	c := pcaptest.Conn{Link: 1, Src: cli4, Dst: srv4, ISN: 10, T0: t0}
	segs := c.Split([]byte(strings.Repeat("x", 300)), 100)
	k := pcap.FlowKey{Src: cli4, Dst: srv4}

	got, _ := assemble(t, c.Frames(segs, false, false))
	if !got[k].closed || got[k].ended != 1 {
		t.Fatalf("FIN: closed %v", got[k].closed)
	}
	got, _ = assemble(t, c.Frames(segs, false, true))
	if got[k].closed {
		t.Fatal("end of capture taken as closed")
	}
	rst := func(seq uint32) pcaptest.Frame {
		ip := pcaptest.IP(cli4.Addr(), srv4.Addr(), 6, pcaptest.TCP(cli4.Port(), srv4.Port(), seq, pcaptest.RST, nil), 1)
		return pcaptest.Frame{TS: t0.Add(time.Second), Data: pcaptest.Link(1, ip)}
	}
	frames := append(c.Frames(segs, false, true), rst(10+1+300))
	if got, _ = assemble(t, frames); !got[k].closed {
		t.Fatal("RST after the last byte: not closed")
	}
	// The last segment missing from the capture: the RST is ahead.
	frames = append(c.Frames(segs[:2], false, true), rst(10+1+300))
	if got, _ = assemble(t, frames); got[k].closed {
		t.Fatal("RST ahead of a hole taken as closed")
	}
}
