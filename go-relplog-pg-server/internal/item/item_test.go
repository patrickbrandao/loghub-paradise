package item

import (
	"net/netip"
	"testing"
	"time"
)

// TestTransportNames pins the closed transport set: the payload's names, and
// no tls (SPEC §1.2).
func TestTransportNames(t *testing.T) {
	want := []string{"udp", "tcp", "unix"}
	if NumTransports != len(want) {
		t.Fatalf("NumTransports = %d, want %d", NumTransports, len(want))
	}
	for i, n := range want {
		tr := Transport(i)
		if tr.String() != n {
			t.Errorf("Transport(%d) = %q, want %q", i, tr, n)
		}
		if back, ok := ParseTransport(n); !ok || back != tr {
			t.Errorf("ParseTransport(%q) = %v, %v", n, back, ok)
		}
	}
	if _, ok := ParseTransport("tls"); ok {
		t.Error("tls is not a transport of this server")
	}
	if !TransportUDP.Network() || !TransportTCP.Network() || TransportUnix.Network() {
		t.Error("Network(): udp and tcp have addresses, unix does not")
	}
}

func TestHeadTime(t *testing.T) {
	at := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
	var h Head
	h.SetTime(at)
	if !h.Time().Equal(at) || h.Sub != 123456789 {
		t.Fatalf("Time() = %v (sub %d), want %v", h.Time(), h.Sub, at)
	}
}

func TestCanonical(t *testing.T) {
	cases := map[string]string{
		"::ffff:10.0.0.3":      "10.0.0.3",
		"2001:DB8:0:0:0:0:0:1": "2001:db8::1",
		"fe80::1%eth0":         "fe80::1",
		"0.0.0.0":              "invalid IP",
		"::":                   "invalid IP",
	}
	for in, want := range cases {
		if got := Canonical(netip.MustParseAddr(in)).String(); got != want {
			t.Errorf("%s: %s want %s", in, got, want)
		}
	}
}

func TestReleaseOnceAndTaint(t *testing.T) {
	buf, class := CopyRaw([]byte("hello"))
	it := New(Head{}, buf, class)
	it.Release()
	it.Release()
	if !it.Released() || it.Raw != nil {
		t.Fatal("release")
	}
	buf, class = CopyRaw([]byte("x"))
	it = New(Head{}, buf, class)
	it.Taint()
	it.Release()
	if it.Raw == nil {
		t.Fatal("a tainted buffer was handed back to the pool")
	}
	it = New(Head{}, []byte("y"), -1)
	it.Discard()
	if !it.Released() || !it.Tainted() || it.Raw == nil {
		t.Fatal("discard")
	}
}

// Review C, win 1: a buffer taken with CopyRawBox goes back in the box it came
// out of, so an accept-and-release cycle allocates the Item and nothing else.
// PutRaw boxes the slice header again on every release, a second allocation.
func TestBoxedReleaseDoesNotAllocate(t *testing.T) {
	line := make([]byte, 200)
	allocs := testing.AllocsPerRun(1000, func() {
		buf, class, box := CopyRawBox(line)
		it := NewBoxed(Head{}, buf, class, box)
		it.Release()
	})
	if allocs > 1 {
		t.Fatalf("%.1f allocations per message, want at most 1 (the Item)", allocs)
	}
	// Control: the unboxed path does pay the second allocation.
	allocs = testing.AllocsPerRun(1000, func() {
		buf, class := CopyRaw(line)
		New(Head{}, buf, class).Release()
	})
	if allocs < 2 {
		t.Logf("unboxed path allocated %.1f times; the control no longer shows the difference", allocs)
	}
}
