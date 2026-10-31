// Package receiver holds the three transports of SPEC §4 -- UDP, TCP and
// HTTP -- the PROXY protocol reader, the GELF chunk reassembler, the
// decompressor, the NUL framer and the shared receive pipeline (Ingest) they
// all funnel into. There is no TLS (SPEC §1.2).
//
// The receive path does the minimum that lets it decide whether a document
// gets in, in this order (SPEC §2.1, §2.2):
//
//	[PROXY] -> chunks / framing / body -> decompression -> size ceiling
//	-> ONE clock sample -> sink UP? -> uuid -> Item(head + raw) -> ring
//
// The document itself is not read here. The full parse and the JLH envelope
// happen in the drain worker, so nothing in this package knows what an
// envelope looks like.
//
// What each transport does with a refusal is its own business, and the whole
// asymmetry of SPEC §2.3:
//
//	tcp   stop reading until the ring drains (SPEC §5.7); nothing is lost
//	http  503 with Retry-After; the client sends again
//	udp   nothing at all; the drop is counted and that is the only record
//
// Wiring, for the app:
//
//	in := receiver.NewIngest(ring, sinkState, metrics, log, receiver.IngestConfig{...})
//	udp, err := receiver.NewUDP(ctx, receiver.UDPConfig{...}, in, metrics, log)
//	tcp, err := receiver.NewTCP(ctx, receiver.TCPConfig{...}, in, metrics, log)
//	web, err := receiver.NewHTTP(ctx, receiver.HTTPConfig{...}, in, metrics, log)
//	... X.Start(recvCtx) on each; at shutdown cancel recvCtx, then X.Close()
//	in reverse boot order (SPEC §13.4).
package receiver

import (
	"net"
	"net/netip"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
)

// obsTransport maps the item's transport to the metric label. The item
// package is go-gelf-server's, copied as is, and still names tls and https;
// obs has only the three transports of this server, so a series that can
// never rise is not exposed (SPEC §14.1). tls and https are never built here
// (SPEC §1.2); they map to their plaintext twins only so the function is
// total. TestTransportEnumsAgree pins the mapping.
func obsTransport(t item.Transport) obs.Transport {
	switch t {
	case item.TransportUDP:
		return obs.TransportUDP
	case item.TransportHTTP, item.TransportHTTPS:
		return obs.TransportHTTP
	}
	return obs.TransportTCP
}

// addrPortOf extracts a canonical address and port from a net.Addr of a TCP or
// UDP socket (SPEC §4.2: IPv4-mapped becomes IPv4, no zone, and an unspecified
// address is the zero Addr -- unknown, never recorded).
func addrPortOf(a net.Addr) (netip.Addr, uint16) {
	var ap netip.AddrPort
	switch v := a.(type) {
	case *net.TCPAddr:
		ap = v.AddrPort()
	case *net.UDPAddr:
		ap = v.AddrPort()
	default:
		return netip.Addr{}, 0
	}
	return item.Canonical(ap.Addr()), ap.Port()
}
