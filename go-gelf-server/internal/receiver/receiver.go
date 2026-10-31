// Package receiver holds the five transports of SPEC §4 -- UDP, TCP, TCP+TLS,
// HTTP and HTTPS -- the PROXY protocol reader, the GELF chunk reassembler, the
// decompressor, the NUL framer and the shared receive pipeline (Ingest) they
// all funnel into.
//
// The receive path does the minimum that lets it decide whether a document
// gets in, in this order (SPEC §2.1, §2.2):
//
//	[PROXY] -> [TLS] -> chunks / framing / body -> decompression
//	-> size ceiling -> ONE clock sample -> level (only with an active filter)
//	-> filter -> rate-limit -> sink UP? -> uuid -> Item(head + raw) -> ring
//
// The level is read with convert.Converter.LevelOf, the converter's own
// function (SPEC §7). The full parse and the JLH envelope happen in the drain
// worker, so nothing in this package knows what an envelope looks like.
//
// What each transport does with a refusal is its own business, and the whole
// asymmetry of SPEC §2.3:
//
//	tcp, tls     stop reading until the ring drains (SPEC §5.7); nothing is lost
//	http, https  503 with Retry-After; the client sends again
//	udp          nothing at all; the drop is counted and that is the only record
//
// Wiring, for the app:
//
//	in := receiver.NewIngest(ring, sinkState, filter, limiter, metrics, log, receiver.IngestConfig{...})
//	udp, err := receiver.NewUDP(ctx, receiver.UDPConfig{...}, in, metrics, log)
//	tcp, err := receiver.NewTCP(ctx, receiver.TCPConfig{...}, in, metrics, log)
//	tls, err := receiver.NewTLS(ctx, receiver.TLSConfig{...}, in, metrics, log)
//	web, err := receiver.NewHTTP(ctx, receiver.HTTPConfig{...}, in, metrics, log)
//	sec, err := receiver.NewHTTPS(ctx, receiver.HTTPConfig{...}, tlsCfg, in, metrics, log)
//	... X.Start(recvCtx) on each; at shutdown cancel recvCtx, then X.Close()
//	on every transport at once, in parallel (SPEC §13.4).
package receiver

import (
	"net"
	"net/netip"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
)

// obsTransport maps the item's transport to the metric label. obs mirrors
// item.Transport on purpose (it imports nothing of this module), and the two
// enumerations have the same order; TestTransportEnumsAgree pins that.
func obsTransport(t item.Transport) obs.Transport { return obs.Transport(t) }

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
