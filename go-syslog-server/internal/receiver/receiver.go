// Package receiver holds the four transports of SPEC §4 -- UDP, unix datagram,
// TCP and TLS -- the PROXY protocol reader, the RFC 6587 framer and the shared
// receive pipeline (Ingest) they all funnel into.
//
// The receive path does the minimum that lets it decide whether a message
// gets in, in this order (SPEC §2.1, §2.2):
//
//	[PROXY] -> [TLS] -> framing -> truncation -> ONE clock sample
//	-> PRI -> filter -> rate-limit -> uuid -> Item(head + raw) -> ring
//
// Only the PRI is read here, with convert.ScanPRI through convert.EffectivePRI
// (SPEC §6.2). The full parse and the JLH envelope happen in the drain worker,
// so nothing in this package knows what an envelope looks like.
//
// What each transport does with a refusal is its own business, and the whole
// asymmetry of SPEC §2.3:
//
//	tcp, tls  stop reading until the ring drains (SPEC §5.7); nothing is lost
//	unix      nothing; the kernel already blocked the sender's send()
//	udp       nothing at all; the drop is counted and that is the only record
//
// Wiring, for the app:
//
//	in := receiver.NewIngest(ring, sinkState, filter, limiter, metrics, log, receiver.IngestConfig{...})
//	udp, err := receiver.NewUDP(ctx, receiver.UDPConfig{...}, in, metrics, log)
//	uni, err := receiver.NewUnix(ctx, receiver.UnixConfig{...}, in, metrics, log)
//	tcp, err := receiver.NewTCP(ctx, receiver.TCPConfig{...}, in, metrics, log)
//	tls, err := receiver.NewTLS(ctx, receiver.TLSConfig{...}, in, metrics, log)
//	... X.Start(recvCtx) on each; at shutdown cancel recvCtx, then X.Close()
//	on every transport at once, in parallel (SPEC §13.4).
package receiver

import (
	"net"
	"net/netip"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
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
