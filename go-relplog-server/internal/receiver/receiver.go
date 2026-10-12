// Package receiver holds the two transports of SPEC §4 -- RELP over TCP and
// RELP over TLS -- the PROXY protocol reader, the RELP framer and session
// (SPEC §5) and the shared receive pipeline (Ingest) they both funnel into.
//
// The receive path does the minimum that lets it decide whether a message
// gets in, in this order (SPEC §2.1, §2.2):
//
//	[PROXY] -> [TLS] -> RELP frame -> truncation -> ONE clock sample
//	-> PRI -> filter -> rate-limit -> uuid -> Item(head + raw) -> ring
//
// Only the PRI is read here, with convert.ScanPRI through convert.EffectivePRI
// (SPEC §6.2). The full parse and the JLH envelope happen in the drain worker,
// so nothing in this package knows what an envelope looks like.
//
// A transaction the ring cannot take yet is HELD, never refused: no answer
// while the condition lasts, the window fills, the reader stops reading and
// the emitter keeps the messages in its own queue (SPEC §2.3, §5.7). Past
// relp.backpressure_timeout the session closes without an answer and librelp
// retransmits; nothing is lost.
//
// Wiring, for the app:
//
//	in := receiver.NewIngest(ring, sinkState, filter, limiter, metrics, log, receiver.IngestConfig{...})
//	tcp, err := receiver.NewTCP(ctx, receiver.TCPConfig{...}, in, metrics, log)
//	tls, err := receiver.NewTLS(ctx, receiver.TLSConfig{...}, in, metrics, log)
//	... X.Start(recvCtx) on each; at shutdown cancel recvCtx, then X.Close()
//	on all of them at once (SPEC §5.8, §13.4).
package receiver

import (
	"net"
	"net/netip"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
)

// obsTransport maps the item's transport to the metric label. obs mirrors
// item.Transport on purpose (it imports nothing of this module), and the two
// enumerations have the same order; TestTransportEnumsAgree pins that.
func obsTransport(t item.Transport) obs.Transport { return obs.Transport(t) }

// addrPortOf extracts a canonical address and port from a net.Addr of a TCP
// socket (SPEC §4.2: IPv4-mapped becomes IPv4, no zone, and an unspecified
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
