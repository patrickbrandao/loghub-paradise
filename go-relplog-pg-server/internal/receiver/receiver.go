// Package receiver holds the transport of SPEC §4 -- RELP over TCP -- the
// PROXY protocol reader, the RELP framer and session (SPEC §5) and the receive
// pipeline (Ingest) it funnels into.
//
// The receive path does the minimum that lets it decide whether a message
// gets in, in this order (SPEC §2.1, §2.2):
//
//	[PROXY] -> RELP frame -> truncation -> ONE clock sample
//	-> sink DOWN? -> ring full? -> uuid -> Item(head + raw) -> ring
//
// Nothing here reads the message itself: there is no filter and no rate
// limiter in this server (SPEC §1.2), so even the PRI is the drain worker's
// business, like the rest of the parse and the JLH envelope.
//
// A transaction the ring cannot take yet is HELD, never refused: no answer
// while the condition lasts, the window fills, the reader stops reading and
// the emitter keeps the messages in its own queue (SPEC §2.3, §5.7). Past
// relp.backpressure_timeout the session closes without an answer and librelp
// retransmits; nothing is lost.
//
// Wiring, for the app:
//
//	in := receiver.NewIngest(ring, sinkState, metrics, log, receiver.IngestConfig{...})
//	tcp, err := receiver.NewTCP(ctx, receiver.TCPConfig{...}, in, metrics, log)
//	... tcp.Start(recvCtx); at shutdown cancel recvCtx, then tcp.Close()
//	(SPEC §5.8, §13.4).
package receiver

import (
	"net"
	"net/netip"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
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
