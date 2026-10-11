package receiver

import (
	"context"
	"fmt"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// TCPConfig is [listen] tcp + tcp_listeners and the [tcp] section (SPEC §4.4,
// §16.5), plus the shared [proxy] section in StreamConfig.Proxy.
type TCPConfig struct {
	StreamConfig
	// Framing is tcp.framing: auto (default), octet or lf (SPEC §5.2).
	Framing FramingMode
}

// NewTCP opens the TCP listeners (RFC 6587, SPEC §4.4). The message size limit
// is the Ingest's (syslog.max_message_bytes).
func NewTCP(ctx context.Context, cfg TCPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	if cfg.Framing > FramingLF {
		return nil, fmt.Errorf("receiver tcp: unknown framing %d", cfg.Framing)
	}
	return newStream(ctx, item.TransportTCP, cfg.StreamConfig, cfg.Framing, nil, 0, in, m, log)
}
