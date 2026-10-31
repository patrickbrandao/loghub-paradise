package receiver

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
)

// TCPConfig is [listen] tcp + tcp_listeners and the [tcp] section (SPEC §4.4,
// §16.5), plus the shared [proxy] section in StreamConfig.Proxy.
type TCPConfig struct {
	StreamConfig
}

// NewTCP opens the GELF TCP listeners (SPEC §4.4). The document size limit is
// the Ingest's (convert.max_message_bytes).
func NewTCP(ctx context.Context, cfg TCPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	return newStream(ctx, item.TransportTCP, cfg.StreamConfig, in, m, log)
}
