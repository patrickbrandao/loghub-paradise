package receiver

import (
	"context"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
)

// TCPConfig is [listen] tcp + tcp_listeners and the [tcp] section (SPEC §4.4,
// §16.5), plus the shared [proxy] and [relp] sections in StreamConfig.
type TCPConfig struct {
	StreamConfig
}

// NewTCP opens the listeners of RELP in clear (SPEC §4.4). The message size
// limit is the Ingest's (syslog.max_message_bytes); the frame fuse is
// relp.max_frame_bytes.
func NewTCP(ctx context.Context, cfg TCPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Stream, error) {
	return newStream(ctx, item.TransportTCP, cfg.StreamConfig, in, m, log)
}
