package server

import (
	"net/http"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/clientip"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server/internal/obs"
)

// Per-request logging, per SPEC §11.3.
//
// Nothing is written per request on the default path. Access logging is opt-in
// and sampled, because a synchronous write per request destroys any benchmark
// and, under attack, fills the disk faster than the ring fills memory.
//
// The sampling decision is taken BEFORE any attribute is built, so an unsampled
// request costs one atomic load and a random number rather than the allocation
// of a log record that is then thrown away.

func (s *Server) accessLog(r *http.Request, ip clientip.Result, code int, bodyLen int64, start time.Time) {
	if !s.log.AccessEnabled() {
		return
	}
	s.log.Info("request",
		"method", r.Method,
		"uri", r.RequestURI,
		"proto", r.Proto,
		"host", r.Host,
		"code", code,
		"body_bytes", bodyLen,
		"src_ip", ip.Addr.String(),
		"src_ip_source", ip.Source.String(),
		"duration_ms", float64(time.Since(start).Microseconds())/1000,
	)
}

// rejectLog records a refusal. Rejections are sampled through the same switch
// as the access log: under an attack producing hundreds of thousands of 429s a
// second, logging each one would be the actual denial of service.
func (s *Server) rejectLog(r *http.Request, code int, reason obs.Reason, start time.Time) {
	if !s.log.AccessEnabled() {
		return
	}
	s.log.Warn("rejected",
		"method", r.Method,
		"uri", r.RequestURI,
		"code", code,
		"reason", reason.String(),
		"duration_ms", float64(time.Since(start).Microseconds())/1000,
	)
}
