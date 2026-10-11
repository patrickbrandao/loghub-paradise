package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/sink"
)

// Admin serves /healthz, /readyz and /metrics on a separate listener (SPEC §14).
//
// It opens FIRST during boot and stays open through the whole startup, so that
// even in a crash loop caused by an unreachable sink the operator still has
// /readyz with the exact reason and /metrics with the backlog count. Without
// that, all that remains is the container log.
//
// It lives in app rather than in its own package because, unlike the sibling
// service, there is no public HTTP server here for it to sit beside. Syslog does
// not have paths to reserve; the separate port exists only so a scrape never
// competes with ingestion.
type Admin struct {
	ring      atomic.Pointer[ring.Ring]
	health    atomic.Pointer[sink.Health]
	metrics   *obs.Metrics
	highWater float64

	// bootState describes startup progress; /readyz reports it verbatim.
	bootState atomic.Pointer[string]
	draining  atomic.Bool

	srv *http.Server
	ln  net.Listener
}

// NewAdmin builds the admin surface. The ring and the health machine are
// attached later, with Attach: the port has to be listening BEFORE either exists,
// because building the sink is exactly the step that can crash-loop.
func NewAdmin(m *obs.Metrics, highWater float64) *Admin {
	a := &Admin{metrics: m, highWater: highWater}
	a.SetBootState("starting")
	return a
}

// Attach wires the components /readyz and /metrics report on.
func (a *Admin) Attach(r ring.Ring, h *sink.Health) {
	a.ring.Store(&r)
	a.health.Store(h)
}

// SetBootState updates the reason /readyz reports while the server is not yet
// ready. An empty string means boot is complete.
func (a *Admin) SetBootState(s string) { a.bootState.Store(&s) }

// StartDraining makes /readyz fail at shutdown phase 1, before anything stops
// accepting, so the load balancer removes this endpoint first.
func (a *Admin) StartDraining() { a.draining.Store(true) }

// Listen binds the admin address.
func (a *Admin) Listen(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)
	mux.HandleFunc("/metrics", a.handleMetrics)

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	a.ln = ln
	a.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return nil
}

// Addr reports the bound admin address.
func (a *Admin) Addr() string {
	if a.ln == nil {
		return ""
	}
	return a.ln.Addr().String()
}

// Serve blocks until the admin server stops.
func (a *Admin) Serve() error {
	if a.srv == nil {
		return nil
	}
	err := a.srv.Serve(a.ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (a *Admin) Shutdown(ctx context.Context) error {
	if a.srv == nil {
		return nil
	}
	return a.srv.Shutdown(ctx)
}

// handleHealthz is liveness: 200 whenever the process is running and able to
// respond. It deliberately says nothing about the sink -- a restart would not fix
// an unreachable NATS, so liveness must not fail on it.
func (a *Admin) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz is readiness. It fails, with the reason in the body, when the
// server should not receive traffic.
func (a *Admin) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	var reasons []string
	if a.draining.Load() {
		reasons = append(reasons, "shutting down")
	}
	if s := a.bootState.Load(); s != nil && *s != "" {
		reasons = append(reasons, *s)
	}
	if h := a.health.Load(); h != nil {
		switch h.State() {
		case sink.StateDown:
			reasons = append(reasons, "sink is DOWN")
		case sink.StateFatal:
			reasons = append(reasons, "sink is permanently unusable")
		}
	}
	if rp := a.ring.Load(); rp != nil {
		st := (*rp).Stats()
		if st.Saturated(a.highWater) {
			reasons = append(reasons, fmt.Sprintf(
				"ring above high water: %d/%d items, %d/%d bytes",
				st.Items, st.CapItems, st.Bytes, st.CapBytes))
		}
	}

	if len(reasons) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(strings.Join(reasons, "; ") + "\n"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func (a *Admin) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	// Refresh the gauges owned by other components, so a scrape reflects the
	// current instant rather than the last time something changed.
	if rp := a.ring.Load(); rp != nil {
		st := (*rp).Stats()
		a.metrics.RingItems.Set(st.Items)
		a.metrics.RingBytes.Set(st.Bytes)
		a.metrics.RingCapacityItems.Set(st.CapItems)
		a.metrics.RingCapacityBytes.Set(st.CapBytes)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = a.metrics.WritePrometheus(w)
}
