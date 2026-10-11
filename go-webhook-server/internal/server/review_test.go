package server

// Tests written after the independent review of 2026-09-14: the wiring of the
// connection limiter (SPEC §13.5), and the admin surface before the ring and
// the health are attached (SPEC §10.1, §11).

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-server/internal/sink"
)

// The wiring, not the mechanism: every listener Listen opens is wrapped by the
// SAME limiter. A limiter created inside the listener loop would pass the
// unit tests on connLimiter and still multiply the ceilings by
// server.listeners (SPEC §13.5).
func TestListenersShareOneLimiter(t *testing.T) {
	m := obs.NewMetrics("test", "none", "go", "stub", "stub")
	log := obs.NewLogger(io.Discard, "text", "error")
	h := sink.NewHealth(healthySink{}, sink.HealthConfig{}, log.Logger, m)
	lim := ratelimit.New(ratelimit.Params{}, 0)
	s := New(Config{
		Bind: "127.0.0.1:0", Listeners: 2, MaxConns: 1,
		MaxBodyBytes: 1 << 20, MaxURIBytes: 8 << 10, MaxHeaderBytes: 64 << 10,
	}, &stubRing{}, h, lim, m, log, nil, 32, nil)
	if err := s.Listen(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if len(s.listeners) != 2 {
		t.Fatalf("%d listeners, want 2", len(s.listeners))
	}
	var shared *connLimiter
	for i, l := range s.listeners {
		ll, ok := l.(*limitListener)
		if !ok {
			t.Fatalf("listener %d is not under the limiter", i)
		}
		if shared == nil {
			shared = ll.lim
		} else if ll.lim != shared {
			t.Fatal("the listeners hold different limiters; max_conns would be per listener again")
		}
	}
}

// SPEC §10.1 step 2: the admin port opens before the sink exists, so /metrics
// and /readyz must answer -- with the boot state, not a nil dereference --
// before Attach hands them the ring and the health.
func TestAdminAnswersBeforeAttach(t *testing.T) {
	a := NewAdmin(obs.NewMetrics("test", "none", "go", "memory", "none"), 0.8)

	w := httptest.NewRecorder()
	a.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "whs_config_info") {
		t.Fatalf("/metrics before Attach: %d %q", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	a.handleReadyz(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before Attach = %d, want 503 with the boot state", w.Code)
	}
}
