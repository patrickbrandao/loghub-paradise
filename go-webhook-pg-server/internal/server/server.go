package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/clientip"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
)

// Config is the subset of the configuration the HTTP layer needs.
type Config struct {
	Bind              string
	Listeners         int
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxBodyBytes      int64
	MaxHeaderBytes    int64
	MaxURIBytes       int64
	MaxConns          int
	MaxConnsPerIP     int
	RejectDrainBytes  int64
	HTTP2             bool
	HTTP2MaxStreams   int

	// The envelope header every request of this instance carries (SPEC §3.1,
	// §3.5): from and space of the identity, the PID as sid, mode fast, and
	// the stage the 200 reports (§4.4: always ring here). Fixed at boot.
	From  string
	Space string
	SID   uint64
	Mode  string
	Stage string
}

// Server owns the public listener and the request pipeline.
type Server struct {
	cfg     Config
	ring    ring.Ring
	health  *sink.Health
	metrics *obs.Metrics
	log     *obs.Logger

	// Fixed at boot: there is no runtime reload (SPEC §13.3).
	resolver      *clientip.Resolver
	headerExclude map[string]bool

	// draining is set at shutdown phase 3, so requests arriving on already-open
	// connections get a retryable 503 rather than a dropped connection.
	draining atomic.Bool

	untrustedSampler *obs.Sampler

	srvs      []*http.Server
	listeners []net.Listener
	mu        sync.Mutex

	// conns maps a live net.Conn to its tag, so the protocol gauge can be
	// decremented from the bucket the connection was actually counted in.
	conns sync.Map

	// listenFn opens one public socket; listen in the binary. A test swaps it
	// to put a faulty listener under the accept retry (SPEC §10.3).
	listenFn func(ctx context.Context, addr string, reusePort bool) (net.Listener, error)
}

// New builds the server from the boot configuration.
func New(cfg Config, r ring.Ring, h *sink.Health,
	m *obs.Metrics, log *obs.Logger,
	trusted []netip.Prefix, maxXFF int, headerExclude map[string]bool) *Server {

	return &Server{
		cfg: cfg, ring: r, health: h, metrics: m, log: log,
		resolver:         clientip.New(trusted, maxXFF),
		headerExclude:    headerExclude,
		untrustedSampler: obs.NewSampler(30 * time.Second),
		listenFn:         listen,
	}
}

// Listen opens the public listener(s) without serving yet, so a bind failure is
// reported before anything else starts.
func (s *Server) Listen(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// One limiter for ALL listeners: the ceilings are per server, as the
	// registry describes them. A limiter per listener would multiply
	// max_conns and max_conns_per_ip by server.listeners (SPEC §13.5).
	var limiter *connLimiter
	if s.cfg.MaxConns > 0 || s.cfg.MaxConnsPerIP > 0 {
		limiter = newConnLimiter(s.cfg.MaxConns, s.cfg.MaxConnsPerIP,
			func() { s.metrics.Reject(obs.ReasonBadRequest) })
	}
	for i := 0; i < s.cfg.Listeners; i++ {
		ln, err := s.listenFn(ctx, s.cfg.Bind, s.cfg.Listeners > 1)
		if err != nil {
			for _, l := range s.listeners {
				_ = l.Close()
			}
			s.listeners = nil
			return err
		}
		// Always, and beneath the limiter: an accept error must never reach
		// net/http, which gives up on any that is not Temporary (SPEC §10.3).
		ln = newRetryListener(ln, s.log)
		if limiter != nil {
			ln = limiter.wrap(ln)
		}
		s.listeners = append(s.listeners, ln)
		s.srvs = append(s.srvs, s.newHTTPServer())
	}
	return nil
}

// Addr reports the address actually bound, which matters when the config asked
// for port 0.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.listeners) == 0 {
		return ""
	}
	return s.listeners[0].Addr().String()
}

func (s *Server) newHTTPServer() *http.Server {
	srv := &http.Server{
		Handler: s,
		// net/http answers "OPTIONS *" itself, 200 with no body, before any
		// handler runs; SPEC §4.2 refuses it with 400 like any target that is
		// not a path, which only the handler can do.
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            s.cfg.ReadHeaderTimeout,
		ReadTimeout:                  s.cfg.ReadTimeout,
		WriteTimeout:                 s.cfg.WriteTimeout,
		IdleTimeout:                  s.cfg.IdleTimeout,
		// The standard library gets a HIGHER ceiling than the configured one,
		// and that is the whole point (see checkHeaderSize). net/http answers
		// its own limit by writing 431 straight to the socket before any
		// handler runs, so anything it catches is invisible to the metrics.
		// Giving it slack moves the ordinary case -- a client with a few
		// cookies too many -- into the handler, where it becomes a counted
		// rejection like every other. net/http stays as the outer guard for the
		// absurd case, at a bounded cost: headerSlackBytes more buffer per
		// connection, nothing more.
		MaxHeaderBytes: int(s.cfg.MaxHeaderBytes + headerSlackBytes),
		ErrorLog:       nil,
		ConnState:      s.onConnState,
		ConnContext:    s.connContext,
	}

	// HTTP/1.0, HTTP/1.1 and h2c on the same port, from the standard library
	// alone (SPEC §4.1). golang.org/x/net/http2/h2c is not needed: net/http
	// detects the "PRI * HTTP/2.0" preface itself.
	//
	// Only prior-knowledge h2c is supported. The HTTP/1.1 "Upgrade: h2c"
	// handshake was removed by RFC 9113 and the standard library does not
	// implement it; such requests are served as HTTP/1.1.
	var p http.Protocols
	p.SetHTTP1(true)
	if s.cfg.HTTP2 {
		p.SetUnencryptedHTTP2(true)
		srv.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: s.cfg.HTTP2MaxStreams}
	}
	srv.Protocols = &p
	return srv
}

// connKey tags each connection with a small piece of state, so the protocol
// gauge can be corrected once the protocol is actually known.
type connKeyType struct{}

var connKey connKeyType

type connInfo struct {
	// isH2 flips at most once, when the first HTTP/2 request arrives on this
	// connection.
	isH2 atomic.Bool
}

// connContext tags a connection and registers it, so ConnState can find the tag
// again when the connection closes.
//
// The registry is necessary, not incidental: ConnState receives only the
// net.Conn and no request context, so without it a connection that was moved to
// the HTTP/2 bucket would be decremented from the HTTP/1 one on close, driving
// one gauge negative and the other up forever. One sync.Map operation per
// connection is nothing beside a TCP handshake.
func (s *Server) connContext(ctx context.Context, c net.Conn) context.Context {
	ci := &connInfo{}
	s.conns.Store(c, ci)
	return context.WithValue(ctx, connKey, ci)
}

// onConnState maintains whs_conns_active.
//
// A connection's protocol is unknown at accept time -- with h2c both HTTP/1.1
// and HTTP/2 arrive on the same port and are told apart only by the preface --
// so every connection is counted as HTTP/1 here and moved by noteProtocol on
// its first request.
func (s *Server) onConnState(c net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		s.metrics.ConnsActive[obs.ProtoHTTP1].Inc()
	case http.StateHijacked, http.StateClosed:
		// The connection was counted in exactly one bucket; decrement that one.
		v, ok := s.conns.LoadAndDelete(c)
		if ok {
			if ci, _ := v.(*connInfo); ci != nil && ci.isH2.Load() {
				s.metrics.ConnsActive[obs.ProtoHTTP2].Dec()
				return
			}
		}
		s.metrics.ConnsActive[obs.ProtoHTTP1].Dec()
	}
}

// noteProtocol moves a connection from the HTTP/1 bucket to the HTTP/2 one, at
// most once per connection.
//
// The move is done on the first request rather than at accept time because that
// is the first moment the protocol is known. Doing it once per connection --
// guarded by the per-connection flag -- is what keeps the two gauges summing to
// the real connection count instead of drifting by one per request.
func (s *Server) noteProtocol(r *http.Request) {
	if r.ProtoMajor != 2 {
		return
	}
	ci, _ := r.Context().Value(connKey).(*connInfo)
	if ci == nil || ci.isH2.Swap(true) {
		return
	}
	s.metrics.ConnsActive[obs.ProtoHTTP1].Dec()
	s.metrics.ConnsActive[obs.ProtoHTTP2].Inc()
}

// Serve starts accepting. It returns nil when every listener has stopped
// through Shutdown or Close, and returns the error AT ONCE when any one of
// them stops with an error (SPEC §10.3): waiting for all of them left a dead
// listener invisible, with server.listeners > 1, until every other one died
// too. The caller then shuts the whole server down, which stops the rest.
func (s *Server) Serve() error {
	s.mu.Lock()
	srvs, lns := s.srvs, s.listeners
	s.mu.Unlock()

	// Buffered for every listener, so the ones still running when Serve has
	// returned can finish later without blocking.
	results := make(chan error, len(srvs))
	for i := range srvs {
		go func(i int) {
			err := srvs[i].Serve(lns[i])
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			results <- err
		}(i)
	}
	for range srvs {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

// StartDraining begins shutdown phase 3: stop accepting, tell open connections
// to close, and answer anything still arriving with a retryable 503.
func (s *Server) StartDraining() { s.draining.Store(true) }

// Shutdown stops the listeners and waits for in-flight requests, up to the
// context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.StartDraining()

	s.mu.Lock()
	srvs := s.srvs
	s.mu.Unlock()

	var wg sync.WaitGroup
	errs := make([]error, len(srvs))
	for i := range srvs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// SetKeepAlivesEnabled(false) makes the remaining responses carry
			// Connection: close, so idle keep-alives drain instead of being
			// severed mid-request.
			srvs[i].SetKeepAlivesEnabled(false)
			errs[i] = srvs[i].Shutdown(ctx)
		}(i)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Close severs everything immediately, for the second-signal abort path and
// for a boot interrupted right after Listen. The listeners are closed too: an
// http.Server only knows the ones Serve was handed, and a boot interrupted
// before Serve left them open (SPEC §10.5).
func (s *Server) Close() error {
	s.mu.Lock()
	srvs, lns := s.srvs, s.listeners
	s.mu.Unlock()

	var errs []error
	for _, srv := range srvs {
		errs = append(errs, srv.Close())
	}
	for _, ln := range lns {
		_ = ln.Close() // a second Close of a served one only says it is closed
	}
	return errors.Join(errs...)
}
