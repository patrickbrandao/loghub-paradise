package sink

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// SPEC §8.5.5, end to end against the real servers: the boot validation of the
// SQL destinations runs under sink.op_timeout, not under the deadline of the
// health check that triggers it, and a cancelled boot still reaches it. The
// unit tests of validationContext prove the helper; these prove each sink
// uses it. A proxy in front of the server holds every chunk for a while, so
// the dozen round trips of the validation take several times what the check
// alone does -- the 60 ms of RTT that made the boot fail in the first place.

// slowProxy forwards TCP to a real server, holding each chunk for delay
// before passing it on.
type slowProxy struct {
	ln       net.Listener
	upstream string
	delay    atomic.Int64

	mu    sync.Mutex
	conns []net.Conn
}

func newSlowProxy(t *testing.T, upstream string) *slowProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &slowProxy{ln: ln, upstream: upstream}
	go p.serve()
	t.Cleanup(func() {
		ln.Close()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.conns {
			c.Close()
		}
	})
	return p
}

func (p *slowProxy) Addr() string { return p.ln.Addr().String() }

func (p *slowProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		u, err := net.Dial("tcp", p.upstream)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, c, u)
		p.mu.Unlock()
		go p.pipe(c, u)
		go p.pipe(u, c)
	}
}

func (p *slowProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			time.Sleep(time.Duration(p.delay.Load()))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// checkValidationDeadline drives one sink through the two halves of SPEC
// §8.5.5. fresh builds a sink behind the proxy, with a validation deadline
// far above anything here; warm opens its connection with a trivial round
// trip, so the slow part measured is the validation and not the dial.
func checkValidationDeadline(t *testing.T, proxy *slowProxy,
	fresh func() (health func(context.Context) error, warm func(context.Context) error)) {
	t.Helper()
	const delay = 30 * time.Millisecond

	// The check's deadline: a few times one round trip through the proxy,
	// enough for the check's own query, far short of the validation.
	health, warm := fresh()
	proxy.delay.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := warm(ctx); err != nil {
		cancel()
		t.Fatalf("warm: %v", err)
	}
	cancel()
	proxy.delay.Store(int64(delay))
	start := time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	if err := warm(ctx); err != nil {
		cancel()
		t.Fatalf("warm through the delay: %v", err)
	}
	cancel()
	checkDeadline := 4*time.Since(start) + 100*time.Millisecond

	t.Run("the validation outlives the check deadline", func(t *testing.T) {
		boot, stopBoot := context.WithCancel(context.Background())
		defer stopBoot()
		check, cancel := context.WithTimeout(withLifetime(boot), checkDeadline)
		defer cancel()
		start := time.Now()
		err := health(check)
		took := time.Since(start)
		if err != nil {
			t.Fatalf("Health under a %s check deadline: %v (after %s) -- the validation ran on the check's deadline, not sink.op_timeout",
				checkDeadline, err, took)
		}
		if took <= checkDeadline {
			t.Fatalf("the validation took %s, within the %s check deadline: the proxy delay proves nothing", took, checkDeadline)
		}
	})

	t.Run("a boot cancelled after the check deadline reaches the validation", func(t *testing.T) {
		health, warm := fresh()
		proxy.delay.Store(0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := warm(ctx); err != nil {
			t.Fatalf("warm: %v", err)
		}
		proxy.delay.Store(int64(delay))

		boot, cancelBoot := context.WithCancel(context.Background())
		defer cancelBoot()
		check, cancelCheck := context.WithTimeout(withLifetime(boot), checkDeadline)
		defer cancelCheck()
		time.AfterFunc(checkDeadline+2*delay, cancelBoot)
		if err := health(check); err == nil {
			t.Fatal("Health validated the schema although the boot was cancelled during the validation")
		}
	})
}

func TestPostgresValidationHasItsOwnDeadline(t *testing.T) {
	f := pgSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addr)
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addr = proxy.Addr()
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		cfg.ValidateTimeout = 30 * time.Second
		p := f.sink(cfg)
		// warm also opens, on its first call, the health check's own
		// connection (SPEC §8.5.8): dialled through the delay, its handshake
		// alone took most of the check deadline. Later calls are the one
		// round trip the deadline is measured by.
		probed := false
		return p.Health, func(ctx context.Context) error {
			if err := p.pool.Ping(ctx); err != nil || probed {
				return err
			}
			probed = true
			var one int
			return p.probeScan(ctx, "SELECT 1", &one)
		}
	})
}

func TestMySQLValidationHasItsOwnDeadline(t *testing.T) {
	f := mysqlSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addr)
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addr = proxy.Addr()
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		p := f.sink(cfg)
		probed := false // see TestPostgresValidationHasItsOwnDeadline
		return p.Health, func(ctx context.Context) error {
			if err := p.db.PingContext(ctx); err != nil || probed {
				return err
			}
			probed = true
			return p.probe.PingContext(ctx) // the health check's own (SPEC §8.6.7)
		}
	})
}

func TestClickHouseValidationHasItsOwnDeadline(t *testing.T) {
	f := chSetup(t)
	proxy := newSlowProxy(t, f.cfg.Addrs[0])
	checkValidationDeadline(t, proxy, func() (func(context.Context) error, func(context.Context) error) {
		cfg := f.cfg
		cfg.Addrs = []string{proxy.Addr()}
		cfg.PoolSize = 1
		cfg.ConnectTimeout = 30 * time.Second
		c := f.sink(cfg)
		probed := false // see TestPostgresValidationHasItsOwnDeadline
		return c.Health, func(ctx context.Context) error {
			if err := c.conn.Ping(ctx); err != nil || probed {
				return err
			}
			probed = true
			return c.probe.Ping(ctx) // the health check's own (SPEC §8.8.8)
		}
	})
}

// lifetimeSink records the lifetime each health check carries.
type lifetimeSink struct {
	None
	mu   sync.Mutex
	seen []context.Context
}

func (s *lifetimeSink) Health(ctx context.Context) error {
	life, _ := ctx.Value(lifetimeKey{}).(context.Context)
	s.mu.Lock()
	s.seen = append(s.seen, life)
	s.mu.Unlock()
	return nil
}

// Both callers of Sink.Health -- the boot's WaitReady and the periodic check
// -- hand the sink the context they were given as its lifetime, or a
// validation running past the check's deadline could not see the boot being
// cancelled (validationContext).
func TestHealthChecksCarryTheirLifetime(t *testing.T) {
	s := &lifetimeSink{}
	h := NewHealth(s, HealthConfig{Interval: time.Hour, Timeout: time.Second}, quietLog(), testMetrics())
	boot := context.WithValue(context.Background(), lifetimeKey{}, "not this one")
	boot, cancel := context.WithCancel(boot)
	defer cancel()

	if err := h.WaitReady(boot, time.Second, time.Millisecond, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	h.check(boot)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seen) != 2 {
		t.Fatalf("%d health checks, want 2", len(s.seen))
	}
	for i, life := range s.seen {
		if life != boot {
			t.Errorf("check %d carried lifetime %v, want the caller's context", i, life)
		}
	}
}
