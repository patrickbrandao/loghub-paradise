package sink

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Ported from go-relplog-server (review_sql_integration_test.go there): the
// PostgreSQL items of S §22.1 against the real server (RELPLOG_TEST_PG_DSN);
// each test skips without it.

// ---------------------------------------------------------------- item 6

// slowProxy forwards TCP to a real server, holding each chunk for delay
// before passing it on: the dozen round trips of a validation then take
// several times what the health check's own query does -- the ~55 ms of RTT
// that made the boot fail in the first place.
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

// checkValidationDeadline drives one sink through both halves of item 6.
// fresh builds a sink behind the proxy with a validation deadline far above
// anything here; warm opens its connection with a trivial round trip, so what
// the delay slows down is the validation, not the dial.
func checkValidationDeadline(t *testing.T, proxy *slowProxy,
	fresh func() (health func(context.Context) error, warm func(context.Context) error)) {
	t.Helper()
	const delay = 30 * time.Millisecond

	// The check's deadline: a few round trips through the proxy, enough for
	// the check's own query, far short of the validation's dozen.
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
	rtt := time.Since(start)
	checkDeadline := 4*rtt + 100*time.Millisecond

	t.Run("the validation outlives the check deadline", func(t *testing.T) {
		check, cancel := context.WithTimeout(context.Background(), checkDeadline)
		defer cancel()
		start := time.Now()
		err := health(check)
		took := time.Since(start)
		if err != nil {
			t.Fatalf("Health under a %s check deadline: %v (after %s) -- the validation ran on the "+
				"check's deadline, not sink.op_timeout", checkDeadline, err, took)
		}
		if took <= checkDeadline {
			t.Fatalf("the validation took %s, within the %s check deadline: the delay proves nothing", took, checkDeadline)
		}
	})

	t.Run("a boot cancelled during the validation reaches it", func(t *testing.T) {
		health, warm := fresh()
		proxy.delay.Store(0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := warm(ctx); err != nil {
			t.Fatalf("warm: %v", err)
		}
		proxy.delay.Store(int64(delay))

		// The boot is cancelled three round trips in: past the check's own
		// query, inside the validation (a dozen round trips). The check's
		// window is still open, which is what the check context alone can
		// forward (validationContext without a lifetime, as Health.WaitReady
		// calls it today); the validation must stop there instead of running
		// on for its 30 s.
		boot, cancelBoot := context.WithCancel(context.Background())
		defer cancelBoot()
		check, cancelCheck := context.WithTimeout(boot, time.Hour)
		defer cancelCheck()
		time.AfterFunc(3*rtt, cancelBoot)
		start := time.Now()
		if err := health(check); err == nil {
			t.Fatal("Health validated the table although the boot was cancelled during the validation")
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Errorf("the cancelled validation ran for %s", took)
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
		return p.Health, p.pool.Ping
	})
}

// ------------------------------------------------------------ items 7, 8

// A 23502 after the boot is the table -- here a required column added after
// the validation -- and fatal, not a logic drop of every chunk.
func TestPostgresNotNullViolationIsFatal(t *testing.T) {
	f := pgSetup(t)
	p := f.sink(f.cfg)
	f.ready(p)
	f.exec("ALTER TABLE " + f.table + " ADD COLUMN tenant text NOT NULL")
	err := p.Write(context.Background(), batchN(t, 2))
	if err == nil {
		t.Fatal("Write succeeded into a table with a required column it never fills")
	}
	if !IsFatal(err) || !strings.Contains(err.Error(), "23502") {
		t.Fatalf("Write = %v (class %s), want fatal with sqlstate 23502", err, ClassOf(err))
	}
}

// sink.postgres.database that does not exist is 3D000 at the login: fatal at
// the first check, not a network failure retried until boot_timeout.
func TestPostgresUnknownDatabaseIsFatal(t *testing.T) {
	f := pgSetup(t)
	cfg := f.cfg
	cfg.Database = "rlpg_no_such_database"
	p := f.sink(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := p.Health(ctx)
	if !IsFatal(err) || !strings.Contains(err.Error(), "3D000") {
		t.Fatalf("Health on a database that does not exist = %v (class %s), want fatal 3D000", err, ClassOf(err))
	}
}
