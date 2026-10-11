// Package app wires the server together and owns its lifecycle (SPEC §12).
//
// It lives here rather than in cmd/ so that the integration tests build the same
// stack the binary builds. A test that assembles its own approximation of the
// wiring proves nothing about what ships.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/receiver"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/sink"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/syslog"
)

// Build identity, set from cmd/ at link time.
var (
	version = "dev"
	commit  = "none"
)

// SetBuildInfo records the identity reported by --version and syslog_build_info.
func SetBuildInfo(v, c string) { version, commit = v, c }

// Exit codes, per SPEC §12.4.
const (
	ExitOK      = 0
	ExitConfig  = 1
	ExitSink    = 3
	ExitAborted = 130
)

// Run loads the configuration from args and runs the server to completion,
// returning the process exit code.
func Run(args []string) int {
	loaded, err := config.Load(config.Options{Args: args})
	if err != nil {
		if errors.Is(err, config.ErrExitEarly) {
			if loaded.Meta.Help {
				_ = config.Usage(os.Stdout)
			} else {
				fmt.Printf("go-syslog-server %s (%s, %s)\n", version, commit, runtime.Version())
			}
			return ExitOK
		}
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		return ExitConfig
	}
	if loaded.Meta.DumpConfig {
		_ = loaded.DumpINI(os.Stdout)
		return ExitOK
	}

	cfg := loaded.Config
	log := obs.NewLogger(os.Stdout, cfg.Log.Format, cfg.Log.Level)
	log.SetAccess(cfg.Log.Access, cfg.Log.AccessSample)

	a := &App{cfg: cfg, loaded: loaded, log: log, args: args}

	// Signals are wired here, at the process boundary, and turned into a
	// context. Everything below works from the context alone, which is what
	// lets the integration tests run the very same lifecycle in-process without
	// signalling the test binary itself.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	return a.run(ctx, stop)
}

// RunContext runs the server until ctx is cancelled, which triggers the same
// graceful shutdown a SIGTERM would. It exists for tests; the binary uses Run.
func RunContext(ctx context.Context, args []string) int {
	return runContext(ctx, args, nil)
}

// runContext is what RunContext and the integration tests share. hook, when set,
// is handed the assembled App before it starts, which is the only way a test can
// reach the abort channel: these instances run INSIDE the test binary, so a real
// second signal would be delivered to the test process itself.
func runContext(ctx context.Context, args []string, hook func(*App)) int {
	loaded, err := config.Load(config.Options{Args: args})
	if err != nil {
		return ExitConfig
	}
	log := obs.NewLogger(os.Stdout, loaded.Config.Log.Format, loaded.Config.Log.Level)
	log.SetAccess(loaded.Config.Log.Access, loaded.Config.Log.AccessSample)

	a := &App{cfg: loaded.Config, loaded: loaded, log: log, args: args}
	if hook != nil {
		hook(a)
	}
	return a.run(ctx, func() {})
}

// transport is what the four receivers have in common for the lifecycle.
type transport interface {
	Start(ctx context.Context)
	Close() error
	Addr() string
}

type namedTransport struct {
	name string
	t    transport
}

// App is the assembled server.
type App struct {
	args []string

	cfg    *config.Config
	loaded *config.Loaded // the BOOT snapshot; never replaced (see reload)

	// current is the last snapshot whose reloadable half was applied by a
	// SIGHUP; nil until the first one, meaning "the boot snapshot".
	current *config.Loaded
	log     *obs.Logger

	metrics *obs.Metrics
	limiter *ratelimit.Limiter
	filter  *filter.Filter
	sink    sink.Sink
	ring    ring.Ring
	health  *sink.Health
	workers *sink.Workers
	ingest  *receiver.Ingest
	admin   *Admin
	emg     *emergency.Store

	// transports in OPEN order: unix, udp, tcp, relp. They close in reverse, so
	// the window in which the server accepts RELP -- the transport with the
	// strongest guarantee -- is a subset of the window in which it accepts
	// anything at all (SPEC §12.1).
	transports []namedTransport

	// stopRecv cancels the receive context at shutdown phase 3. It is separate
	// from the signal context on purpose: phase 2 keeps accepting traffic after
	// the signal has already arrived.
	stopRecv context.CancelFunc

	// ready is closed once every transport is open. Tests wait on it.
	ready chan struct{}

	// abort replaces the OS signal channel that shutdown watches for a second
	// interrupt. It is nil in the binary; the integration tests set it.
	abort chan os.Signal

	// wrapSink, when set, decorates the real sink right after it is built. It is
	// nil in the binary. It exists because the one scenario that matters most --
	// a sink that stops accepting writes while the health check still passes, so
	// workers HOLD batches that are no longer in the ring -- cannot be produced
	// reliably from outside the process: a read-only directory does not stop a
	// SQLite file that is already open, and a stopped Redis fails the boot
	// instead of failing the writes.
	wrapSink func(sink.Sink) sink.Sink
}

func (a *App) run(ctx context.Context, stopSignals func()) int {
	if a.ready == nil {
		a.ready = make(chan struct{})
	}

	a.log.Info("starting",
		"version", version, "commit", commit, "go", runtime.Version(),
		"node_id", a.cfg.Node.ID,
		"ring_mode", a.cfg.Ring.Mode, "sink_type", a.cfg.Sink.Type,
		"transports", strings.Join(a.enabledTransports(), ","),
		"config_file", a.loaded.File)

	for _, w := range a.loaded.Warnings {
		a.log.Warn(w)
	}
	a.warnMemoryLimit()
	a.log.Debug("effective configuration", "config", a.loaded.Effective())

	a.metrics = obs.NewMetrics(a.cfg.Ring.Mode, a.cfg.Sink.Type,
		strings.Join(a.enabledTransports(), ","),
		obs.BuildInfo{Version: version, Commit: commit, Go: runtime.Version()})

	// --- step 2: the admin port opens FIRST --------------------------------
	//
	// It stays open through the whole startup, so a crash loop caused by an
	// unreachable sink is still diagnosable through /readyz and /metrics.
	a.admin = NewAdmin(a.metrics, a.cfg.Ring.HighWater)
	if a.cfg.Admin.Enabled {
		if err := a.admin.Listen(ctx, a.cfg.Admin.Bind); err != nil {
			a.log.Error("cannot bind the admin listener", "addr", a.cfg.Admin.Bind, "error", err)
			return ExitConfig
		}
		go func() {
			if err := a.admin.Serve(); err != nil {
				a.log.Error("admin listener stopped", "error", err)
			}
		}()
		defer a.admin.Shutdown(context.Background())
		a.log.Info("admin listener open", "addr", a.admin.Addr())
	}

	// --- step 3: the one writable root -------------------------------------
	//
	// The emergency directory is created NOW, not at the moment of the dump. A
	// directory that is only created during the incident fails during the
	// incident (SPEC §11.3).
	if code := a.prepareDirs(); code != ExitOK {
		return code
	}

	// --- step 4: the sink ---------------------------------------------------
	a.admin.SetBootState("connecting to the sink")
	if code := a.buildSink(ctx); code != ExitOK {
		return code
	}
	if a.wrapSink != nil {
		a.sink = a.wrapSink(a.sink)
	}
	defer a.sink.Close()

	a.health = sink.NewHealth(a.sink, sink.HealthConfig{
		Interval:   a.cfg.Sink.HealthInterval,
		Timeout:    a.cfg.Sink.HealthTimeout,
		FailAfter:  a.cfg.Sink.FailTimeout,
		MaxRetries: a.cfg.Sink.MaxRetries,
	}, a.log.Logger, a.metrics)

	a.admin.SetBootState("waiting for the sink")
	if err := a.health.WaitReady(ctx, a.cfg.Sink.BootTimeout,
		100*time.Millisecond, 5*time.Second); err != nil {
		if ctx.Err() != nil {
			a.log.Info("interrupted while waiting for the sink")
			return ExitAborted
		}
		a.log.Error("sink did not become usable within sink.boot_timeout",
			"timeout", a.cfg.Sink.BootTimeout.String(), "error", err)
		return ExitSink
	}

	// --- the ring, and everything that depends on it ------------------------
	if code := a.buildRing(); code != ExitOK {
		return code
	}
	defer a.ring.Close()
	a.admin.Attach(a.ring, a.health)

	if code := a.buildIngest(); code != ExitOK {
		return code
	}

	// --- step 5: reingest emergency dumps from a previous run ---------------
	if code := a.reingest(ctx); code != ExitOK {
		return code
	}

	// --- step 6: drain the backlog with NO transport open -------------------
	if code := a.drainBacklog(ctx); code != ExitOK {
		return code
	}

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	go a.health.Run(workerCtx)
	go a.limiter.RunSweeper(workerCtx.Done(), func() time.Duration {
		return a.limiter.Params().SweepInterval
	})
	go a.pollGauges(workerCtx)
	a.workers.Start(workerCtx)

	// --- step 7: the transports open LAST, and together ---------------------
	//
	// Opening UDP before the sink was ready would drop datagrams with nobody
	// knowing, because UDP cannot refuse: the only defence is not to be
	// listening, and let the emitter see ICMP port unreachable.
	recvCtx, stopRecv := context.WithCancel(context.Background())
	a.stopRecv = stopRecv
	defer stopRecv()

	if code := a.openTransports(ctx, recvCtx); code != ExitOK {
		a.closeTransports()
		return code
	}

	// --- step 8 -------------------------------------------------------------
	a.admin.SetBootState("")
	close(a.ready)

	go a.watchSIGHUP(workerCtx)

	select {
	case <-a.health.Fatal():
		a.log.Error("sink became permanently unusable", "error", a.health.FatalErr())
		return a.shutdown(stopWorkers, true)

	case <-ctx.Done():
		stopSignals() // restore default handling so a second signal is not swallowed
		a.log.Info("shutdown requested")
		return a.shutdown(stopWorkers, false)
	}
}

// Ready is closed once every transport is open.
func (a *App) Ready() <-chan struct{} { return a.ready }

func (a *App) enabledTransports() []string {
	var out []string
	if a.cfg.Listen.Unix != "" {
		out = append(out, "unix")
	}
	if a.cfg.Listen.UDP != "" {
		out = append(out, "udp")
	}
	if a.cfg.Listen.TCP != "" {
		out = append(out, "tcp")
	}
	if a.cfg.Listen.RELP != "" {
		out = append(out, "relp")
	}
	return out
}

func (a *App) prepareDirs() int {
	dirs := []string{a.cfg.Ring.DataDir}
	if a.cfg.Ring.Mode == config.RingMemory && a.cfg.Emergency.Enabled {
		dirs = append(dirs, a.cfg.Emergency.Dir)
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o750); err != nil {
			a.log.Error("cannot create a data directory; under a read-only root this "+
				"means the path does not derive from ring.data_dir", "dir", d, "error", err)
			return ExitConfig
		}
	}
	return ExitOK
}

// buildSink constructs the destination.
//
// Redis dials on construction, so a Redis that is not up yet would otherwise
// end the boot on the first attempt. The same backoff WaitReady uses is applied
// here, inside sink.boot_timeout, so a Redis starting alongside this process in
// the same compose file is simply waited for.
func (a *App) buildSink(ctx context.Context) int {
	switch a.cfg.Sink.Type {
	case config.SinkNoneType:
		a.sink = sink.NewNone()
		return ExitOK

	case config.SinkSQLiteType:
		s, err := sink.NewSQLite(sink.SQLiteConfig{
			Path:              a.cfg.SinkSQL.Path,
			Rotate:            a.cfg.SinkSQL.Rotate,
			KeepFiles:         a.cfg.SinkSQL.KeepFiles,
			Synchronous:       a.cfg.SinkSQL.Synchronous,
			BusyTimeout:       a.cfg.SinkSQL.BusyTimeout,
			WALAutocheckpoint: a.cfg.SinkSQL.WALAutocheckpoint,
		}, a.log.Logger, a.metrics)
		if err != nil {
			a.log.Error("cannot open the events database", "error", err)
			return ExitSink
		}
		a.sink = s
		a.log.Info("sink ready", "type", "sqlite", "path", a.cfg.SinkSQL.Path,
			"rotate", a.cfg.SinkSQL.Rotate, "synchronous", a.cfg.SinkSQL.Synchronous)
		return ExitOK

	case config.SinkPostgresType:
		// Nothing connects here: the pool is lazy and WaitReady owns the retry
		// loop, so the only failures are configuration and they exit 1.
		s, err := sink.NewPostgres(sink.PostgresConfig{
			Addr:           a.cfg.SinkPG.Addr,
			Username:       a.cfg.SinkPG.Username,
			Password:       a.cfg.SinkPG.Password,
			Database:       a.cfg.SinkPG.Database,
			Table:          a.cfg.SinkPG.Table,
			PoolSize:       a.cfg.SinkPG.PoolSize,
			TLS:            a.cfg.SinkPG.TLS,
			TLSCA:          a.cfg.SinkPG.TLSCA,
			TLSInsecure:    a.cfg.SinkPG.TLSInsecure,
			ConnectTimeout: a.cfg.Sink.OpTimeout,
			AppName:        "sls/" + a.cfg.Node.ID,
		}, a.log.Logger, a.metrics)
		if err != nil {
			a.log.Error("cannot configure the PostgreSQL sink", "error", err)
			return ExitConfig
		}
		a.sink = s
		a.log.Info("sink configured", "type", "postgres", "addr", a.cfg.SinkPG.Addr,
			"database", a.cfg.SinkPG.Database, "table", a.cfg.SinkPG.Table,
			"pool_size", a.cfg.SinkPG.PoolSize, "tls", a.cfg.SinkPG.TLS)
		return ExitOK

	case config.SinkMySQLType:
		// Lazy like postgres: sql.OpenDB does not connect, WaitReady retries,
		// and the only failures here are configuration (SPEC §10.7.1).
		s, err := sink.NewMySQL(sink.MySQLConfig{
			Addr:             a.cfg.SinkMySQL.Addr,
			Username:         a.cfg.SinkMySQL.Username,
			Password:         a.cfg.SinkMySQL.Password,
			Database:         a.cfg.SinkMySQL.Database,
			Table:            a.cfg.SinkMySQL.Table,
			PoolSize:         a.cfg.SinkMySQL.PoolSize,
			TLS:              a.cfg.SinkMySQL.TLS,
			TLSCA:            a.cfg.SinkMySQL.TLSCA,
			TLSInsecure:      a.cfg.SinkMySQL.TLSInsecure,
			ConnectTimeout:   a.cfg.Sink.OpTimeout,
			AppName:          "sls/" + a.cfg.Node.ID,
			MaxRawBytes:      maxRawBytes(a.cfg),
			MaxEnvelopeBytes: maxEnvelopeBytes(a.cfg),
		}, a.log.Logger, a.metrics)
		if err != nil {
			a.log.Error("cannot configure the MySQL sink", "error", err)
			return ExitConfig
		}
		a.sink = s
		a.log.Info("sink configured", "type", "mysql", "addr", a.cfg.SinkMySQL.Addr,
			"database", a.cfg.SinkMySQL.Database, "table", a.cfg.SinkMySQL.Table,
			"pool_size", a.cfg.SinkMySQL.PoolSize, "tls", a.cfg.SinkMySQL.TLS)
		return ExitOK

	case config.SinkNATSType:
		// The connection is opened by the first health check (SPEC §10.8.1); a
		// CA or credentials file that does not load is the only failure here.
		s, err := sink.NewNATS(sink.NATSConfig{
			URLs:             a.cfg.SinkNATS.URLs,
			Username:         a.cfg.SinkNATS.Username,
			Password:         a.cfg.SinkNATS.Password,
			Token:            a.cfg.SinkNATS.Token,
			Credentials:      a.cfg.SinkNATS.Credentials,
			TLS:              a.cfg.SinkNATS.TLS,
			TLSCA:            a.cfg.SinkNATS.TLSCA,
			TLSInsecure:      a.cfg.SinkNATS.TLSInsecure,
			Stream:           a.cfg.SinkNATS.Stream,
			Subject:          a.cfg.SinkNATS.Subject,
			ClientName:       "sls/" + a.cfg.Node.ID,
			ConnectTimeout:   a.cfg.Sink.OpTimeout,
			OpTimeout:        a.cfg.Sink.OpTimeout,
			MaxRawBytes:      maxRawBytes(a.cfg),
			MaxEnvelopeBytes: maxEnvelopeBytes(a.cfg),
			// Every publish this process can have in flight at once: the drain
			// workers' batches plus the write-through receivers of ring=none
			// (SPEC §10.8.2).
			MaxPending: a.cfg.Sink.Workers*a.cfg.Sink.Batch + a.cfg.Ring.MaxInflight,
		}, a.log.Logger, a.metrics)
		if err != nil {
			a.log.Error("cannot configure the NATS sink", "error", err)
			return ExitConfig
		}
		a.sink = s
		a.log.Info("sink configured", "type", "nats", "urls", a.cfg.SinkNATS.URLs,
			"stream", a.cfg.SinkNATS.Stream, "subject", a.cfg.SinkNATS.Subject,
			"tls", a.cfg.SinkNATS.TLS)
		return ExitOK

	case config.SinkRedisType:
		deadline := time.Now().Add(a.cfg.Sink.BootTimeout)
		backoff := 100 * time.Millisecond
		for attempt := 1; ; attempt++ {
			octx, cancel := context.WithTimeout(ctx, a.cfg.Sink.OpTimeout)
			s, err := sink.NewRedis(octx, sink.RedisConfig{
				Addrs:               a.cfg.SinkRedis.Addrs,
				Username:            a.cfg.SinkRedis.Username,
				Password:            a.cfg.SinkRedis.Password,
				DB:                  a.cfg.SinkRedis.DB,
				SentinelMaster:      a.cfg.SinkRedis.SentinelMaster,
				TLS:                 a.cfg.SinkRedis.TLS,
				TLSCA:               a.cfg.SinkRedis.TLSCA,
				TLSInsecure:         a.cfg.SinkRedis.TLSInsecure,
				Stream:              a.cfg.SinkRedis.Stream,
				MaxLen:              a.cfg.SinkRedis.MaxLen,
				RingScaleEachConn:   a.cfg.SinkRedis.RingScaleEachConn,
				PipelineMultiplex:   a.cfg.SinkRedis.PipelineMultiplex,
				ReadBufferEachConn:  a.cfg.SinkRedis.ReadBufferEachConn,
				WriteBufferEachConn: a.cfg.SinkRedis.WriteBufferEachConn,
				ClientName:          "sls/" + a.cfg.Node.ID,
			}, a.log.Logger, a.metrics)
			cancel()
			if err == nil {
				a.sink = s
				a.log.Info("sink ready", "type", "redis",
					"addrs", a.cfg.SinkRedis.Addrs, "stream", a.cfg.SinkRedis.Stream)
				return ExitOK
			}
			if sink.IsFatal(err) {
				// OOM, WRONGTYPE, a bad password: none of it changes by waiting.
				a.log.Error("Redis refused this server permanently", "error", err)
				return ExitSink
			}
			if ctx.Err() != nil {
				return ExitAborted
			}
			if time.Now().After(deadline) {
				a.log.Error("Redis did not become reachable within sink.boot_timeout",
					"addrs", a.cfg.SinkRedis.Addrs, "error", err)
				return ExitSink
			}
			a.log.Warn("Redis not reachable yet, retrying",
				"attempt", attempt, "error", err, "retry_in", backoff.String())
			select {
			case <-ctx.Done():
				return ExitAborted
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}

	a.log.Error("unknown sink type", "type", a.cfg.Sink.Type)
	return ExitConfig
}

// maxRawBytes is the longest raw line an ENABLED transport can hand to the ring
// (SPEC §3.9). A transport that is off accepts nothing, so its ceiling does not
// count: an operator who serves only UDP to NATS owes nothing to the 16 MiB
// default of tcp.max_frame_bytes.
func maxRawBytes(c *config.Config) int64 {
	var n int64
	if c.Listen.UDP != "" || c.Listen.Unix != "" {
		n = receiver.MaxDatagramBytes
	}
	if c.Listen.TCP != "" {
		n = max(n, c.TCP.MaxFrameBytes)
	}
	if c.Listen.RELP != "" {
		n = max(n, c.RELP.MaxFrameBytes)
	}
	return n
}

// maxEnvelopeBytes is the largest envelope the longest raw line can produce
// (SPEC §3.9). The listener field holds the address a socket actually bound,
// which can be longer than the one configured -- ":1514" binds as "[::]:1514" --
// so every configured address is given the 64 bytes of the widest one a kernel
// reports, a zoned IPv6 with its port.
func maxEnvelopeBytes(c *config.Config) int64 {
	listener := 64
	for _, addr := range []string{c.Listen.UDP, c.Listen.TCP, c.Listen.RELP, c.Listen.Unix} {
		listener = max(listener, len(addr)+64)
	}
	return envelope.MaxBytes(maxRawBytes(c), int64(listener), int64(len(c.Node.ID)))
}

func (a *App) parseOptions() syslog.Options {
	opt := syslog.Options{
		SDMaxElements:   a.cfg.Parse.SDMaxElements,
		SDMaxParams:     a.cfg.Parse.SDMaxParams,
		DefaultFacility: a.cfg.Parse.DefaultFacility,
		DefaultSeverity: a.cfg.Parse.DefaultSeverity,
	}
	switch a.cfg.Parse.Prefer {
	case config.Prefer5424:
		opt.Prefer = syslog.RFC5424
	case config.Prefer3164:
		opt.Prefer = syslog.RFC3164
	}
	return opt
}

func (a *App) buildRing() int {
	switch a.cfg.Ring.Mode {
	case config.RingNone:
		// ring=none writes through from the receiver and never touches the drain
		// workers, so it gets the instrumented adapter: the full parse, the
		// sink metrics and the fatal escalation, none of which would happen
		// otherwise (SPEC §9.5).
		a.ring = ring.NewNone(
			sink.NewInstrumented(a.sink, a.health, a.metrics, a.cfg.Node.ID, a.parseOptions()),
			a.cfg.Ring.MaxInflight, a.cfg.Sink.OpTimeout)

	case config.RingMemory:
		a.ring = ring.NewMemory(a.cfg.Ring.CapacityItems, a.cfg.Ring.MaxBytes,
			a.cfg.Sink.BatchMaxDelay)

	case config.RingSQLiteMode:
		r, err := ring.NewSQLite(ring.SQLiteConfig{
			Path:              a.cfg.RingSQL.Path,
			Synchronous:       a.cfg.RingSQL.Synchronous,
			CommitBatch:       a.cfg.RingSQL.CommitBatch,
			CommitMaxDelay:    a.cfg.RingSQL.CommitMaxDelay,
			BusyTimeout:       a.cfg.RingSQL.BusyTimeout,
			WALAutocheckpoint: a.cfg.RingSQL.WALAutocheckpoint,
			CapacityItems:     a.cfg.Ring.CapacityItems,
			CapacityBytes:     a.cfg.Ring.MaxBytes,
			Quarantined: func(n int) {
				a.metrics.RingQuarantinedTotal.Add(uint64(n))
			},
		}, a.log.Logger)
		if err != nil {
			a.log.Error("cannot open the ring database", "error", err)
			return ExitConfig
		}
		a.ring = r
		a.log.Info("ring ready", "mode", "sqlite", "path", a.cfg.RingSQL.Path,
			"synchronous", a.cfg.RingSQL.Synchronous, "backlog", r.Backlog())

	default:
		a.log.Error("unknown ring mode", "mode", a.cfg.Ring.Mode)
		return ExitConfig
	}

	// The emergency dump belongs to the memory ring: with sqlite the items are
	// already on disk, and with none there is never a backlog (SPEC §11).
	if a.cfg.Ring.Mode == config.RingMemory && a.cfg.Emergency.Enabled {
		a.emg = emergency.NewStore(a.cfg.Emergency.Dir, a.cfg.Node.ID)
	}

	a.workers = sink.NewWorkers(a.ring, a.sink, a.health, sink.WorkerConfig{
		Workers:    a.cfg.Sink.Workers,
		Batch:      a.cfg.Sink.Batch,
		OpTimeout:  a.cfg.Sink.OpTimeout,
		BackoffMin: a.cfg.Sink.RetryBackoffMin,
		BackoffMax: a.cfg.Sink.RetryBackoffMax,
		NodeID:     a.cfg.Node.ID,
		Parse:      a.parseOptions(),
	}, a.log.Logger, a.metrics)
	return ExitOK
}

func (a *App) buildIngest() int {
	a.limiter = ratelimit.New(a.rateLimitParamsFrom(a.cfg), 0)

	rule, err := filter.Compile(a.cfg.Filter.MinSeverity, a.cfg.Filter.Facilities)
	if err != nil {
		a.log.Error("filter", "error", err)
		return ExitConfig
	}
	a.filter = filter.New(rule)
	a.log.Info("filter", "min_severity", syslog.SeverityName(rule.MinSeverity()),
		"facilities", a.cfg.Filter.Facilities)

	a.ingest = receiver.NewIngest(a.ring, a.health, a.filter, a.limiter, a.metrics, a.log,
		receiver.IngestConfig{
			DefaultFacility: a.cfg.Parse.DefaultFacility,
			DefaultSeverity: a.cfg.Parse.DefaultSeverity,
		})
	return ExitOK
}

// openTransports binds every configured transport, in the order unix, udp, tcp,
// relp. Any failure closes what was already opened and ends the boot: a server
// that silently runs with one transport missing looks healthy on every probe
// while an emitter's messages go nowhere.
func (a *App) openTransports(bindCtx, recvCtx context.Context) int {
	if a.cfg.Listen.Unix != "" {
		d, err := receiver.NewDatagram(bindCtx, receiver.DatagramConfig{
			Addr: a.cfg.Listen.Unix, Unix: true, Mode: a.cfg.Listen.UnixMode,
			ReadBuffer: a.cfg.Listen.UnixReadBuffer,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the unix transport", "path", a.cfg.Listen.Unix, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"unix", d})
	}

	if a.cfg.Listen.UDP != "" {
		d, err := receiver.NewDatagram(bindCtx, receiver.DatagramConfig{
			Addr: a.cfg.Listen.UDP, Listeners: a.cfg.Listen.UDPListeners,
			ReadBuffer: a.cfg.Listen.UDPReadBuffer,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the udp transport", "addr", a.cfg.Listen.UDP, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"udp", d})
	}

	if a.cfg.Listen.TCP != "" {
		framing, auto := envelope.FramingLF, true
		switch a.cfg.TCP.Framing {
		case config.FramingOctet:
			framing, auto = envelope.FramingOctet, false
		case config.FramingLF:
			framing, auto = envelope.FramingLF, false
		}
		t, err := receiver.NewTCP(bindCtx, receiver.TCPConfig{
			Addr:                a.cfg.Listen.TCP,
			Listeners:           a.cfg.Listen.TCPListeners,
			Framing:             framing,
			FramingAuto:         auto,
			MaxFrameBytes:       a.cfg.TCP.MaxFrameBytes,
			IdleTimeout:         a.cfg.TCP.IdleTimeout,
			MaxConns:            a.cfg.TCP.MaxConns,
			MaxConnsPerIP:       a.cfg.TCP.MaxConnsPerIP,
			BackpressureTimeout: a.cfg.TCP.BackpressureTimeout,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the tcp transport", "addr", a.cfg.Listen.TCP, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"tcp", t})
	}

	if a.cfg.Listen.RELP != "" {
		r, err := receiver.NewRELP(bindCtx, receiver.RELPConfig{
			Addr:                a.cfg.Listen.RELP,
			Listeners:           a.cfg.Listen.RELPListeners,
			MaxFrameBytes:       a.cfg.RELP.MaxFrameBytes,
			IdleTimeout:         a.cfg.RELP.IdleTimeout,
			MaxConns:            a.cfg.RELP.MaxConns,
			MaxConnsPerIP:       a.cfg.RELP.MaxConnsPerIP,
			Window:              a.cfg.RELP.Window,
			BackpressureTimeout: a.cfg.RELP.BackpressureTimeout,
			Version:             version,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the relp transport", "addr", a.cfg.Listen.RELP, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"relp", r})
	}

	for _, nt := range a.transports {
		nt.t.Start(recvCtx)
		// The configured number and the wire number differ inside a container
		// (1514 against 514), and saying both is what keeps an operator from
		// chasing the wrong port during an incident (SPEC §4.1).
		a.log.Info("transport open", "transport", nt.name, "addr", nt.t.Addr())
	}
	return ExitOK
}

// TransportAddr reports the bound address of a transport by name, for tests that
// bind to port 0.
func (a *App) TransportAddr(name string) string {
	for _, nt := range a.transports {
		if nt.name == name {
			return nt.t.Addr()
		}
	}
	return ""
}

// closeTransports closes in REVERSE open order: relp first, unix last.
func (a *App) closeTransports() {
	for i := len(a.transports) - 1; i >= 0; i-- {
		nt := a.transports[i]
		if err := nt.t.Close(); err != nil {
			a.log.Warn("closing a transport", "transport", nt.name, "error", err)
		}
	}
}

// shutdown runs the five phases of SPEC §12.3. A second signal skips straight to
// the exit, accepting the loss.
func (a *App) shutdown(stopWorkers context.CancelFunc, fatal bool) int {
	abort := a.abort
	if abort == nil {
		abort = make(chan os.Signal, 1)
		signal.Notify(abort, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		defer signal.Stop(abort)
	}

	done := make(chan int, 1)
	go func() { done <- a.gracefulShutdown(stopWorkers, fatal) }()

	select {
	case code := <-done:
		return code
	case sig := <-abort:
		a.log.Warn("second signal during shutdown; exiting immediately and accepting the loss",
			"signal", sig.String())
		st := a.ring.Stats()
		if st.Items > 0 {
			if a.cfg.Ring.Mode == config.RingSQLiteMode {
				// The rows are on disk and the next boot drains them: cut
				// short, not lost.
				a.log.Warn("messages left in ring.db for the next boot", "count", st.Items)
			} else {
				a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(st.Items))
				a.log.Error("events lost to the abort", "count", st.Items)
			}
		}
		return ExitAborted
	}
}

func (a *App) gracefulShutdown(stopWorkers context.CancelFunc, fatal bool) int {
	// Phase 1: /readyz starts failing so the load balancer drops this endpoint.
	a.admin.StartDraining()

	// Phase 2: keep receiving for pre_stop_delay. It matters more here than in
	// the sibling service: a UDP emitter gets no error at all when the socket
	// disappears, it simply throws the datagram away.
	if d := a.cfg.Shutdown.PreStopDelay; d > 0 && !fatal {
		a.log.Info("pre-stop delay: still receiving", "delay", d.String())
		time.Sleep(d)
	}

	// Phase 3: stop receiving. The receive context is cancelled FIRST: that is
	// what releases a reader parked in the backpressure loop (and makes RELP
	// answer "500 shutdown"); closing the transports first would wait out
	// backpressure_timeout on every such reader. A message already handed to
	// the ring is not affected by the cancellation -- Enqueue awaits its group
	// commit or its synchronous write regardless (SPEC §9.4, §9.5, §12.3) --
	// so what was read finishes entering the ring and gets its confirmation.
	// Then the transports close in reverse open order.
	a.log.Info("closing transports")
	if a.stopRecv != nil {
		a.stopRecv()
	}
	a.closeTransports()

	// Phase 4: empty the ring.
	//
	// The running workers ARE the drain, so they are left alone to finish first.
	// Stopping them up front would make every in-flight batch fail against a
	// cancelled context and be abandoned -- turning a clean shutdown into an
	// emergency dump of work that would have been delivered.
	code := ExitOK
	if fatal {
		code = ExitSink
	}
	if !fatal {
		a.waitDrained(a.cfg.Shutdown.DrainTimeout)
	}

	stopWorkers()
	a.workers.Wait()

	// A worker retrying a failed batch holds items that are no longer in the
	// ring (SPEC §10.2). Without this collection they would be lost despite
	// having been confirmed -- on RELP, to an emitter that then forgot them.
	leftover := a.workers.Pending()
	if len(leftover) > 0 {
		a.log.Warn("workers surrendered batches they were retrying", "count", len(leftover))
	}
	leftover = append(leftover, a.ring.Drain()...)

	if a.cfg.Ring.Mode == config.RingSQLiteMode {
		// The rows were never deleted from ring.db: the next boot's backlog
		// drain delivers them. Nothing is lost, so nothing is counted as
		// dropped and nothing is dumped -- a dump would deliver every row
		// twice -- and the exit code stays clean unless the sink is FATAL
		// (SPEC §12.3). What the workers surrendered is still in the table,
		// on top of the rows the ring itself still counts.
		if n := a.ring.Stats().Items + int64(len(leftover)); n > 0 {
			a.log.Warn("messages left in ring.db for the next boot", "count", n)
		}
	} else if len(leftover) > 0 {
		a.log.Error("messages still pending after the drain", "count", len(leftover))
		if !a.dumpEmergency(leftover) && a.emg == nil {
			// No emergency store (ring=memory with emergency.enabled=false):
			// the loss is the abort's. A dump that FAILED was already counted
			// by dumpEmergency as emergency_failed, and the same messages are
			// not lost twice (SPEC §13.1).
			a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(len(leftover)))
		}
		code = ExitSink
	}

	// Phase 5: the sink and the ring close through the defers in run.
	a.log.Info("shutdown complete", "exit_code", code)
	return code
}

// waitDrained blocks until the ring is empty or the deadline passes.
func (a *App) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if a.ring.Stats().Items == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := a.ring.Stats(); st.Items > 0 {
		a.log.Warn("drain deadline reached with messages still queued",
			"items", st.Items, "timeout", timeout.String())
	}
}

func (a *App) rateLimitParamsFrom(c *config.Config) ratelimit.Params {
	exempt, _ := config.ParsePrefixes(c.RateLimit.Exempt)
	return ratelimit.Params{
		Enabled:       c.RateLimit.Enabled,
		Rate:          c.RateLimit.Rate,
		Burst:         c.RateLimit.Burst,
		V4Mask:        c.RateLimit.V4Mask,
		V6Mask:        c.RateLimit.V6Mask,
		Exempt:        exempt,
		MaxBuckets:    c.RateLimit.MaxBuckets,
		SweepInterval: c.RateLimit.SweepInterval,
	}
}

// pollGauges refreshes the gauges whose owners do not push them.
func (a *App) pollGauges(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.metrics.RateLimitBuckets.Set(int64(a.limiter.Len()))
		}
	}
}

// memoryAdvice is what the GOMEMLIMIT check of SPEC §14.4 compares against.
//
// Only a memory ring holds ring.max_bytes in RAM. With sqlite the items live on
// disk and with none there is no ring; what is resident then is the raw lines
// in flight -- bounded by max_inflight (none) or by the drain workers' batches
// (sqlite), plus the envelope memory of SPEC §14.3 -- and those ceilings are
// worst cases far too large to suggest as a limit. So for those modes there is
// no number to compare against: only the absence of a limit is worth a warning,
// with the ceiling named.
func memoryAdvice(c *config.Config) (want int64, resident string) {
	switch c.Ring.Mode {
	case config.RingMemory:
		return c.Ring.MaxBytes * 2, "ring.max_bytes"
	case config.RingNone:
		return 0, "ring.max_inflight x tcp.max_frame_bytes"
	default:
		return 0, "sink.workers x sink.batch x tcp.max_frame_bytes plus the envelope memory of SPEC 14.3"
	}
}

// warnMemoryLimit implements the GOMEMLIMIT check of SPEC §14.4. The value is
// never set automatically: reading cgroups correctly is subtler than it looks,
// and silently overriding what an operator configured elsewhere would be worse
// than a warning.
func (a *App) warnMemoryLimit() {
	limit := debug.SetMemoryLimit(-1) // read-only probe
	want, resident := memoryAdvice(a.cfg)

	if limit == 1<<63-1 {
		// The runtime reports the same "no limit" for an absent variable and
		// for GOMEMLIMIT=off; the operator deserves to be told which.
		state := "not set"
		if os.Getenv("GOMEMLIMIT") == "off" {
			state = "off"
		}
		if want > 0 {
			a.log.Warn("GOMEMLIMIT is "+state+"; a ring at its byte ceiling plus the drain "+
				"workers' envelopes can push this process into an OOM kill",
				"ring_max_bytes", a.cfg.Ring.MaxBytes, "suggested_gomemlimit", want)
			return
		}
		a.log.Warn("GOMEMLIMIT is "+state+"; the resident set is bounded by "+resident+
			", and without a limit the OOM killer arrives before the GC",
			"ring_mode", a.cfg.Ring.Mode)
		return
	}
	if want > 0 && limit < want {
		a.log.Warn("GOMEMLIMIT is below twice ring.max_bytes",
			"gomemlimit", limit, "ring_max_bytes", a.cfg.Ring.MaxBytes,
			"suggested_gomemlimit", want)
	}
}
