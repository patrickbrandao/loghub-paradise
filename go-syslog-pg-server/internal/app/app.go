// Package app wires the server together and owns its lifecycle (SPEC §13).
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/receiver"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/sink"
)

// Build identity, set from cmd/ at link time.
var (
	version = "dev"
	commit  = "none"
)

// SetBuildInfo records the identity reported by --version and syslog_build_info.
func SetBuildInfo(v, c string) { version, commit = v, c }

// Exit codes, per SPEC §13.5. 100..114 are go-loghub-ident's and never pass
// through here: the library exits on its own.
const (
	ExitOK      = 0
	ExitConfig  = 1
	ExitSink    = 3
	ExitAborted = 130
)

// IdentityInit resolves the process identity. In the binary it exports DATADIR
// and calls go-loghub-ident's Initialize, which may run only once per process
// (cmd/go-syslog-pg-server); the tests pass a function returning a fixed identity.
type IdentityInit func(dataDir string) (ident.Identity, error)

// Main loads the configuration from args and runs the server to completion,
// returning the process exit code.
func Main(args []string, initIdentity IdentityInit) int {
	// SIGHUP and SIGQUIT are caught for the whole life of the process, from
	// its first line. Go's default action ends the process for both -- SIGHUP
	// with exit 129 and no log line, SIGQUIT with a goroutine dump and exit 2
	// -- and neither is an exit code of SPEC §13.5. The boot alone can last
	// sink.boot_timeout plus the reingestion, and the compose file tells
	// operators to send HUP. The channels are never
	// stopped: the process ends when Main returns.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGQUIT)

	// The environment as the process received it. Boot step 2 exports
	// DATADIR for go-loghub-ident, and a SIGHUP that read the exported value
	// would see it as an env override of server.data_dir: an INI change to
	// data_dir would then be masked instead of reported as restart-only.
	origDataDir, hadDataDir := os.LookupEnv("DATADIR")
	opts := config.Options{Args: args, LookupEnv: func(k string) (string, bool) {
		if k == "DATADIR" {
			return origDataDir, hadDataDir
		}
		return os.LookupEnv(k)
	}}
	loaded, err := config.Load(opts)
	if err != nil {
		if errors.Is(err, config.ErrExitEarly) {
			if loaded != nil && loaded.Meta.Help {
				_ = config.Usage(os.Stdout)
			} else {
				fmt.Printf("go-syslog-pg-server %s (%s, %s)\n", version, commit, runtime.Version())
			}
			return ExitOK
		}
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		return ExitConfig
	}
	if loaded.Meta.DumpConfig {
		// The dump runs before the identity is resolved, on purpose: it must
		// not touch the disk (SPEC §13.1 step 1). The [identity] block shows
		// what the environment says, which is what go-loghub-ident reads first.
		_ = loaded.DumpINI(os.Stdout, identityFromEnv())
		return ExitOK
	}

	// SPEC §13.1 step 2: the one writable root exists before the identity
	// library may need to persist a generated agent_uuid into it.
	if err := os.MkdirAll(loaded.Config.Server.DataDir, 0o750); err != nil {
		fmt.Fprintf(os.Stderr, "cannot create server.data_dir %s: %v\n", loaded.Config.Server.DataDir, err)
		return ExitConfig
	}
	id, err := initIdentity(loaded.Config.Server.DataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "identity error: %v\n", err)
		return ExitConfig
	}

	cfg := loaded.Config
	log := obs.NewLogger(os.Stdout, cfg.Log.Format, cfg.Log.Level)
	log.SetAccess(cfg.Log.Access, cfg.Log.AccessSample)

	a := &App{cfg: cfg, loaded: loaded, opts: opts, log: log, id: id, hup: hup, quit: quit}

	// Signals are wired here, at the process boundary, and turned into a
	// context. Everything below works from the context alone, which is what
	// lets the integration tests run the very same lifecycle in-process.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return a.run(ctx, stop)
}

// identityFromEnv is the informative [identity] section of --dump-config.
func identityFromEnv() config.InfoSection {
	sec := config.InfoSection{
		Name: "identity",
		Note: "read by go-loghub-ident at boot (env, then $DATADIR files, then generated); not parameters of this file",
	}
	for _, k := range []string{"AGENT_UUID", "AGENT_NAME", "WORKSPACE", "HOSTNAME", "MACHINE_ID", "MACHINE_ID_FILE"} {
		v, ok := os.LookupEnv(k)
		if !ok {
			v = "(not set)"
		}
		sec.Entries = append(sec.Entries, config.InfoEntry{Key: strings.ToLower(k), Value: v})
	}
	return sec
}

// RunContext runs the server until ctx is cancelled, which triggers the same
// graceful shutdown a SIGTERM would. It exists for tests; the binary uses Main.
func RunContext(ctx context.Context, args []string, id ident.Identity) int {
	return runContext(ctx, args, id, nil)
}

// runContext is what RunContext and the integration tests share. hook, when
// set, is handed the assembled App before it starts.
func runContext(ctx context.Context, args []string, id ident.Identity, hook func(*App)) int {
	opts := config.Options{Args: args}
	loaded, err := config.Load(opts)
	if err != nil {
		return ExitConfig
	}
	if err := os.MkdirAll(loaded.Config.Server.DataDir, 0o750); err != nil {
		return ExitConfig
	}
	if err := id.Validate(); err != nil {
		return ExitConfig
	}
	log := obs.NewLogger(os.Stdout, loaded.Config.Log.Format, loaded.Config.Log.Level)
	log.SetAccess(loaded.Config.Log.Access, loaded.Config.Log.AccessSample)

	a := &App{cfg: loaded.Config, loaded: loaded, opts: opts, log: log, id: id}
	if hook != nil {
		hook(a)
	}
	return a.run(ctx, func() {})
}

// transport is what the three receivers have in common for the lifecycle.
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
	opts config.Options

	cfg    *config.Config
	loaded *config.Loaded // the BOOT snapshot; never replaced (see reload)

	// current is the last snapshot whose reloadable half a SIGHUP applied;
	// nil until the first one, meaning "the boot snapshot".
	current *config.Loaded
	log     *obs.Logger
	id      ident.Identity

	metrics *obs.Metrics
	sink    sink.Sink
	ring    ring.Ring
	health  *sink.Health
	workers *sink.Workers
	direct  *sink.Instrumented // boot reingestion
	ingest  *receiver.Ingest
	admin   *Admin
	emg     *emergency.Store

	// transports in OPEN order: unix, udp, tcp. They close in reverse.
	transports []namedTransport

	// stopRecv cancels the receive context at shutdown phase 3. It is separate
	// from the signal context: phase 2 keeps accepting traffic after the
	// signal has arrived.
	stopRecv context.CancelFunc

	// ready is closed once every transport is open. Tests wait on it.
	ready chan struct{}

	// abort replaces the OS signal channel that shutdown watches for a second
	// interrupt. It is nil in the binary; the integration tests set it.
	abort chan os.Signal

	// hup and quit are the process-wide SIGHUP and SIGQUIT channels, set by
	// Main. In-process runs (RunContext, the tests) leave them nil: run then
	// registers SIGHUP for its own lifetime, and SIGQUIT is not watched.
	hup, quit <-chan os.Signal

	// reloadOpen is true between the end of the boot (step 11) and the start
	// of the shutdown: the only window in which a SIGHUP may swap anything.
	// Outside it the signal is logged and ignored, never fatal.
	reloadOpen atomic.Bool

	// quitting records that the context was cancelled by SIGQUIT, which
	// aborts at once instead of shutting down gracefully (SPEC §13.4).
	quitting atomic.Bool

	// dumping is the number of leftovers handed to the emergency dump while
	// it runs, for abortLoss (0 otherwise).
	dumping atomic.Int64

	// wrapSink, when set, decorates the real sink right after it is built. It
	// is nil in the binary; the tests use it to produce a sink that stops
	// accepting writes while its health check still passes.
	wrapSink func(sink.Sink) sink.Sink

	started time.Time
}

func (a *App) run(ctx context.Context, stopSignals func()) int {
	if a.ready == nil {
		a.ready = make(chan struct{})
	}
	a.started = time.Now()

	// lifeCtx ends when run returns; it bounds the signal watchers below.
	lifeCtx, endLife := context.WithCancel(context.Background())
	defer endLife()
	if a.hup == nil {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		defer signal.Stop(ch)
		a.hup = ch
	}
	go a.watchSIGHUP(lifeCtx)
	if a.quit != nil {
		// SIGQUIT cancels the same context as SIGTERM, so every boot step
		// that honours a SIGTERM (exit 130) honours it too; once the server
		// is up, quitting turns the shutdown into an immediate abort.
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-a.quit:
				a.quitting.Store(true)
				cancel()
			case <-lifeCtx.Done():
			}
		}()
	}

	a.log.Info("starting",
		"version", version, "commit", commit, "go", runtime.Version(),
		"from", a.id.From, "space", a.id.Space, "agent_name", a.id.AgentName,
		"hostname", a.id.Hostname, "data_dir", a.cfg.Server.DataDir,
		"ring_mode", ringMode, "sink_type", sinkType, "table", a.cfg.SinkPG.Table,
		"transports", strings.Join(a.enabledTransports(), ","),
		"config_file", a.loaded.File)
	for _, w := range a.loaded.Warnings {
		a.log.Warn(w)
	}
	for k, note := range a.loaded.Notes {
		a.log.Info("configuration adjusted", "key", k, "note", note)
	}
	a.warnMemoryLimit()
	a.log.Debug("effective configuration", "config", a.loaded.Effective())

	a.metrics = obs.NewMetrics(obs.Info{
		RingMode: ringMode, SinkType: sinkType,
		Transports: strings.Join(a.enabledTransports(), ","),
		Version:    version, Commit: commit, Go: runtime.Version(),
		From: a.id.From, Space: a.id.Space, AgentName: a.id.AgentName,
	})

	// --- step 5: the admin port opens FIRST -------------------------------
	//
	// It stays open through the whole startup, so a crash loop caused by an
	// unreachable destination is still diagnosable through /readyz and
	// /metrics.
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

	// --- step 6: the emergency directory exists BEFORE the incident -------
	if a.cfg.Emergency.Enabled {
		a.emg = emergency.NewStore(a.cfg.Emergency.Dir, a.id.From)
		if err := a.emg.Prepare(); err != nil {
			a.log.Error("cannot prepare the emergency directory; under a read-only root this "+
				"means the path does not derive from server.data_dir",
				"dir", a.cfg.Emergency.Dir, "error", err)
			return ExitConfig
		}
	}

	// --- step 7: the sink ---------------------------------------------------
	a.admin.SetBootState("connecting to the sink")
	if code := a.buildSink(); code != ExitOK {
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
		if sink.IsFatal(err) {
			// A refusal that waiting will not change ends the boot at once;
			// saying "within sink.boot_timeout" would send the operator after
			// a timeout that never ran.
			a.log.Error("the sink refused this server permanently", "error", err)
		} else {
			a.log.Error("sink did not become usable within sink.boot_timeout",
				"timeout", a.cfg.Sink.BootTimeout.String(), "error", err)
		}
		return ExitSink
	}

	// --- step 8: the ring, and everything that depends on it --------------
	a.direct = sink.NewInstrumented(a.sink, a.health, a.workerConfig(), a.log.Logger, a.metrics)
	a.ring = ring.NewMemory(a.cfg.Ring.CapacityItems, a.cfg.Ring.MaxBytes, a.cfg.Sink.BatchMaxDelay)
	defer a.ring.Close()
	a.workers = sink.NewWorkers(a.ring, a.sink, a.health, a.workerConfig(), a.log.Logger, a.metrics)
	a.admin.Attach(a.ring, a.health)
	a.ingest = receiver.NewIngest(a.ring, a.health, a.metrics, a.log, receiver.IngestConfig{
		Identity:        a.id,
		MaxMessageBytes: int(a.cfg.Syslog.MaxMessageBytes),
	})

	// --- step 9: reingest emergency dumps from a previous run -------------
	if code := a.reingest(ctx); code != ExitOK {
		return code
	}

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	go a.health.Run(workerCtx)
	a.workers.Start(workerCtx)

	// --- step 10: the transports open LAST, and together -------------------
	//
	// Opening UDP before the sink was ready would drop datagrams with nobody
	// knowing, because UDP cannot refuse: the only defence is not to be
	// listening.
	recvCtx, stopRecv := context.WithCancel(context.Background())
	a.stopRecv = stopRecv
	defer stopRecv()

	if code := a.openTransports(ctx, recvCtx); code != ExitOK {
		a.closeTransports()
		return code
	}

	// --- step 11 --------------------------------------------------------------
	a.admin.SetBootState("")
	close(a.ready)
	a.reloadOpen.Store(true)

	select {
	case <-a.health.Fatal():
		a.reloadOpen.Store(false)
		a.log.Error("sink became permanently unusable", "error", a.health.FatalErr())
		return a.shutdown(stopWorkers, true, a.watchAbort())

	case <-ctx.Done():
		a.reloadOpen.Store(false)
		if a.quitting.Load() {
			a.log.Warn("SIGQUIT: exiting immediately and accepting the loss")
			a.abortLoss()
			return ExitAborted
		}
		// The abort watcher is registered BEFORE the signal context lets go
		// of SIGTERM/SIGINT: in between, a second signal would get Go's
		// default action and kill the process with 143.
		abort := a.watchAbort()
		stopSignals()
		a.log.Info("shutdown requested")
		return a.shutdown(stopWorkers, false, abort)
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
	return out
}

// The two axes of the parent go-syslog-server, fixed here (SPEC §2.1). They
// stay in the boot log and in syslog_config_info so a dashboard built for the
// parent reads this server too.
const (
	ringMode = "memory"
	sinkType = "postgres"
)

// clientName identifies this instance in pg_stat_activity (application_name):
// AgentName for the program, Hostname for the node (SPEC §13.1).
func (a *App) clientName() string { return "slpg/" + a.id.AgentName + "@" + a.id.Hostname }

// convertConfig is the frozen conversion configuration (SPEC §3.5).
func (a *App) convertConfig() convert.Config {
	loc, err := a.cfg.Syslog.Location()
	if err != nil {
		// Validation already loaded the zone; this cannot fail in practice.
		loc = time.UTC
	}
	cs, _ := convert.ParseCharset(a.cfg.Syslog.LegacyCharset)
	return convert.Config{
		DefaultPriority: a.cfg.Syslog.DefaultPriority,
		Location:        loc,
		IncludeRaw:      a.cfg.Syslog.PayloadRaw,
		Charset:         cs,
		MaxMessageBytes: int(a.cfg.Syslog.MaxMessageBytes),
		SDMaxElements:   a.cfg.Syslog.SDMaxElements,
		SDMaxParams:     a.cfg.Syslog.SDMaxParams,
	}
}

func (a *App) workerConfig() sink.WorkerConfig {
	return sink.WorkerConfig{
		Workers:         a.cfg.Sink.Workers,
		Batch:           a.cfg.Sink.Batch,
		BatchMaxBytes:   int(a.cfg.Sink.BatchMaxBytes),
		OpTimeout:       a.cfg.Sink.OpTimeout,
		BackoffMin:      a.cfg.Sink.RetryBackoffMin,
		BackoffMax:      a.cfg.Sink.RetryBackoffMax,
		Convert:         a.convertConfig(),
		AcceptMalformed: a.cfg.Syslog.AcceptMalformed,
	}
}

// buildSink constructs the destination. It is lazy: nothing connects here, and
// the WaitReady of step 7 owns the retry loop, so the only failures here are
// configuration and they exit 1.
func (a *App) buildSink() int {
	var err error
	a.sink, err = sink.NewPostgres(sink.PostgresConfig{
		Addr: a.cfg.SinkPG.Addr, Username: a.cfg.SinkPG.Username,
		Password: a.cfg.SinkPG.Password, Database: a.cfg.SinkPG.Database,
		Table: a.cfg.SinkPG.Table, PoolSize: a.cfg.SinkPG.PoolSize,
		ConnectTimeout: a.cfg.Sink.OpTimeout, AppName: a.clientName(),
	}, a.log.Logger, a.metrics)
	if err != nil {
		a.log.Error("cannot configure the sink", "error", err)
		return ExitConfig
	}
	a.log.Info("sink configured", "type", sinkType, "addr", a.cfg.SinkPG.Addr,
		"table", a.cfg.SinkPG.Table, "pool_size", a.cfg.SinkPG.PoolSize,
		"max_event_bytes", config.MaxEventBytes(a.cfg))
	return ExitOK
}

func (a *App) proxyConfig() (receiver.ProxyConfig, error) {
	mode, err := receiver.ParseProxyMode(a.cfg.Proxy.Protocol)
	if err != nil {
		return receiver.ProxyConfig{}, err
	}
	return receiver.ProxyConfig{Mode: mode, Trusted: a.cfg.Proxy.Trusted,
		HeaderTimeout: a.cfg.Proxy.HeaderTimeout}, nil
}

func streamConfig(addr string, listeners int, l config.ConnLimits, p receiver.ProxyConfig) receiver.StreamConfig {
	return receiver.StreamConfig{
		Addr: addr, Listeners: listeners,
		IdleTimeout: l.IdleTimeout, MaxConns: l.MaxConns, MaxConnsPerIP: l.MaxConnsPerIP,
		BackpressureTimeout: l.BackpressureTimeout, Proxy: p,
	}
}

// openTransports binds every configured transport, in the order unix, udp, tcp.
// Any failure closes what was already opened and ends the boot: a server
// that silently runs with one transport missing looks healthy on every probe
// while an emitter's messages go nowhere.
func (a *App) openTransports(bindCtx, recvCtx context.Context) int {
	proxy, err := a.proxyConfig()
	if err != nil {
		a.log.Error("proxy", "error", err)
		return ExitConfig
	}

	if a.cfg.Listen.Unix != "" {
		d, err := receiver.NewUnix(bindCtx, receiver.UnixConfig{
			Path: a.cfg.Listen.Unix, Mode: os.FileMode(a.cfg.Listen.UnixMode),
			ReadBuffer: int(a.cfg.Listen.UnixReadBuffer),
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the unix transport", "path", a.cfg.Listen.Unix, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"unix", d})
	}

	if a.cfg.Listen.UDP != "" {
		d, err := receiver.NewUDP(bindCtx, receiver.UDPConfig{
			Addr: a.cfg.Listen.UDP, Listeners: a.cfg.Listen.UDPListeners,
			ReadBuffer: int(a.cfg.Listen.UDPReadBuffer),
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the udp transport", "addr", a.cfg.Listen.UDP, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"udp", d})
	}

	if a.cfg.Listen.TCP != "" {
		framing, err := receiver.ParseFramingMode(a.cfg.TCP.Framing)
		if err != nil {
			a.log.Error("tcp.framing", "error", err)
			return ExitConfig
		}
		t, err := receiver.NewTCP(bindCtx, receiver.TCPConfig{
			StreamConfig: streamConfig(a.cfg.Listen.TCP, a.cfg.Listen.TCPListeners, a.cfg.TCP.ConnLimits, proxy),
			Framing:      framing,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			a.log.Error("cannot open the tcp transport", "addr", a.cfg.Listen.TCP, "error", err)
			return ExitConfig
		}
		a.transports = append(a.transports, namedTransport{"tcp", t})
	}

	for _, nt := range a.transports {
		nt.t.Start(recvCtx)
		// The configured number and the wire number differ inside a container
		// (1514 against 514); saying the bound one keeps an operator from
		// chasing the wrong port during an incident (SPEC §4.1).
		a.log.Info("transport open", "transport", nt.name, "addr", nt.t.Addr())
	}
	return ExitOK
}

// TransportAddr reports the bound address of a transport by name, for tests
// that bind to port 0.
func (a *App) TransportAddr(name string) string {
	for _, nt := range a.transports {
		if nt.name == name {
			return nt.t.Addr()
		}
	}
	return ""
}

// closeTransports closes in REVERSE open order: tcp first, unix last.
func (a *App) closeTransports() {
	for i := len(a.transports) - 1; i >= 0; i-- {
		nt := a.transports[i]
		if err := nt.t.Close(); err != nil {
			a.log.Warn("closing a transport", "transport", nt.name, "error", err)
		}
	}
}

// watchAbort is the channel shutdown watches for a second signal: the test's
// when set, otherwise SIGTERM, SIGINT and SIGQUIT from the OS. The OS
// registration is never stopped; the process ends when Main returns.
func (a *App) watchAbort() <-chan os.Signal {
	if a.abort != nil {
		return a.abort
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	return ch
}

// shutdown runs the five phases of SPEC §13.4. A second signal skips straight
// to the exit, accepting the loss.
//
// A FATAL shutdown starts with no signal at all, so the FIRST signal that
// arrives during it -- a docker stop while the dump is being written -- is not
// the second one of SPEC §13.4: it is logged and the dump goes on; only the
// next one aborts.
func (a *App) shutdown(stopWorkers context.CancelFunc, fatal bool, abort <-chan os.Signal) int {
	return a.shutdownWith(func() int { return a.gracefulShutdown(stopWorkers, fatal) }, fatal, abort)
}

// shutdownWith is shutdown around any graceful procedure, so the signal rules
// can be tested without a whole server.
func (a *App) shutdownWith(graceful func() int, fatal bool, abort <-chan os.Signal) int {
	done := make(chan int, 1)
	go func() { done <- graceful() }()

	tolerated := fatal
	for {
		select {
		case code := <-done:
			return code
		case sig := <-abort:
			if tolerated {
				tolerated = false
				a.log.Warn("signal during the FATAL shutdown; the emergency dump goes on, a "+
					"second signal aborts it and accepts the loss", "signal", sig.String())
				continue
			}
			a.log.Warn("second signal during shutdown; exiting immediately and accepting the loss",
				"signal", sig.String())
			a.abortLoss()
			return ExitAborted
		}
	}
}

// abortLoss accounts for what an abort leaves behind: everything still queued
// (reason="abort", SPEC §13.4). Once the shutdown collected the leftovers for
// the dump, the ring's own count no longer holds them -- Drain committed them
// -- so the dump's count is the one that is lost, until the dump is safe.
func (a *App) abortLoss() {
	if a.ring == nil {
		return
	}
	st := a.ring.Stats()
	if n := a.dumping.Load(); n > 0 {
		st.Items = n
	}
	if st.Items == 0 {
		return
	}
	a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(st.Items))
	a.log.Error("messages lost to the abort", "count", st.Items)
}

func (a *App) gracefulShutdown(stopWorkers context.CancelFunc, fatal bool) int {
	// Phase 1: /readyz starts failing so the load balancer drops this endpoint.
	a.admin.StartDraining()

	// Phase 2: keep receiving for pre_stop_delay. A UDP emitter gets no error
	// at all when the socket disappears; it simply throws the datagram away.
	if d := a.cfg.Shutdown.PreStopDelay; d > 0 && !fatal {
		a.log.Info("pre-stop delay: still receiving", "delay", d.String())
		time.Sleep(d)
	}

	// Phase 3: stop receiving. The receive context is cancelled FIRST: that is
	// what releases a reader parked in the backpressure loop. A message already
	// handed to the ring is not affected, so what was read finishes entering
	// the ring. Then the transports close in reverse order.
	a.log.Info("closing transports")
	if a.stopRecv != nil {
		a.stopRecv()
	}
	a.closeTransports()

	// Phase 4: empty the ring. The running workers ARE the drain, so they are
	// left alone to finish first; stopping them up front would abandon every
	// in-flight batch.
	code := ExitOK
	if !fatal {
		a.waitDrained(a.cfg.Shutdown.DrainTimeout)
	}
	// The sink may also have gone FATAL DURING the drain: the exit code is 3
	// whatever the ring (SPEC §13.4: "sai com 0 (3 se o sink estiver FATAL)").
	if fatal || a.health.State() == sink.StateFatal {
		code = ExitSink
	}

	stopWorkers()
	a.workers.Wait()

	// A worker retrying a failed batch holds items that are no longer in the
	// ring (SPEC §10.2). Without this collection they would be lost.
	leftover := a.workers.Pending()
	if len(leftover) > 0 {
		a.log.Warn("workers surrendered batches they were retrying", "count", len(leftover))
	}
	leftover = append(leftover, a.ring.Drain()...)

	if len(leftover) > 0 {
		a.log.Error("messages still pending after the drain", "count", len(leftover))
		a.dumping.Store(int64(len(leftover)))
		saved := a.dumpEmergency(leftover)
		// Saved, or lost and already counted below: either way an abort from
		// here on loses nothing more.
		a.dumping.Store(0)
		if !saved && a.emg == nil {
			// No emergency store (emergency.enabled=false): SPEC §12.3 counts
			// this exactly like a dump that failed. A dump that FAILED was
			// already counted by dumpEmergency, and the same messages are not
			// lost twice.
			a.log.Error("emergency dumps are disabled; these messages are lost", "count", len(leftover))
			a.metrics.EventsDroppedTotal[obs.EventDropEmergencyFailed].Add(uint64(len(leftover)))
		}
		code = ExitSink
	}

	// Phase 5: the sink and the ring close through the defers in run.
	a.log.Info("shutdown complete", "exit_code", code)
	return code
}

// waitDrained blocks until the ring is empty, the sink goes FATAL (nothing
// will drain any more; waiting out drain_timeout would only delay the dump), or
// the deadline passes.
func (a *App) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if a.ring.Stats().Items == 0 {
			return
		}
		if a.health.State() == sink.StateFatal {
			a.log.Error("the sink went FATAL during the drain", "error", a.health.FatalErr())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := a.ring.Stats(); st.Items > 0 {
		a.log.Warn("drain deadline reached with messages still queued",
			"items", st.Items, "timeout", timeout.String())
	}
}

// warnMemoryLimit is the GOMEMLIMIT check of SPEC §15.4. The configuration
// package already produced the warning text into Loaded.Warnings; this only
// adds the runtime's own reading, which is the one that matters.
func (a *App) warnMemoryLimit() {
	if limit := debug.SetMemoryLimit(-1); limit == 1<<63-1 {
		a.log.Debug("GOMEMLIMIT is not set; see the configuration warnings",
			"ring_max_bytes", a.cfg.Ring.MaxBytes)
	}
}
