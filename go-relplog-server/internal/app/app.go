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
	"math"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/control"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/filter"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ratelimit"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/receiver"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/sink"
)

// Build identity, set from cmd/ at link time.
var (
	version = "dev"
	commit  = "none"
)

// SetBuildInfo records the identity reported by --version and relplog_build_info.
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
// (cmd/go-relplog-server); the tests pass a function returning a fixed identity.
type IdentityInit func(dataDir string) (ident.Identity, error)

// Main loads the configuration from args and runs the server to completion,
// returning the process exit code.
func Main(args []string, initIdentity IdentityInit) int {
	// SIGHUP and SIGQUIT are caught for the whole life of the process, from
	// its first line. Go's default action ends the process for both -- SIGHUP
	// with exit 129 and no log line, SIGQUIT with a goroutine dump and exit 2
	// -- and neither is an exit code of SPEC §13.5. The boot alone can last
	// sink.boot_timeout plus the reingestion and the backlog drain, and the
	// compose files tell operators to send HUP. The channels are never
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
				fmt.Printf("go-relplog-server %s (%s, %s)\n", version, commit, runtime.Version())
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
	if err := config.CheckFiles(loaded.Config); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		return ExitConfig
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
	if err := config.CheckFiles(loaded.Config); err != nil {
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
	opts config.Options

	cfg    *config.Config
	loaded *config.Loaded // the BOOT snapshot; never replaced (see reload)

	// current is the last snapshot whose reloadable half a SIGHUP applied;
	// nil until the first one, meaning "the boot snapshot".
	current *config.Loaded
	log     *obs.Logger
	id      ident.Identity

	metrics *obs.Metrics
	limiter *ratelimit.Limiter
	filter  *filter.Filter
	sink    sink.Sink
	nats    *sink.NATS // non-nil only with sink=nats; the beacon publishes on it
	beacon  *control.Beacon
	ring    ring.Ring
	health  *sink.Health
	workers *sink.Workers
	direct  *sink.Instrumented // boot reingestion and backlog drain
	ingest  *receiver.Ingest
	budget  *receiver.Budget
	admin   *Admin
	emg     *emergency.Store

	// transports in OPEN order: tcp, tls. They close together (SPEC §13.4).
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

	// reloadOpen is true between the end of the boot (step 13) and the start
	// of the shutdown: the only window in which a SIGHUP may swap anything.
	// Outside it the signal is logged and ignored, never fatal.
	reloadOpen atomic.Bool

	// quitting records that the context was cancelled by SIGQUIT, which
	// aborts at once instead of shutting down gracefully (SPEC §13.4).
	quitting atomic.Bool

	// quitNow is closed together with quitting. shutdownWith reads quitting
	// once, at entry, and then waits on quitNow too: a SIGQUIT whose watcher
	// stores the flag just after that read is still "now" (independent
	// review of go-relplog-nats-server, 2026-10-13). nil (never closed) when
	// SIGQUIT is not watched.
	quitNow chan struct{}

	// lossMu orders the abort against the moments the shutdown hands the
	// loss counter around (enterDumpPhase, oweDump, settleDump): one side
	// counts each message, never both (SPEC §13.4). aborted is set by
	// abortLoss under it, and is what tells an emergency dump still being
	// written to give up (emergency.Store.SetAbandon): a dump counted lost
	// must not come back at the next boot.
	lossMu  sync.Mutex
	aborted atomic.Bool

	// dumping is the number of leftovers handed to the emergency dump while
	// it is being written, for abortLoss; 0 otherwise. Drain takes the items
	// out of the ring's accounting BEFORE the dump, so an abort that lands
	// while the file is written would read the ring as (nearly) empty and
	// count a fraction of a loss that is in fact the whole dump: os.Exit cuts
	// the write short (SPEC §13.4).
	dumping atomic.Int64
	// dumpPhase is set when the shutdown starts collecting the leftovers of a
	// ring that does not outlive the process: from then on abortLoss counts
	// dumping, never the ring's count (see there).
	dumpPhase atomic.Bool

	// beforeDump, when set, runs right before the emergency dump is written.
	// It is nil in the binary; the tests use it to hold the shutdown inside
	// the dump while they send a signal. beforeCollect, likewise, runs right
	// after the dump phase started and before the leftovers are collected
	// (Workers.Pending, Drain): the tests land an abort there. inAbort runs
	// inside abortLoss in the dump phase, right after the abort is published
	// and before it takes the dump's count: the tests land the dump's own
	// count there (settleDump).
	beforeDump    func()
	beforeCollect func()
	inAbort       func()

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
		a.quitNow = make(chan struct{})
		go func() {
			select {
			case <-a.quit:
				a.quitting.Store(true)
				close(a.quitNow)
				cancel()
			case <-lifeCtx.Done():
			}
		}()
	}

	a.log.Info("starting",
		"version", version, "commit", commit, "go", runtime.Version(),
		"from", a.id.From, "space", a.id.Space, "agent_name", a.id.AgentName,
		"hostname", a.id.Hostname, "data_dir", a.cfg.Server.DataDir,
		"ring_mode", a.cfg.Ring.Mode, "sink_type", a.cfg.Sink.Type,
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
		RingMode: a.cfg.Ring.Mode, SinkType: a.cfg.Sink.Type,
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
			if ctx.Err() != nil {
				// The signal context, not the address: a host name's lookup
				// cancelled by a SIGTERM is an interrupted boot, 130, not a
				// bad admin.bind (independent review of
				// go-relplog-nats-server, 2026-10-13; SPEC §13.5).
				a.log.Info("interrupted while opening the admin listener")
				return ExitAborted
			}
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
	if a.cfg.Ring.Mode == config.RingModeMemory && a.cfg.Emergency.Enabled {
		a.emg = emergency.NewStore(a.cfg.Emergency.Dir, a.id.From)
		a.emg.SetAbandon(a.aborted.Load)
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

	// The beacon hook is registered BEFORE the sink connects, so the beacon of
	// the very first connection is not lost (SPEC §11).
	if a.nats != nil {
		a.beacon = control.New(a.nats, control.Config{
			Identity:   a.id,
			Interval:   a.cfg.LogHub.BeaconInterval,
			Namespace:  a.cfg.LogHub.CtlNamespace,
			Version:    version,
			RingMode:   a.cfg.Ring.Mode,
			Transports: a.enabledTransports(),
			Started:    a.started,
		}, a.log.Logger, a.metrics)
		a.nats.OnConnect(a.beacon.Kick)
	} else {
		a.log.Info("no heartbeat beacon: it is only published with sink=nats", "sink_type", a.cfg.Sink.Type)
	}

	a.admin.SetBootState("waiting for the sink")
	if err := a.health.WaitReady(ctx, a.cfg.Sink.BootTimeout,
		100*time.Millisecond, 5*time.Second); err != nil {
		if ctx.Err() != nil {
			a.log.Info("interrupted while waiting for the sink")
			return ExitAborted
		}
		if sink.IsFatal(err) {
			// WaitReady gives up at once on a refusal that waiting will not
			// change (credentials, a missing table or stream, the largest
			// event above the destination's limit). Saying "within
			// sink.boot_timeout" there would send the operator after the
			// network and a timeout that never ran; the ring Redis has its own
			// line for the same reason.
			a.log.Error("the sink refused this server permanently", "error", err)
		} else {
			a.log.Error("sink did not become usable within sink.boot_timeout",
				"timeout", a.cfg.Sink.BootTimeout.String(), "error", err)
		}
		return ExitSink
	}

	// --- step 8: the ring, and everything that depends on it --------------
	a.direct = sink.NewInstrumented(a.sink, a.health, a.workerConfig(), a.log.Logger, a.metrics)
	if code := a.buildRing(ctx); code != ExitOK {
		return code
	}
	defer a.ring.Close()
	a.admin.Attach(a.ring, a.health)

	if code := a.buildIngest(); code != ExitOK {
		return code
	}

	// --- step 9: reingest emergency dumps from a previous run -------------
	if code := a.reingest(ctx); code != ExitOK {
		return code
	}

	// --- step 10: drain the local backlog with NO transport open ----------
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

	// --- step 11: the beacon ------------------------------------------------
	if a.beacon != nil {
		a.beacon.Start(workerCtx)
		defer a.beacon.Stop()
	}

	// --- step 12: the transports open LAST, and together -------------------
	//
	// Opening RELP before the sink was ready would only hold every
	// transaction until backpressure_timeout and bounce the emitters; not
	// listening lets omrelp keep the messages in its queue and retry.
	recvCtx, stopRecv := context.WithCancel(context.Background())
	a.stopRecv = stopRecv
	defer stopRecv()

	if code := a.openTransports(ctx, recvCtx); code != ExitOK {
		a.closeTransports()
		return code
	}

	// --- step 13 --------------------------------------------------------------
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
	if a.cfg.Listen.TCP != "" {
		out = append(out, "tcp")
	}
	if a.cfg.Listen.TLS != "" {
		out = append(out, "tls")
	}
	return out
}

// clientName identifies this instance to every destination: AgentName for the
// program, Hostname for the node (the old node.id is gone, SPEC §13.1).
func (a *App) clientName() string { return "rls/" + a.id.AgentName + "@" + a.id.Hostname }

// relpConfig is the [relp] section for both transports, with the ONE
// in-flight budget of the process (SPEC §5.6).
func (a *App) relpConfig() receiver.RELPConfig {
	if a.budget == nil {
		a.budget = receiver.NewBudget(a.cfg.RELP.InflightMaxBytes, a.metrics)
	}
	return receiver.RELPConfig{
		Window:              a.cfg.RELP.Window,
		MaxFrameBytes:       a.cfg.RELP.MaxFrameBytes,
		BackpressureTimeout: a.cfg.RELP.BackpressureTimeout,
		Budget:              a.budget,
		Software:            "go-relplog-server," + version,
	}
}

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
		Columns:         sink.NeedsColumns(a.cfg.Sink.Type),
		XTransport:      XTransport,
	}
}

// natsMaxPending is the PublishAsyncMaxPending of the NATS sink: every publish
// this process can have in flight at once -- the drain workers' batches plus
// the write-through receivers of ring=none -- doubled: the futures of a
// tainted chunk keep counting until their own async timeout, which starts at
// the publish, a little after the worker's deadline, and a quick retry would
// stall on a full window and fail with ErrTooManyStalledMsgs
// (go-relplog-nats-server, independent review, 2026-10-13; SPEC §10.7).
// Computed in int64 and capped: config bounds every term (SPEC §13.2), and the
// cap keeps the sum an int on every platform, where an overflow became a
// negative number NewNATS silently replaced with 4096 (go-syslog-server
// review, 2026-10-13).
func natsMaxPending(c *config.Config) int {
	n := 2 * (int64(c.Sink.Workers)*int64(c.Sink.Batch) + int64(c.Ring.MaxInflight))
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	return int(n)
}

// XTransport is the payload extension every envelope of this server carries
// (decision 2, SPEC §3.2): the skill's transport enum has no relp, so
// transport says tcp or tls and "x-transport" says RELP.
const XTransport = "relp"

func (a *App) workerConfig() sink.WorkerConfig {
	wc := sink.WorkerConfig{
		Workers:         a.cfg.Sink.Workers,
		Batch:           a.cfg.Sink.Batch,
		BatchMaxBytes:   int(a.cfg.Sink.BatchMaxBytes),
		OpTimeout:       a.cfg.Sink.OpTimeout,
		BackoffMin:      a.cfg.Sink.RetryBackoffMin,
		BackoffMax:      a.cfg.Sink.RetryBackoffMax,
		Convert:         a.convertConfig(),
		AcceptMalformed: a.cfg.Syslog.AcceptMalformed,
	}
	if a.cfg.Ring.Mode == config.RingModeRedis {
		// A worker holding a batch in retry renews its claim well inside the
		// idle window, so another replica's XAUTOCLAIM does not take a batch
		// that is alive (SPEC §9.5).
		wc.TouchInterval = a.cfg.RingRedis.ClaimIdle / 3
	}
	return wc
}

// buildSink constructs the destination. Every destination is lazy: nothing
// connects here, and the WaitReady of step 7 owns the retry loop, so the only
// failures here are configuration and they exit 1.
func (a *App) buildSink() int {
	maxEvent := config.MaxEventBytes(a.cfg)
	var err error
	switch a.cfg.Sink.Type {
	case config.SinkTypeNone:
		a.sink = sink.NewNone()

	case config.SinkTypeSQLite:
		a.sink, err = sink.NewSQLite(sink.SQLiteConfig{
			Path:              a.cfg.SinkSQL.Path,
			Rotate:            a.cfg.SinkSQL.Rotate,
			KeepFiles:         a.cfg.SinkSQL.KeepFiles,
			Synchronous:       a.cfg.SinkSQL.Synchronous,
			BusyTimeout:       a.cfg.SinkSQL.BusyTimeout,
			WALAutocheckpoint: a.cfg.SinkSQL.WALAutocheckpoint,
		}, a.log.Logger, a.metrics)
		if err != nil {
			// A database that cannot be opened is a destination failure, not
			// a configuration one (SPEC §10.4).
			a.log.Error("cannot open the events database", "path", a.cfg.SinkSQL.Path, "error", err)
			return ExitSink
		}

	case config.SinkTypePostgres:
		a.sink, err = sink.NewPostgres(sink.PostgresConfig{
			Addr: a.cfg.SinkPG.Addr, Username: a.cfg.SinkPG.Username,
			Password: a.cfg.SinkPG.Password, Database: a.cfg.SinkPG.Database,
			Table: a.cfg.SinkPG.Table, PoolSize: a.cfg.SinkPG.PoolSize,
			TLS: a.cfg.SinkPG.TLS, TLSCA: a.cfg.SinkPG.TLSCA, TLSInsecure: a.cfg.SinkPG.TLSInsecure,
			ConnectTimeout: a.cfg.Sink.OpTimeout, AppName: a.clientName(),
		}, a.log.Logger, a.metrics)

	case config.SinkTypeMySQL:
		a.sink, err = sink.NewMySQL(sink.MySQLConfig{
			Addr: a.cfg.SinkMySQL.Addr, Username: a.cfg.SinkMySQL.Username,
			Password: a.cfg.SinkMySQL.Password, Database: a.cfg.SinkMySQL.Database,
			Table: a.cfg.SinkMySQL.Table, PoolSize: a.cfg.SinkMySQL.PoolSize,
			TLS: a.cfg.SinkMySQL.TLS, TLSCA: a.cfg.SinkMySQL.TLSCA, TLSInsecure: a.cfg.SinkMySQL.TLSInsecure,
			ConnectTimeout: a.cfg.Sink.OpTimeout, AppName: a.clientName(),
			MaxMessageBytes: a.cfg.Syslog.MaxMessageBytes, MaxEventBytes: maxEvent,
		}, a.log.Logger, a.metrics)

	case config.SinkTypeClickHouse:
		// Lazy like postgres: clickhouse.Open does not dial, WaitReady
		// retries, and the only failures here are configuration (SPEC §10.10).
		a.sink, err = sink.NewClickHouse(sink.ClickHouseConfig{
			Addrs: a.cfg.SinkCH.Addrs, Username: a.cfg.SinkCH.Username,
			Password: a.cfg.SinkCH.Password, Database: a.cfg.SinkCH.Database,
			Table: a.cfg.SinkCH.Table, PoolSize: a.cfg.SinkCH.PoolSize,
			TLS: a.cfg.SinkCH.TLS, TLSCA: a.cfg.SinkCH.TLSCA, TLSInsecure: a.cfg.SinkCH.TLSInsecure,
			ConnectTimeout: a.cfg.Sink.OpTimeout, AppName: a.clientName(),
			MaxBatchBytes: config.WorstChunkBytes(a.cfg),
			// The memory check counts the INSERTs this instance can have in
			// flight, not the pool (go-syslog-server review, 2026-10-13).
			Writers: a.cfg.SinkWriters(),
		}, a.log.Logger, a.metrics)

	case config.SinkTypeNATS:
		var n *sink.NATS
		n, err = sink.NewNATS(sink.NATSConfig{
			URLs: a.cfg.SinkNATS.URLs, Username: a.cfg.SinkNATS.Username,
			Password: a.cfg.SinkNATS.Password, Token: a.cfg.SinkNATS.Token,
			Credentials: a.cfg.SinkNATS.Credentials,
			TLS:         a.cfg.SinkNATS.TLS, TLSCA: a.cfg.SinkNATS.TLSCA, TLSInsecure: a.cfg.SinkNATS.TLSInsecure,
			Mode: a.cfg.SinkNATS.Mode, Stream: a.cfg.SinkNATS.Stream, Subjects: a.cfg.SinkNATS.Subjects,
			Workspace: a.id.Space, CtlNamespace: a.cfg.LogHub.CtlNamespace,
			ClientName: a.clientName(), ConnectTimeout: a.cfg.Sink.OpTimeout, OpTimeout: a.cfg.Sink.OpTimeout,
			MaxPending:    natsMaxPending(a.cfg),
			MaxEventBytes: maxEvent,
		}, a.log.Logger, a.metrics)
		if err == nil {
			a.sink, a.nats = n, n
		}

	case config.SinkTypeRedis:
		a.sink, err = sink.NewRedis(sink.RedisConfig{
			Config:    a.cfg.SinkRedis.ClientConfig(a.clientName()),
			Stream:    a.cfg.SinkRedis.Stream,
			MaxLen:    a.cfg.SinkRedis.MaxLen,
			Workspace: a.id.Space,
		}, a.log.Logger, a.metrics)

	default:
		a.log.Error("unknown sink type", "type", a.cfg.Sink.Type)
		return ExitConfig
	}
	if err != nil {
		a.log.Error("cannot configure the sink", "type", a.cfg.Sink.Type, "error", err)
		return ExitConfig
	}
	a.log.Info("sink configured", "type", a.cfg.Sink.Type, "max_event_bytes", maxEvent)
	return ExitOK
}

func (a *App) buildRing(ctx context.Context) int {
	switch a.cfg.Ring.Mode {
	case config.RingModeNone:
		// ring=none writes through from the receiver and never touches the
		// drain workers, so it gets the instrumented writer: the conversion,
		// the sink metrics and the fatal escalation (SPEC §9.6). With sink=none
		// there is nothing that could be slow, and the semaphore is dispensed
		// with -- a decision that belongs here, because the ring cannot see the
		// sink's type (invariant §20.15).
		inflight := a.cfg.Ring.MaxInflight
		if a.cfg.Sink.Type == config.SinkTypeNone {
			inflight = 0
		}
		a.ring = ring.NewNone(a.direct, inflight, a.cfg.Sink.OpTimeout)

	case config.RingModeMemory:
		a.ring = ring.NewMemory(a.cfg.Ring.CapacityItems, a.cfg.Ring.MaxBytes, a.cfg.Sink.BatchMaxDelay)

	case config.RingModeSQLite:
		r, err := ring.NewSQLite(ring.SQLiteConfig{
			Path:              a.cfg.RingSQL.Path,
			Synchronous:       a.cfg.RingSQL.Synchronous,
			CommitBatch:       a.cfg.RingSQL.CommitBatch,
			CommitMaxDelay:    a.cfg.RingSQL.CommitMaxDelay,
			BusyTimeout:       a.cfg.RingSQL.BusyTimeout,
			WALAutocheckpoint: a.cfg.RingSQL.WALAutocheckpoint,
			CapacityItems:     a.cfg.Ring.CapacityItems,
			CapacityBytes:     a.cfg.Ring.MaxBytes,
			// sink=none would discard a previous life's rows -- events a life
			// with a destination answered 200: they stay in ring.db, outside
			// this run's accounting (SPEC §9.4; go-syslog-server review,
			// 2026-10-13).
			KeepBacklog: a.cfg.Sink.Type == config.SinkTypeNone,
			Metrics:     a.metrics,
		}, a.log.Logger)
		if err != nil {
			a.log.Error("cannot open the ring database", "path", a.cfg.RingSQL.Path, "error", err)
			return ExitConfig
		}
		a.ring = r
		a.log.Info("ring ready", "mode", "sqlite", "path", a.cfg.RingSQL.Path,
			"synchronous", a.cfg.RingSQL.Synchronous, "backlog", r.Backlog())

	case config.RingModeRedis:
		if code := a.buildRedisRing(ctx); code != ExitOK {
			return code
		}

	default:
		a.log.Error("unknown ring mode", "mode", a.cfg.Ring.Mode)
		return ExitConfig
	}

	a.workers = sink.NewWorkers(a.ring, a.sink, a.health, a.workerConfig(), a.log.Logger, a.metrics)
	return ExitOK
}

// buildRedisRing connects the shared ring (SPEC §9.5). A refusal that will not
// change by waiting (credentials, a key of the wrong type, OOM) is fatal at
// once; anything else is retried with backoff inside sink.boot_timeout, the
// same budget the sink gets, and then exits 3.
func (a *App) buildRedisRing(ctx context.Context) int {
	deadline := time.Now().Add(a.cfg.Sink.BootTimeout)
	backoff := 100 * time.Millisecond
	a.admin.SetBootState("connecting to the ring redis")
	for attempt := 1; ; attempt++ {
		r, err := ring.NewRedis(ctx, ring.RedisConfig{
			Config:     a.cfg.RingRedis.ClientConfig(a.clientName() + "/ring"),
			Stream:     a.cfg.RingRedis.Stream,
			Group:      a.cfg.RingRedis.Group,
			ClaimIdle:  a.cfg.RingRedis.ClaimIdle,
			CapItems:   a.cfg.Ring.CapacityItems,
			CapBytes:   a.cfg.Ring.MaxBytes,
			Consumer:   a.id.From,
			OpTimeout:  a.cfg.Sink.OpTimeout,
			ClaimCount: a.cfg.Sink.Batch,
			Metrics:    a.metrics,
		}, a.log.Logger)
		if err == nil {
			a.ring = r
			a.log.Info("ring ready", "mode", "redis", "addrs", a.cfg.RingRedis.Addrs,
				"stream", a.cfg.RingRedis.Stream, "group", a.cfg.RingRedis.Group, "consumer", a.id.From)
			return ExitOK
		}
		if errors.Is(err, ring.ErrFatal) {
			a.log.Error("the ring redis refused this server permanently", "error", err)
			return ExitSink
		}
		if ctx.Err() != nil {
			return ExitAborted
		}
		if time.Now().After(deadline) {
			a.log.Error("the ring redis did not become reachable within sink.boot_timeout",
				"addrs", a.cfg.RingRedis.Addrs, "error", err)
			return ExitSink
		}
		a.log.Warn("ring redis not reachable yet, retrying",
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

func (a *App) buildIngest() int {
	a.limiter = ratelimit.New(rateLimitParamsFrom(a.cfg), 0)

	rule, err := filter.Compile(a.cfg.Filter.MinSeverity, a.cfg.Filter.Facilities)
	if err != nil {
		a.log.Error("filter", "error", err)
		return ExitConfig
	}
	a.filter = filter.New(rule)
	a.log.Info("filter", "min_severity", convert.SeverityNames[rule.MinSeverity()],
		"facilities", a.cfg.Filter.Facilities)

	a.ingest = receiver.NewIngest(a.ring, a.health, a.filter, a.limiter, a.metrics, a.log,
		receiver.IngestConfig{
			Identity:        a.id,
			DefaultPriority: a.cfg.Syslog.DefaultPriority,
			MaxMessageBytes: int(a.cfg.Syslog.MaxMessageBytes),
			// ring=none: a logic refusal was counted as lost by the
			// instrumented writer and is final for that message (SPEC §9.6,
			// §10.2). Retrying it would hold a TCP reader for the whole
			// backpressure timeout and count the same loss on every attempt.
			Final: func(err error) bool { return sink.ClassOf(err) == sink.ClassLogic },
		})
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

func streamConfig(addr string, listeners int, l config.ConnLimits, p receiver.ProxyConfig,
	r receiver.RELPConfig) receiver.StreamConfig {
	return receiver.StreamConfig{
		Addr: addr, Listeners: listeners,
		IdleTimeout: l.IdleTimeout, MaxConns: l.MaxConns, MaxConnsPerIP: l.MaxConnsPerIP,
		Proxy: p, RELP: r,
	}
}

// openTransports binds every configured transport, in the order tcp, tls.
// Any failure closes what was already opened and ends the boot: a server that
// silently runs with one transport missing looks healthy on every probe while
// an emitter's messages go nowhere.
func (a *App) openTransports(bindCtx, recvCtx context.Context) int {
	proxy, err := a.proxyConfig()
	if err != nil {
		a.log.Error("proxy", "error", err)
		return ExitConfig
	}

	// bindFailed is the exit of a transport that could not be opened. A
	// signal that cancelled the bind (a host name's lookup fails with
	// "operation was canceled") is an interrupted boot, 130, not a bad
	// address (independent review of go-relplog-nats-server, 2026-10-13).
	bindFailed := func(transport, addr string, err error) int {
		if bindCtx.Err() != nil {
			a.log.Info("interrupted while opening the transports")
			return ExitAborted
		}
		a.log.Error("cannot open the "+transport+" transport", "addr", addr, "error", err)
		return ExitConfig
	}

	relp := a.relpConfig()
	if a.cfg.Listen.TCP != "" {
		t, err := receiver.NewTCP(bindCtx, receiver.TCPConfig{
			StreamConfig: streamConfig(a.cfg.Listen.TCP, a.cfg.Listen.TCPListeners, a.cfg.TCP.ConnLimits, proxy, relp),
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			return bindFailed("tcp", a.cfg.Listen.TCP, err)
		}
		a.transports = append(a.transports, namedTransport{"tcp", t})
	}

	if a.cfg.Listen.TLS != "" {
		auth, err := receiver.ParseClientAuth(a.cfg.TLS.ClientAuth)
		if err != nil {
			a.log.Error("tls.client_auth", "error", err)
			return ExitConfig
		}
		minVer, err := receiver.ParseTLSMinVersion(a.cfg.TLS.MinVersion)
		if err != nil {
			a.log.Error("tls.min_version", "error", err)
			return ExitConfig
		}
		t, err := receiver.NewTLS(bindCtx, receiver.TLSConfig{
			StreamConfig:     streamConfig(a.cfg.Listen.TLS, a.cfg.Listen.TLSListeners, a.cfg.TLS.ConnLimits, proxy, relp),
			CertFile:         a.cfg.TLS.CertFile,
			KeyFile:          a.cfg.TLS.KeyFile,
			ClientAuth:       auth,
			ClientCAFile:     a.cfg.TLS.ClientCA,
			MinVersion:       minVer,
			HandshakeTimeout: a.cfg.TLS.HandshakeTimeout,
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			return bindFailed("tls", a.cfg.Listen.TLS, err)
		}
		a.transports = append(a.transports, namedTransport{"tls", t})
	}

	// A signal that landed while the binds ran but did not fail them -- a
	// literal IP looks nothing up -- is still an interrupted boot: checked
	// here, before a single connection is accepted. Without it the server
	// opened, /readyz said 200 and the shutdown that followed exited 0
	// instead of 130 (independent review of go-syslog-server, 2026-10-13, A1;
	// SPEC §13.1, §13.5).
	if bindCtx.Err() != nil {
		a.log.Info("interrupted while opening the transports")
		return ExitAborted
	}

	for _, nt := range a.transports {
		nt.t.Start(recvCtx)
		// The configured number and the wire number differ inside a container
		// (20514 against a published 20514+n); saying the bound one keeps an operator from
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

// closeTransports closes every transport (SPEC §13.4, phase 3).
//
// The two are closed AT ONCE: each Close stops reading, lets its sessions
// flush their decided answers and gives the stuck ones closeGrace. One after
// the other, the tcp sessions kept reading -- and accepting -- for as long as
// the tls ones took, and phase 3 could last two closeGraces instead of the one
// that stop_grace_period is sized for (independent review, 2026-10-11; SPEC
// §5.8).
func (a *App) closeTransports() {
	var wg sync.WaitGroup
	for i := len(a.transports) - 1; i >= 0; i-- {
		nt := a.transports[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := nt.t.Close(); err != nil {
				a.log.Warn("closing a transport", "transport", nt.name, "error", err)
			}
		}()
	}
	wg.Wait()
}

// persistentRing reports whether the ring keeps its content outside this
// process: leftovers there are not lost at shutdown, and nothing is dumped.
func (a *App) persistentRing() bool {
	return a.cfg.Ring.Mode == config.RingModeSQLite || a.cfg.Ring.Mode == config.RingModeRedis
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
// A FATAL shutdown starts with no signal at all, so the FIRST SIGTERM or
// SIGINT that arrives during it -- a docker stop, a deploy, while the workers
// are being collected or the dump is being written -- is not the second
// signal of SPEC §13.4. Treating it as one turned the exit 3 with a dump of
// SPEC §13.3 into an exit 130 that lost the ring. It is logged and the
// shutdown goes on; the next one aborts. SIGQUIT is never tolerated: it means
// "now", in every phase.
func (a *App) shutdown(stopWorkers context.CancelFunc, fatal bool, abort <-chan os.Signal) int {
	return a.shutdownWith(func() int { return a.gracefulShutdown(stopWorkers, fatal) }, fatal, abort)
}

// shutdownWith is shutdown around any graceful procedure, so the signal rules
// can be tested without a whole server.
func (a *App) shutdownWith(graceful func() int, fatal bool, abort <-chan os.Signal) int {
	// A SIGQUIT that arrived after the shutdown branch was taken and before
	// abort was registered reached only the SIGQUIT watcher of run, which set
	// quitting: it still means "now" (independent review, 2026-10-11). The
	// loop below waits on quitNow for a watcher that stores the flag just
	// after this read (independent review of go-relplog-nats-server,
	// 2026-10-13).
	if a.quitting.Load() {
		a.log.Warn("SIGQUIT: exiting immediately and accepting the loss")
		a.abortLoss()
		return ExitAborted
	}
	done := make(chan int, 1)
	go func() { done <- graceful() }()

	tolerate := fatal
	for {
		select {
		case code := <-done:
			return code
		case <-a.quitNow:
			a.log.Warn("SIGQUIT during shutdown; exiting immediately and accepting the loss")
			a.abortLoss()
			return ExitAborted
		case sig := <-abort:
			if tolerate && sig != syscall.SIGQUIT {
				tolerate = false
				a.log.Warn("signal during the FATAL shutdown; it goes on to the emergency dump and "+
					"exit 3, a second signal aborts it and accepts the loss", "signal", sig.String())
				continue
			}
			a.log.Warn("second signal during shutdown; exiting immediately and accepting the loss",
				"signal", sig.String())
			a.abortLoss()
			return ExitAborted
		}
	}
}

// abortLoss accounts for what an abort leaves behind: nothing with a ring that
// keeps its content outside the process, everything still queued otherwise
// (reason="abort", SPEC §13.4).
//
// Once the shutdown has collected the leftovers for the dump, the ring's own
// count no longer holds them -- Drain committed them -- so while the dump is
// being written its size is what is lost: the abort's os.Exit cuts the write
// short, and the file that may remain is incomplete.
//
// From the moment the shutdown starts collecting them (dumpPhase), the
// ring's count is no longer the measure: the held batches are still counted
// there although they are going to the dump, and the drained ones no longer
// are. The counter is the only measure then, and it is taken with Swap so
// that exactly one side -- this abort, or dumpEmergency counting a failed or
// disabled dump -- counts the same messages. Under lossMu, and only once:
// aborted, set here, closes the dump phase (enterDumpPhase, oweDump,
// settleDump) and makes a dump still being written give up
// (emergency.Store.SetAbandon).
func (a *App) abortLoss() {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	if a.aborted.Swap(true) {
		return // counted already: one abort, one count
	}
	if a.dumpPhase.Load() {
		if a.inAbort != nil {
			a.inAbort()
		}
		if n := a.dumping.Swap(0); n > 0 {
			a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(n))
			a.log.Error("messages lost to the abort: the emergency dump was being written", "count", n)
		}
		return
	}
	if a.ring == nil {
		return
	}
	st := a.ring.Stats()
	if st.Items == 0 {
		return
	}
	if a.persistentRing() {
		if a.cfg.Ring.Mode == config.RingModeRedis {
			// The stream is shared: its occupancy is the fleet's, not what
			// this instance leaves (go-syslog-server review, 2026-10-13).
			a.log.Warn("messages left in the shared ring stream for this or another instance",
				"stream_count", st.Items)
			return
		}
		a.log.Warn("messages left in the ring for the next boot", "count", st.Items)
		return
	}
	a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(st.Items))
	a.log.Error("messages lost to the abort", "count", st.Items)
}

// enterDumpPhase hands the loss counter from the ring's own count to dumping:
// from here an abort counts dumping, not the ring (SPEC §13.4). Under lossMu,
// so an abort lands wholly before (it counted the ring; false is returned) or
// wholly after (it takes dumping). Done separately, an abort between the two
// stores counted the ring and left dumping for a failed dump to count again
// (independent review of go-relplog-nats-server, 2026-10-13).
func (a *App) enterDumpPhase() bool {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	if a.aborted.Load() {
		return false
	}
	a.dumping.Store(a.ring.Stats().Items)
	a.dumpPhase.Store(true)
	return true
}

// oweDump sets the counter to exactly what goes to the dump, unless an abort
// already took it: then the messages were counted, the process is exiting,
// and putting the number back would let a failed or disabled dump count them
// a second time (independent review of go-relplog-nats-server, 2026-10-13:
// 10 counted lost for 5).
func (a *App) oweDump(n int) bool {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	if a.aborted.Load() {
		return false
	}
	a.dumping.Store(int64(n))
	return true
}

// settleDump takes the dump's count back once its outcome is known, under
// lossMu like every other hand-over of the counter. abortLoss publishes aborted
// -- which a dump being written reads to give up (Store.SetAbandon) -- before
// it takes dumping; outside the lock the dump could see the abort, give up and
// take the count first: emergency_failed=n and abort=0, the loss counted once
// but blamed on the disk (independent review of go-syslog-server, 2026-10-13,
// A2). Under it, the abort either finished (0 here: it counted) or has not
// started (the dump counts, the abort finds 0).
func (a *App) settleDump() int64 {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	return a.dumping.Swap(0)
}

func (a *App) gracefulShutdown(stopWorkers context.CancelFunc, fatal bool) int {
	// Phase 1: /readyz starts failing so the load balancer drops this endpoint.
	a.admin.StartDraining()

	// Phase 2: keep receiving for pre_stop_delay, so the load balancer drops
	// this endpoint before omrelp sees its session break and reconnects.
	if d := a.cfg.Shutdown.PreStopDelay; d > 0 && !fatal {
		a.log.Info("pre-stop delay: still receiving", "delay", d.String())
		time.Sleep(d)
	}

	// Phase 3: stop receiving. The receive context is cancelled FIRST: that is
	// what releases a reader parked in the backpressure loop. A message already
	// handed to the ring is not affected -- Enqueue awaits its group commit or
	// its synchronous write regardless (SPEC §9.4, §9.6) -- so what was read
	// finishes entering the ring; a transaction not yet handed over is left
	// unanswered, and librelp retransmits it (SPEC §5.7, §5.8). Then the
	// transports close, all at once (closeTransports).
	a.log.Info("closing transports")
	if a.stopRecv != nil {
		a.stopRecv()
	}
	a.closeTransports()

	// Phase 4: empty the ring. The running workers ARE the drain, so they are
	// left alone to finish first; stopping them up front would abandon every
	// in-flight batch. With ring=redis the occupancy is GLOBAL -- other
	// replicas keep feeding it -- so there is nothing to wait for: what this
	// instance read and did not confirm stays pending in the stream and is
	// claimed back (SPEC §9.5, §13.4).
	code := ExitOK
	if !fatal {
		if a.cfg.Ring.Mode == config.RingModeRedis {
			a.waitHeld(a.cfg.Shutdown.DrainTimeout)
		} else {
			a.waitDrained(a.cfg.Shutdown.DrainTimeout)
		}
	}
	// The sink may also have gone FATAL DURING the drain: the exit code is 3
	// whatever the ring (SPEC §13.4: "sai com 0 (3 se o sink estiver FATAL)").
	if fatal || a.health.State() == sink.StateFatal {
		code = ExitSink
	}

	stopWorkers()
	a.workers.Wait()

	if !a.persistentRing() {
		// From here an abort loses whatever the ring still counts -- queued
		// items and the batches the workers hold, which are the same
		// population (SPEC §9.2) -- until the dump is safe. Set BEFORE the
		// drain: in between, the drained items would be counted by nobody.
		if !a.enterDumpPhase() {
			return ExitAborted // the abort counted the ring; the process is exiting
		}
	}
	if a.beforeCollect != nil {
		a.beforeCollect()
	}

	// A worker retrying a failed batch holds items that are no longer in the
	// ring (SPEC §10.2). Without this collection they would be lost.
	leftover := a.workers.Pending()
	if len(leftover) > 0 {
		a.log.Warn("workers surrendered batches they were retrying", "count", len(leftover))
	}
	leftover = append(leftover, a.ring.Drain()...)

	if a.persistentRing() {
		// The rows (sqlite) or entries (redis) were never deleted: the next
		// boot, or another replica, delivers them. Nothing is lost, so nothing
		// is counted as dropped and nothing is dumped -- a dump would deliver
		// everything twice.
		if n := int64(len(leftover)); n > 0 || (a.cfg.Ring.Mode == config.RingModeSQLite && a.ring.Stats().Items > 0) {
			a.log.Warn("messages left in the ring for the next boot",
				"held_by_workers", n, "ring_items", a.ring.Stats().Items)
		}
	} else if len(leftover) > 0 {
		a.log.Error("messages still pending after the drain", "count", len(leftover))
		// From here until the dump is safe on disk (or counted lost), an abort
		// loses exactly leftover (see abortLoss); dumpEmergency takes the
		// counter back, and counts a disabled or failed dump itself.
		if !a.oweDump(len(leftover)) {
			return ExitAborted
		}
		a.dumpEmergency(leftover)
		code = ExitSink
	} else if !a.oweDump(0) {
		return ExitAborted
	}

	// Phase 5: the beacon, the sink and the ring close through the defers in run.
	a.log.Info("shutdown complete", "exit_code", code)
	return code
}

// waitHeld is phase 4 for ring=redis. The stream is shared and its occupancy is
// global, so "empty" never comes; what this instance owes is the batches its
// workers already hold. The ring stops handing out new entries (Quiesce), and
// the workers finish what they have, so an in-flight write is not cut into a
// duplicate and no fresh entry is pulled into this consumer's pending list on
// the way out. Whatever is still unconfirmed at the deadline stays pending in
// the stream and is claimed back (SPEC §9.5, §13.4).
func (a *App) waitHeld(timeout time.Duration) {
	if q, ok := a.ring.(interface{ Quiesce() }); ok {
		q.Quiesce()
	}
	deadline := time.Now().Add(timeout)
	idle := 0
	for time.Now().Before(deadline) {
		if a.health.State() == sink.StateFatal {
			return // nothing held will be delivered any more
		}
		// Two consecutive empty readings: a Dequeue that returned just before
		// Quiesce may not have registered its batch yet.
		if a.workers.Held() == 0 {
			if idle++; idle >= 2 {
				return
			}
		} else {
			idle = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	a.log.Warn("drain deadline reached with batches still held; they stay pending in the ring stream",
		"held", a.workers.Held(), "timeout", timeout.String())
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

func rateLimitParamsFrom(c *config.Config) ratelimit.Params {
	return ratelimit.Params{
		Enabled:       c.RateLimit.Enabled,
		Rate:          c.RateLimit.Rate,
		Burst:         c.RateLimit.Burst,
		V4Mask:        c.RateLimit.V4Mask,
		V6Mask:        c.RateLimit.V6Mask,
		Exempt:        c.RateLimit.Exempt,
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

// warnMemoryLimit is the GOMEMLIMIT check of SPEC §15.4. The configuration
// package already produced the warning text into Loaded.Warnings; this only
// adds the runtime's own reading, which is the one that matters.
func (a *App) warnMemoryLimit() {
	if limit := debug.SetMemoryLimit(-1); limit == 1<<63-1 {
		a.log.Debug("GOMEMLIMIT is not set; see the configuration warnings", "ring_mode", a.cfg.Ring.Mode)
	}
}
