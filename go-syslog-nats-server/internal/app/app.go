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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/control"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/receiver"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/sink"
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
// (cmd/go-syslog-nats-server); the tests pass a function returning a fixed identity.
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
				fmt.Printf("go-syslog-nats-server %s (%s, %s)\n", version, commit, runtime.Version())
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
	nats    *sink.NATS // the real sink under any test wrapper; the beacon publishes on it
	beacon  *control.Beacon
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

	// reloadOpen is true between the end of the boot (step 12) and the start
	// of the shutdown: the only window in which a SIGHUP may swap anything.
	// Outside it the signal is logged and ignored, never fatal.
	reloadOpen atomic.Bool

	// quitting records that the context was cancelled by SIGQUIT, which
	// aborts at once instead of shutting down gracefully (SPEC §13.4).
	quitting atomic.Bool

	// quitNow is closed together with quitting. shutdownWith reads quitting
	// once, at entry, and then waits on quitNow too: a SIGQUIT whose watcher
	// stores the flag just after that read is still "now" (go-relplog-pg-server's
	// independent review, 2026-10-13). nil (never closed) when SIGQUIT is
	// not watched.
	quitNow chan struct{}

	// lossMu orders the abort against the moments the shutdown hands the
	// loss counter around (enterDumpPhase, oweDump): one side counts each
	// message, never both (SPEC §13.4). aborted is set by abortLoss under it,
	// and is what tells an emergency dump still being written to give up
	// (emergency.Store.SetAbandon): a dump counted lost must not come back at
	// the next boot.
	lossMu  sync.Mutex
	aborted atomic.Bool

	// dumping is the number of leftovers handed to the emergency dump while
	// it is being written, for abortLoss; 0 otherwise. Drain takes the items
	// out of the ring's accounting BEFORE the dump, so an abort that lands
	// while the file is written would read the ring as (nearly) empty and
	// count a fraction of a loss that is in fact the whole dump: os.Exit cuts
	// the write short (SPEC §13.4).
	dumping atomic.Int64
	// dumpPhase is set when the shutdown starts collecting the leftovers of
	// the ring, which does not outlive the process: from then on abortLoss
	// counts dumping, never the ring's count (see there).
	dumpPhase atomic.Bool

	// beforeDump, when set, runs right before the emergency dump is written.
	// It is nil in the binary; the tests use it to hold the shutdown inside
	// the dump while they send a signal. beforeCollect, likewise, runs right
	// after the dump phase started and before the leftovers are collected
	// (Workers.Pending, Drain): the tests land an abort there.
	beforeDump    func()
	beforeCollect func()

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
		"ring_mode", ringMode, "sink_type", sinkType, "nats_mode", a.cfg.SinkNATS.Mode,
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
			if ctx.Err() != nil {
				// The signal context, not the address: see openTransports.
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
	if a.cfg.Emergency.Enabled {
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
	a.beacon = control.New(a.nats, control.Config{
		Identity:   a.id,
		Interval:   a.cfg.LogHub.BeaconInterval,
		Namespace:  a.cfg.LogHub.CtlNamespace,
		Version:    version,
		RingMode:   ringMode,
		Transports: a.enabledTransports(),
		Started:    a.started,
	}, a.log.Logger, a.metrics)
	a.nats.OnConnect(a.beacon.Kick)

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

	// --- step 10: the beacon ------------------------------------------------
	a.beacon.Start(workerCtx)
	defer a.beacon.Stop()

	// --- step 11: the transports open LAST, and together -------------------
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

	// --- step 12 --------------------------------------------------------------
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
	sinkType = "nats"
)

// clientName identifies this instance in the NATS server's connz: AgentName
// for the program, Hostname for the node (SPEC §13.1).
func (a *App) clientName() string { return "slnats/" + a.id.AgentName + "@" + a.id.Hostname }

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

// maxPending is the client's PublishAsyncMaxPending: every publish this
// process can have in flight at once -- the drain workers' batches
// (reingestion runs before the workers start) -- doubled: the futures of a
// tainted chunk keep counting until their own async timeout, which starts at
// the publish, a little after the worker's deadline, and a quick retry would
// stall on a full window and fail with ErrTooManyStalledMsgs
// (go-relplog-nats-server's independent review, 2026-10-13; SPEC §10.7). The
// ceilings of sink.workers and sink.batch keep it an int (validate.go).
func maxPending(c *config.Config) int { return 2 * c.Sink.Workers * c.Sink.Batch }

// buildSink constructs the destination. It is lazy: nothing connects here, and
// the WaitReady of step 7 owns the retry loop, so the only failures here are
// configuration and they exit 1.
func (a *App) buildSink() int {
	maxEvent := config.MaxEventBytes(a.cfg)
	n, err := sink.NewNATS(sink.NATSConfig{
		URLs: a.cfg.SinkNATS.URLs, Username: a.cfg.SinkNATS.Username,
		Password: a.cfg.SinkNATS.Password, Token: a.cfg.SinkNATS.Token,
		Credentials: a.cfg.SinkNATS.Credentials,
		Mode:        a.cfg.SinkNATS.Mode, Stream: a.cfg.SinkNATS.Stream,
		Workspace: a.id.Space, CtlNamespace: a.cfg.LogHub.CtlNamespace,
		ClientName: a.clientName(), ConnectTimeout: a.cfg.Sink.OpTimeout, OpTimeout: a.cfg.Sink.OpTimeout,
		MaxPending:    maxPending(a.cfg),
		MaxEventBytes: maxEvent,
	}, a.log.Logger, a.metrics)
	if err != nil {
		a.log.Error("cannot configure the sink", "error", err)
		return ExitConfig
	}
	a.sink, a.nats = n, n
	a.log.Info("sink configured", "type", sinkType, "mode", a.cfg.SinkNATS.Mode,
		"stream", a.cfg.SinkNATS.Stream, "max_event_bytes", maxEvent)
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
			return a.bindFailed(bindCtx, "unix", "path", a.cfg.Listen.Unix, err)
		}
		a.transports = append(a.transports, namedTransport{"unix", d})
	}

	if a.cfg.Listen.UDP != "" {
		d, err := receiver.NewUDP(bindCtx, receiver.UDPConfig{
			Addr: a.cfg.Listen.UDP, Listeners: a.cfg.Listen.UDPListeners,
			ReadBuffer: int(a.cfg.Listen.UDPReadBuffer),
		}, a.ingest, a.metrics, a.log)
		if err != nil {
			return a.bindFailed(bindCtx, "udp", "addr", a.cfg.Listen.UDP, err)
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
			return a.bindFailed(bindCtx, "tcp", "addr", a.cfg.Listen.TCP, err)
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

// bindFailed answers a transport that could not open. A signal that cancelled
// the bind -- a host name's lookup fails with "operation was canceled" -- is an
// interrupted boot, exit 130, not a bad address (go-relplog-pg-server's
// independent review, 2026-10-13; SPEC §13.1).
func (a *App) bindFailed(bindCtx context.Context, name, key, addr string, err error) int {
	if bindCtx.Err() != nil {
		a.log.Info("interrupted while opening the transports")
		return ExitAborted
	}
	a.log.Error("cannot open the "+name+" transport", key, addr, "error", err)
	return ExitConfig
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
	// quitting: it still means "now" (go-relplog-server's independent review).
	// The loop below waits on quitNow for a watcher that stores the flag just
	// after this read (go-relplog-pg-server's independent review, 2026-10-13).
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

// abortLoss accounts for what an abort leaves behind: everything still queued
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
// disabled dump -- counts the same messages.
func (a *App) abortLoss() {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	if a.aborted.Swap(true) {
		return // counted already: one abort, one count
	}
	if a.dumpPhase.Load() {
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
	a.metrics.EventsDroppedTotal[obs.EventDropAbort].Add(uint64(st.Items))
	a.log.Error("messages lost to the abort", "count", st.Items)
}

// enterDumpPhase hands the loss counter from the ring's own count to dumping:
// from here an abort counts dumping, not the ring (SPEC §13.4). Under lossMu,
// so an abort lands wholly before (it counted the ring; false is returned) or
// wholly after (it takes dumping). Done separately, an abort between the two
// stores counted the ring and left dumping for a failed dump to count again.
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
// a second time (go-relplog-pg-server's independent review, 2026-10-13: 10
// counted lost for 5).
func (a *App) oweDump(n int) bool {
	a.lossMu.Lock()
	defer a.lossMu.Unlock()
	if a.aborted.Load() {
		return false
	}
	a.dumping.Store(int64(n))
	return true
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

	// From here an abort loses whatever the ring still counts -- queued items
	// and the batches the workers hold, which are the same population (SPEC
	// §9.2) -- until the dump is safe. Set BEFORE the drain: in between, the
	// drained items would be counted by nobody (go-syslog-server's independent
	// review, 2026-10-11, ported through go-relplog-server).
	if !a.enterDumpPhase() {
		return ExitAborted // the abort counted the ring; the process is exiting
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

	if len(leftover) > 0 {
		a.log.Error("messages still pending after the drain", "count", len(leftover))
		// From here until the dump is safe on disk (or counted lost), an abort
		// loses exactly leftover (see abortLoss); dumpEmergency takes the
		// counter back.
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
