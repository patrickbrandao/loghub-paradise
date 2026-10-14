// Package app wires the server together and owns its lifecycle.
//
// It lives here rather than in cmd/ so that the integration tests can build the
// same stack the binary builds. A test that assembles its own approximation of
// the wiring proves nothing about what ships.
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/control"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/server"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/sink"
)

// Build identity, set from cmd/ at link time.
var (
	version = "dev"
	commit  = "none"
)

// SetBuildInfo records the identity reported by --version and whs_build_info.
func SetBuildInfo(v, c string) { version, commit = v, c }

// Exit codes, per SPEC §10.5.
const (
	ExitOK      = 0
	ExitConfig  = 1
	ExitSink    = 3
	ExitAborted = 130
)

// IdentityInit resolves the process identity (SPEC §3.5). In the binary it
// exports DATADIR and calls go-loghub-ident's Initialize, which may run only
// once per process (cmd/go-webhook-nats-server); the tests pass a function returning
// a fixed identity.
type IdentityInit func(dataDir string) (ident.Identity, error)

// Main loads the configuration from args and runs the server to completion,
// returning the process exit code.
func Main(args []string, initIdentity IdentityInit) int {
	// Before anything else, even the configuration and the identity: Go's
	// default action for SIGHUP ends the process, and there is no reload to
	// give it (SPEC §13.3). run replaces this with a handler that warns; once
	// run returns the signal is ignored again until os.Exit.
	signal.Ignore(syscall.SIGHUP)

	loaded, err := config.Load(config.Options{Args: args})
	if err != nil {
		if err == config.ErrExitEarly {
			if loaded.Meta.Help {
				_ = config.Usage(os.Stdout)
			} else {
				fmt.Printf("go-webhook-nats-server %s (%s, %s)\n", version, commit, runtime.Version())
			}
			return ExitOK
		}
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		return ExitConfig
	}

	if loaded.Meta.DumpConfig {
		// Before the identity, on purpose: the dump must not touch the disk
		// (SPEC §10.1 step 1).
		_ = loaded.DumpINI(os.Stdout)
		return ExitOK
	}

	// SPEC §10.1 step 1a: the one writable root exists, absolute, before the
	// identity library may need to persist a generated agent_uuid into it --
	// go-loghub-ident refuses a relative DATADIR with exit 100.
	dataDir, err := filepath.Abs(loaded.Config.Ring.DataDir)
	if err == nil {
		err = os.MkdirAll(dataDir, 0o750)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot create ring.data_dir %s: %v\n", loaded.Config.Ring.DataDir, err)
		return ExitConfig
	}
	id, err := initIdentity(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "identity error: %v\n", err)
		return ExitConfig
	}

	cfg := loaded.Config
	log := obs.NewLogger(os.Stdout, cfg.Log.Format, cfg.Log.Level)
	log.SetAccess(cfg.Log.Access, cfg.Log.AccessSample)

	a := &App{cfg: cfg, loaded: loaded, log: log, args: args, id: id}

	// Signals are wired here, at the process boundary, and turned into a
	// context. Everything below works from the context alone, which is what
	// lets the integration tests run the very same lifecycle in-process
	// without signalling the test binary itself.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	return a.run(ctx, stop)
}

// RunContext runs the server until ctx is cancelled, which triggers the same
// graceful shutdown a SIGTERM would. It exists for tests; the binary uses Main.
func RunContext(ctx context.Context, args []string, id ident.Identity) int {
	return runContext(ctx, args, id, nil)
}

// runContext is what RunContext and the integration tests share. hook, when
// set, is handed the assembled App before it starts, which is the only way a
// test can reach the abort channel: these instances run INSIDE the test binary,
// so delivering a real second signal would be delivered to the test process
// itself and race with Go's own handlers.
func runContext(ctx context.Context, args []string, id ident.Identity, hook func(*App)) int {
	loaded, err := config.Load(config.Options{Args: args})
	if err != nil {
		return ExitConfig
	}
	if err := id.Validate(); err != nil {
		return ExitConfig
	}
	log := obs.NewLogger(os.Stdout, loaded.Config.Log.Format, loaded.Config.Log.Level)
	log.SetAccess(loaded.Config.Log.Access, loaded.Config.Log.AccessSample)

	a := &App{cfg: loaded.Config, loaded: loaded, log: log, args: args, id: id}
	if hook != nil {
		hook(a)
	}
	return a.run(ctx, func() {})
}

// App is the assembled server.
type App struct {
	args []string
	id   ident.Identity // frozen at boot (SPEC §3.5)

	cfg    *config.Config
	loaded *config.Loaded // read once, at boot: there is no reload (SPEC §13.3)
	log    *obs.Logger

	metrics *obs.Metrics
	sink    sink.Sink
	nats    *sink.NATS // the real sink, nil under sinkOverride; the beacon publishes on it
	beacon  *control.Beacon
	ring    ring.Ring
	health  *sink.Health
	workers *sink.Workers
	srv     *server.Server
	admin   *server.Admin

	// emg reads the dumps of a previous life at every boot, emergency.enabled
	// or not; dumps says whether this life writes them (SPEC §9.2). The store
	// used to exist only with the key on, and a next life with it off left a
	// dump stranded with no log (SPEC §19.1 item 8 of go-webhook-pg-server,
	// §18.1 item 4 of go-webhook-nats-server; the rule of
	// go-webhook-fast-server, ported 2026-10-13).
	emg   *emergency.Store
	dumps bool

	// abort replaces the OS signal channel that shutdown watches for a second
	// interrupt. It is nil in the binary, which registers the real signals; the
	// integration tests set it instead, for the reason given on runContext.
	abort chan os.Signal

	// sinkOverride replaces the NATS sink. It is nil in the binary; the
	// integration tests that do not need a server set a fake one, defined in
	// the test files, so the whole lifecycle -- boot, drain, DOWN, FATAL, dump,
	// reingestion -- runs without NATS (SPEC §15.3). There is no beacon then:
	// it needs the NATS connection.
	sinkOverride sink.Sink

	// wrapRing, when a test sets it, wraps the memory ring before anything
	// uses it, so the test can act inside the shutdown's last Drain: the one
	// instant a late request must not be able to slip into the ring (SPEC
	// §10.3). A hook called after the Drain could not tell a Seal before the
	// Drain from one between the two (the review of this port). nil in the
	// binary.
	wrapRing func(*ring.Memory) ring.Ring

	// lossMu orders the abort against the moments the shutdown hands the
	// loss counter around (enterDumpPhase, oweDump, settleDump): one side
	// counts each event, never both (SPEC §10.3). aborted is set by abortLoss
	// under it, and is what tells an emergency dump still being written to
	// give up (emergency.Store.SetAbandon): a dump counted lost must not come
	// back at the next boot. The form is the final one of the syslog/RELP
	// family (go-relplog-nats-server's independent review, 2026-10-13),
	// ported 2026-10-13; it replaces the atRisk/collected pair, which left
	// two windows -- an abort between the Drain and the collected flag read
	// the ring without the drained events, and one during the dump counted
	// events that the link could still publish.
	lossMu  sync.Mutex
	aborted atomic.Bool

	// dumping is the number of accepted events the shutdown is collecting or
	// dumping, for abortLoss; 0 otherwise. Drain takes the items out of the
	// ring's accounting BEFORE the dump, so an abort that lands while the
	// file is written would read the ring as (nearly) empty and count a
	// fraction of a loss that is in fact the whole dump.
	dumping atomic.Int64
	// dumpPhase is set when the shutdown starts collecting the leftovers of
	// the ring: from then on abortLoss counts dumping, never the ring's count.
	dumpPhase atomic.Bool

	// beforeDump, when a test sets it, runs right before the emergency dump
	// is written; beforeCollect, right after the dump phase started and
	// before the leftovers are collected (Workers.Pending, Drain); inAbort,
	// inside abortLoss in the dump phase, right after aborted is set and
	// before the counter is taken, with lossMu held. The tests land an abort,
	// or the dump's own count (settleDump), there. nil in the binary.
	beforeDump    func()
	beforeCollect func()
	inAbort       func()

	// wrapServe, when a test sets it, wraps the public Serve, so the test can
	// make it return an error the retrying listener never lets through
	// (SPEC §10.3). nil in the binary.
	wrapServe func(serve func() error) func() error
}

func (a *App) run(ctx context.Context, stopSignals func()) int {
	// First thing, before anything can take long: Go's default action for
	// SIGHUP is to end the process, and a kill -HUP from a procedure written
	// for go-webhook-fast-server -- which reloaded on it -- would take this
	// instance down without a drain or a dump (SPEC §13.3).
	defer ignoreSIGHUP(a.log)()

	a.log.Info("starting",
		"version", version, "commit", commit, "go", runtime.Version(),
		"agent_uuid", a.id.From, "workspace", a.id.Space, "agent_name", a.id.AgentName,
		"hostname", a.id.Hostname, "stage", stage,
		"ring_mode", config.RingMemory, "sink_type", config.SinkNATSType,
		"config_file", a.loaded.File)

	for _, w := range a.loaded.Warnings {
		a.log.Warn(w)
	}
	a.warnMemoryLimit()
	a.log.Debug("effective configuration", "config", a.loaded.Effective())

	a.metrics = obs.NewMetrics(version, commit, runtime.Version(),
		config.RingMemory, config.SinkNATSType)

	// SPEC §10.1 step 2: the admin port opens FIRST -- before the sink is even
	// built -- and stays open through the whole startup, so a crash loop caused
	// by an unreachable sink is still diagnosable through /readyz and /metrics.
	a.admin = server.NewAdmin(a.metrics, a.cfg.Ring.HighWater)
	if a.cfg.Admin.Enabled {
		if err := a.admin.Listen(ctx, a.cfg.Admin.Bind); err != nil {
			if ctx.Err() != nil {
				// The signal context, not the address: a host name's lookup
				// fails with "operation was canceled". An interrupted boot is
				// 130, not the exit 1 of a bad address (SPEC §10.5; port of
				// the syslog/RELP family, 2026-10-13).
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
		// Closed on every exit path, so a boot that fails after this point
		// does not leave the listener behind. In the binary os.Exit makes
		// that moot; in the tests many boots share one process.
		defer a.admin.Shutdown(context.Background())
		a.log.Info("admin listener open", "addr", a.admin.Addr())
	}

	// Step 3: the sink. The connection is lazy; WaitReady below is where a
	// destination that is not up yet is waited for, within sink.boot_timeout.
	a.admin.SetBootState("connecting to the sink")
	if code := a.buildSink(); code != ExitOK {
		return code
	}
	defer a.sink.Close()

	// The beacon hook is registered BEFORE the sink connects, so the beacon of
	// the first connection is asked for and kept until the beacon starts
	// (SPEC §8.7.10).
	if a.nats != nil {
		a.beacon = control.New(a.nats, control.Config{
			Identity:   a.id,
			Interval:   a.cfg.LogHub.BeaconInterval,
			Namespace:  a.cfg.LogHub.CtlNamespace,
			Version:    version,
			RingMode:   config.RingMemory,
			SinkMode:   a.cfg.SinkNATS.Mode,
			Transports: a.transports(),
		}, a.log.Logger, a.metrics)
		a.nats.OnConnect(a.beacon.Kick)
	}

	a.health = sink.NewHealth(a.sink, sink.HealthConfig{
		Interval:   a.cfg.Sink.HealthInterval,
		Timeout:    a.cfg.Sink.HealthTimeout,
		FailAfter:  a.cfg.Sink.FailTimeout,
		MaxRetries: a.cfg.Sink.MaxRetries,
	}, a.log.Logger, a.metrics)

	// The public port stays closed until the destination answers, so nothing
	// is ever accepted that cannot be stored.
	a.admin.SetBootState("waiting for the sink")
	if err := a.health.WaitReady(ctx, a.cfg.Sink.BootTimeout,
		100*time.Millisecond, 5*time.Second); err != nil {
		if ctx.Err() != nil {
			a.log.Info("interrupted while waiting for the sink")
			return ExitAborted
		}
		if sink.IsFatal(err) {
			// Refused at once (SPEC §8.7.5): saying "within boot_timeout" here
			// would send the operator to the network when the stream, the
			// credentials or the TLS of the server are what is wrong.
			a.log.Error("the sink refused this server permanently; not waiting for sink.boot_timeout",
				"error", err)
			return ExitSink
		}
		a.log.Error("sink did not become reachable within sink.boot_timeout",
			"timeout", a.cfg.Sink.BootTimeout.String(), "error", err)
		return ExitSink
	}

	// The ring and everything that depends on it.
	if code := a.build(); code != ExitOK {
		return code
	}
	defer a.ring.Close()
	a.admin.Attach(a.ring, a.health)

	// Step 4: reingest emergency files from a previous run. Step 5 of the
	// fast -- the ring.db backlog -- does not exist: the memory ring starts
	// empty (SPEC §10.1).
	if code := a.reingest(ctx); code != ExitOK {
		return code
	}

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	go a.health.Run(workerCtx)

	a.workers.Start(workerCtx)

	// Step 4a: the beacon. The one of the first connection, asked for during
	// WaitReady, goes out now. Deferred after the sink's Close, so it stops
	// first: phase 5 of SPEC §10.3.
	if a.beacon != nil {
		a.beacon.Start(workerCtx)
		defer a.beacon.Stop()
	}

	// Step 6: the public listener opens last.
	if err := a.srv.Listen(ctx); err != nil {
		if ctx.Err() != nil {
			// See the admin listener: a signal, not a bad address.
			a.log.Info("interrupted while opening the public listener")
			return ExitAborted
		}
		a.log.Error("cannot bind the public listener", "addr", a.cfg.Server.Bind, "error", err)
		return ExitConfig
	}
	// A literal IP binds without ever looking at ctx, so a signal between the
	// reingestion and here used to reach this point with the bind done: the
	// server opened, answered /readyz 200 and left with 0 through the graceful
	// path instead of 130 (SPEC §10.5; port of the syslog/RELP family,
	// 2026-10-13). Nothing was accepted yet: the listeners just close.
	if ctx.Err() != nil {
		_ = a.srv.Close()
		a.log.Info("interrupted while opening the public listener")
		return ExitAborted
	}
	a.log.Info("public listener open",
		"addr", a.srv.Addr(), "listeners", a.cfg.Server.Listeners,
		"h2c", a.cfg.Server.HTTP2)
	// /readyz answers 200 only now, with the public port actually open
	// (SPEC §10.1 step 6).
	a.admin.SetBootState("")

	serve := a.srv.Serve
	if a.wrapServe != nil {
		serve = a.wrapServe(serve)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- serve() }()

	select {
	case err := <-serveErr:
		if err == nil {
			return ExitOK
		}
		// The listener retries every accept error but its own close, so
		// this is rare; when it happens, the ring and the batches the
		// workers hold were already answered 200. Exiting here dropped them
		// with no dump and no count: they go through the shutdown like a
		// SIGTERM's, and a complete one still exits 1, because the process
		// did fail (SPEC §10.3, §10.5).
		a.log.Error("public listener stopped; shutting down", "error", err)
		abort, stopAbort := a.watchAbort()
		defer stopAbort()
		// No signal started this shutdown, so -- as in a FATAL one -- the
		// first SIGTERM/SIGINT, the orchestrator's answer to /readyz
		// failing, is tolerated; the drain stays a SIGTERM's (W 31).
		stopSignals()
		if code := a.shutdown(stopWorkers, false, true, abort); code != ExitOK {
			return code
		}
		return ExitConfig

	case <-a.health.Fatal():
		a.log.Error("sink became permanently unusable", "error", a.health.FatalErr())
		abort, stopAbort := a.watchAbort()
		defer stopAbort()
		return a.shutdown(stopWorkers, true, true, abort)

	case <-ctx.Done():
		// The abort watcher is registered BEFORE the signal context lets go
		// of SIGTERM/SIGINT: in between, a second signal got Go's default
		// action and killed the process with 143, the loss counted nowhere
		// (port of the syslog/RELP family, 2026-10-13).
		abort, stopAbort := a.watchAbort()
		defer stopAbort()
		stopSignals() // a second signal now reaches abort, not the context
		a.log.Info("shutdown requested")
		return a.shutdown(stopWorkers, false, false, abort)
	}
}

// watchAbort is the channel shutdown watches for a second signal: the test's
// when set, otherwise SIGTERM, SIGINT and SIGQUIT from the OS, until stop.
func (a *App) watchAbort() (abort <-chan os.Signal, stop func()) {
	if a.abort != nil {
		return a.abort, func() {}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	return ch, func() { signal.Stop(ch) }
}

// buildSink constructs the destination. Nothing connects here: the
// connection is lazy and WaitReady owns the boot retry loop, so the only
// failures are configuration -- a credentials file that does not exist, a URL
// that is not nats:// -- and they exit 1 (SPEC §8.7.1).
func (a *App) buildSink() int {
	if a.sinkOverride != nil {
		a.sink = a.sinkOverride
		return ExitOK
	}
	s, err := sink.NewNATS(a.natsConfig(), a.log.Logger, a.metrics)
	if err != nil {
		a.log.Error("cannot configure the NATS sink", "error", err)
		return ExitConfig
	}
	a.sink, a.nats = s, s
	attrs := []any{"type", config.SinkNATSType, "mode", a.cfg.SinkNATS.Mode,
		"urls", a.loaded.Effective()["sink.nats.urls"], "subject", "loghub." + a.id.Space + ".webhook"}
	if a.cfg.SinkNATS.Mode == config.NATSModeJetStream {
		attrs = append(attrs, "stream", a.cfg.SinkNATS.Stream)
	}
	a.log.Info("sink ready", attrs...)
	return ExitOK
}

// natsConfig maps [sink.nats], [loghub] and the [sink] deadlines onto the sink.
func (a *App) natsConfig() sink.NATSConfig {
	return sink.NATSConfig{
		URLs:           a.cfg.SinkNATS.URLs,
		Username:       a.cfg.SinkNATS.Username,
		Password:       a.cfg.SinkNATS.Password,
		Token:          a.cfg.SinkNATS.Token,
		Credentials:    a.cfg.SinkNATS.Credentials,
		Mode:           a.cfg.SinkNATS.Mode,
		Stream:         a.cfg.SinkNATS.Stream,
		Workspace:      a.id.Space,
		CtlNamespace:   a.cfg.LogHub.CtlNamespace,
		ClientName:     a.clientName(),
		ConnectTimeout: a.cfg.Sink.OpTimeout,
		OpTimeout:      a.cfg.Sink.OpTimeout,
		MaxPending:     maxPending(a.cfg),
		MaxEventBytes:  config.MaxEventBytes(a.cfg),
	}
}

// maxPending is the client's PublishAsyncMaxPending (SPEC §8.7.2): every
// publish this process can have in flight at once -- the drain workers'
// batches (the boot reingestion runs before they start) -- doubled: the
// futures of a tainted batch keep counting until their own async timeout,
// which starts at the publish, a little after the worker's deadline, and a
// quick retry would stall on a full window and fail with
// ErrTooManyStalledMsgs (go-relplog-nats-server's independent review,
// 2026-10-13; porte da família syslog/RELP). The ceilings of sink.workers and
// sink.batch keep it an int (config/validate.go).
func maxPending(c *config.Config) int { return 2 * c.Sink.Workers * c.Sink.Batch }

// clientName names this instance's connection to the destination (SPEC §3.5,
// §8.7.1): the NATS client name, visible in the server's connz.
func (a *App) clientName() string {
	return "whs/" + a.id.AgentName + "@" + a.id.Hostname
}

// transports are the protocols the public port serves, for the beacon's meta
// (SPEC §8.7.10).
func (a *App) transports() []string {
	if a.cfg.Server.HTTP2 {
		return []string{"http1", "h2c"}
	}
	return []string{"http1"}
}

// stage is the "stage" of the 200 (SPEC §4.4): the envelope is in the
// memory ring when the client is answered, always.
const stage = "ring"

// build wires the ring, the workers and the HTTP surface, in that order: the
// ring needs the health machine to escalate to, and the server needs both. The
// sink and the health machine already exist by now (see run).
func (a *App) build() int {
	mem := ring.NewMemory(a.cfg.Ring.CapacityItems, a.cfg.Ring.MaxBytes,
		a.cfg.Sink.BatchMaxDelay)
	a.ring = mem
	if a.wrapRing != nil {
		a.ring = a.wrapRing(mem)
	}

	// Writing a dump belongs to the ring and to emergency.enabled; reading
	// one belongs to every boot: the dump is a previous life's (SPEC §9.2).
	// One line of a dump is at most the largest envelope this configuration
	// can produce. NewStore touches no disk.
	a.emg = emergency.NewStore(a.cfg.Emergency.Dir, a.id.From, config.MaxEventBytes(a.cfg))
	a.emg.SetAbandon(a.aborted.Load)
	a.dumps = a.cfg.Emergency.Enabled

	a.workers = sink.NewWorkers(a.ring, a.sink, a.health, sink.WorkerConfig{
		Workers:    a.cfg.Sink.Workers,
		Batch:      a.cfg.Sink.Batch,
		OpTimeout:  a.cfg.Sink.OpTimeout,
		BackoffMin: a.cfg.Sink.RetryBackoffMin,
		BackoffMax: a.cfg.Sink.RetryBackoffMax,
		// SPEC §8.1 step 2: a batch goes out in pieces under this ceiling.
		BatchMaxBytes: a.cfg.Sink.BatchMaxBytes,
	}, a.log.Logger, a.metrics)

	trusted, err := config.ParsePrefixes(a.cfg.Proxy.Trusted)
	if err != nil {
		a.log.Error("proxy.trusted", "error", err)
		return ExitConfig
	}

	a.srv = server.New(server.Config{
		Bind:              a.cfg.Server.Bind,
		Listeners:         a.cfg.Server.Listeners,
		ReadHeaderTimeout: a.cfg.Server.ReadHeaderTimeout,
		ReadTimeout:       a.cfg.Server.ReadTimeout,
		WriteTimeout:      a.cfg.Server.WriteTimeout,
		IdleTimeout:       a.cfg.Server.IdleTimeout,
		MaxBodyBytes:      a.cfg.Server.MaxBodyBytes,
		MaxHeaderBytes:    a.cfg.Server.MaxHeaderBytes,
		MaxURIBytes:       a.cfg.Server.MaxURIBytes,
		MaxConns:          a.cfg.Server.MaxConns,
		MaxConnsPerIP:     a.cfg.Server.MaxConnsPerIP,
		RejectDrainBytes:  a.cfg.Server.RejectDrainBytes,
		HTTP2:             a.cfg.Server.HTTP2,
		HTTP2MaxStreams:   a.cfg.Server.HTTP2MaxConcurrentStreams,
		From:              a.id.From,
		Space:             a.id.Space,
		SID:               uint64(os.Getpid()),
		Mode:              envelope.ModeFast,
		Stage:             stage,
	}, a.ring, a.health, a.metrics, a.log,
		trusted, a.cfg.Proxy.MaxXFFEntries,
		envelope.ExcludeSet(a.cfg.Headers.Exclude))

	return ExitOK
}

// shutdown runs the five phases of SPEC §10.3. A second signal skips straight
// to the exit, accepting the loss.
//
// A FATAL shutdown starts with no signal at all, so the FIRST SIGTERM or
// SIGINT that arrives during it -- the orchestrator's ordinary one, while the
// dump is being written -- is not the second signal of SPEC §10.3: it is
// logged and the shutdown goes on; the next one aborts (independent review,
// 2026-10-07). The same holds for the shutdown a public Serve error starts,
// which is otherwise a SIGTERM's: tolerateFirst is therefore its own
// parameter, not fatal (W 31). SIGQUIT is never tolerated: it means "now", in
// every phase.
func (a *App) shutdown(stopWorkers context.CancelFunc, fatal, tolerateFirst bool, abort <-chan os.Signal) int {
	return a.shutdownWith(func(abortCtx context.Context) int {
		return a.gracefulShutdown(abortCtx, stopWorkers, fatal)
	}, tolerateFirst, abort)
}

// shutdownWith is shutdown around any graceful procedure, so the signal rules
// can be tested without a whole server. The procedure is handed abortCtx,
// cancelled when the abort wins: in the binary os.Exit makes that moot, but in
// the tests the instance would otherwise keep draining and dumping into a
// directory the test has removed.
func (a *App) shutdownWith(graceful func(abortCtx context.Context) int, tolerateFirst bool, abort <-chan os.Signal) int {
	abortCtx, stopGraceful := context.WithCancel(context.Background())
	defer stopGraceful()

	done := make(chan int, 1)
	go func() { done <- graceful(abortCtx) }()

	tolerate := tolerateFirst
	for {
		select {
		case code := <-done:
			return code
		case sig := <-abort:
			if tolerate && sig != syscall.SIGQUIT {
				tolerate = false
				a.log.Warn("signal during a shutdown no signal started; it goes on -- a second one aborts "+
					"and accepts the loss", "signal", sig.String())
				continue
			}
			stopGraceful()
			a.log.Warn("second signal during shutdown; exiting immediately and accepting the loss",
				"signal", sig.String())
			if a.srv != nil {
				_ = a.srv.Close()
			}
			a.abortLoss()
			return ExitAborted
		}
	}
}

// abortLoss accounts for what an abort leaves behind: everything still queued
// (reason="abort", SPEC §10.3).
//
// From the moment the shutdown starts collecting the leftovers (dumpPhase),
// the ring's count is no longer the measure: the held batches are still
// counted there although they are going to the dump, and the drained ones no
// longer are. The counter is the only measure then, and it is taken with Swap
// so that exactly one side -- this abort, or dumpEmergency counting a failed
// or disabled dump -- counts the same events. Under lossMu, and only once:
// aborted, set here, closes the dump phase (enterDumpPhase, oweDump) and makes
// a dump still being written give up (emergency.Store.SetAbandon).
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
			a.metrics.EventsDroppedTotal[obs.DropAbort].Add(uint64(n))
			a.log.Error("events lost to the abort: the shutdown was collecting or dumping them",
				"count", n)
		}
		return
	}
	if a.ring == nil {
		return
	}
	if n := a.ring.Stats().Items; n > 0 {
		a.metrics.EventsDroppedTotal[obs.DropAbort].Add(uint64(n))
		a.log.Error("events lost to the abort", "count", n)
	}
}

// enterDumpPhase hands the loss counter from the ring's own count to dumping:
// from here an abort counts dumping, not the ring (SPEC §10.3). Under lossMu,
// so an abort lands wholly before (it counted the ring; false is returned) or
// wholly after (it takes dumping).
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
// already took it: then the events were counted, the process is exiting, and
// putting the number back would let a failed or disabled dump count them a
// second time (go-relplog-nats-server's independent review, 2026-10-13: 10
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

func (a *App) gracefulShutdown(abortCtx context.Context, stopWorkers context.CancelFunc, fatal bool) int {
	// Phase 1: /readyz starts failing so the load balancer drops this endpoint.
	a.admin.StartDraining()

	// Phase 2: keep serving for pre_stop_delay. In Kubernetes the endpoint
	// removal is asynchronous, so traffic still arrives after SIGTERM; without
	// this wait the sender gets a connection refusal, which is a network error
	// rather than a retryable 503.
	if d := a.cfg.Shutdown.PreStopDelay; d > 0 && !fatal {
		a.log.Info("pre-stop delay: still accepting traffic", "delay", d.String())
		time.Sleep(d)
	}

	// Phase 3: stop accepting; open connections finish and are told to close.
	//
	// One deadline for phases 3 and 4 together: drain_timeout bounds the time
	// from the last accepted connection to the dump. Each phase had its own,
	// so a slow upload and a DOWN sink took twice drain_timeout before the
	// dump started -- past the stop_grace_period sized for once, and the
	// SIGKILL took the whole ring, answered 200, with nothing counting it.
	a.log.Info("draining connections")
	deadline := time.Now().Add(a.cfg.Shutdown.DrainTimeout)
	ctx, cancel := context.WithDeadline(abortCtx, deadline)
	defer cancel()
	if err := a.srv.Shutdown(ctx); err != nil {
		a.log.Warn("connection drain did not finish cleanly", "error", err)
	}

	// Phase 4: empty the ring.
	//
	// The running workers ARE the drain, so they are left alone to finish
	// first. Stopping them up front would make every in-flight batch fail
	// against a cancelled context and be abandoned -- turning a clean shutdown
	// into an emergency dump of work that would have been delivered.
	//
	// Only once the ring is empty (or the deadline passes) are they stopped,
	// and only then is what they still hold collected. A worker retrying a
	// failed batch holds items that are no longer in the ring (SPEC §10.4);
	// without that collection they would be lost despite having been answered
	// 200.
	code := ExitOK
	if !fatal {
		a.waitDrained(abortCtx, time.Until(deadline))
	}
	// The sink may also have gone FATAL DURING the drain: the exit is 3 (SPEC
	// §10.2, §10.5; port of the syslog/RELP family, 2026-10-13).
	if fatal || a.health.State() == sink.StateFatal {
		code = ExitSink
	}
	if abortCtx.Err() != nil {
		return ExitAborted // the abort has already counted what was left
	}

	stopWorkers()
	a.workers.Wait()

	// From here an abort loses whatever the ring still counts -- queued items
	// and the batches the workers hold, one population there until a Commit
	// -- until the dump is safe. Set BEFORE the collection: the Drain takes
	// the items out of the ring's count, and in between nobody counted them
	// (SPEC §10.3).
	if !a.enterDumpPhase() {
		return ExitAborted // the abort counted the ring; the process is exiting
	}
	if a.beforeCollect != nil {
		a.beforeCollect()
	}

	leftover := a.workers.Pending()
	if len(leftover) > 0 {
		a.log.Warn("workers surrendered batches they were retrying", "count", len(leftover))
	}
	// Seal the ring before the last Drain (SPEC §10.3). An upload still
	// reading its body when phase 3 ran out of time goes on after it; without
	// the seal it could finish now, land in a ring nobody reads again and be
	// answered 200 -- in neither the stream nor the dump, counted nowhere.
	// Sealed, it gets a 503.
	if s, ok := a.ring.(interface{ Seal() }); ok {
		s.Seal()
	}
	leftover = append(leftover, a.ring.Drain()...)

	if len(leftover) > 0 {
		a.log.Error("events still pending after the drain", "count", len(leftover))
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

	// Phase 5.
	_ = a.admin.Shutdown(ctx)
	a.log.Info("shutdown complete", "exit_code", code)
	return code
}

// waitDrained blocks until the ring is empty, the sink goes FATAL (nothing
// will drain any more; waiting out drain_timeout would only delay the dump,
// perhaps past the orchestrator's SIGKILL), or the deadline passes.
//
// Polling is adequate here and nowhere else: this runs once, during shutdown,
// and the alternative -- a completion signal threaded through the Ring
// interface -- would add a channel to the hot path to serve a path taken once
// per process.
func (a *App) waitDrained(abortCtx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st := a.ring.Stats()
		if st.Items == 0 || abortCtx.Err() != nil {
			return
		}
		if a.health.State() == sink.StateFatal {
			a.log.Error("the sink went FATAL during the drain; nothing more will drain",
				"error", a.health.FatalErr())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := a.ring.Stats(); st.Items > 0 {
		a.log.Warn("drain deadline reached with items still queued",
			"items", st.Items, "drain_timeout", a.cfg.Shutdown.DrainTimeout.String())
	}
}

// warnMemoryLimit implements the GOMEMLIMIT check of SPEC §12.4. The value is
// never set automatically: reading cgroups correctly (v1 versus v2, nested
// hierarchies) is subtler than it looks, and silently overriding what an
// operator configured elsewhere would be worse than a warning.
func (a *App) warnMemoryLimit() {
	limit := debug.SetMemoryLimit(-1) // read-only probe
	// Only the ring holds ring.max_bytes in RAM; twice that leaves the GC
	// room for request overhead and the drain's batches (SPEC §12.4).
	want := a.cfg.Ring.MaxBytes * 2

	if limit == 1<<63-1 {
		// The runtime reports the same "no limit" for an absent variable and
		// for GOMEMLIMIT=off; the operator deserves to be told which.
		state := "not set"
		if os.Getenv("GOMEMLIMIT") == "off" {
			state = "off"
		}
		a.log.Warn("GOMEMLIMIT is "+state+"; a ring at its byte ceiling plus request "+
			"overhead can push this process into an OOM kill",
			"ring_max_bytes", a.cfg.Ring.MaxBytes, "suggested_gomemlimit", want)
		return
	}
	if limit < want {
		a.log.Warn("GOMEMLIMIT is below twice ring.max_bytes",
			"gomemlimit", limit, "ring_max_bytes", a.cfg.Ring.MaxBytes,
			"suggested_gomemlimit", want)
	}
}

// ignoreSIGHUP catches SIGHUP until the returned stop function is called --
// which leaves it ignored -- and answers each one with a WARN instead of a reload: the
// configuration is read once, at boot (SPEC §13.3). Catching it is the point --
// left alone, the signal would terminate the process with no drain and no
// emergency dump.
func ignoreSIGHUP(log *obs.Logger) (stop func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				log.Warn("SIGHUP ignored: this server does not reload its configuration; " +
					"restart it to apply changes")
			}
		}
	}()
	// Ignore, not Stop: Stop would hand SIGHUP back to Go's default action and
	// a signal between here and os.Exit would end the process by signal
	// instead of with the exit code run computed. Ignore also undoes Notify.
	return func() {
		signal.Ignore(syscall.SIGHUP)
		close(done)
	}
}
