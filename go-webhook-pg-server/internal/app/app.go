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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/config"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/server"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
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
// once per process (cmd/go-webhook-pg-server); the tests pass a function returning
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
				fmt.Printf("go-webhook-pg-server %s (%s, %s)\n", version, commit, runtime.Version())
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
	ring    ring.Ring
	health  *sink.Health
	workers *sink.Workers
	srv     *server.Server
	admin   *server.Admin
	emg     *emergency.Store

	// abort replaces the OS signal channel that shutdown watches for a second
	// interrupt. It is nil in the binary, which registers the real signals; the
	// integration tests set it instead, for the reason given on runContext.
	abort chan os.Signal

	// sinkOverride replaces the PostgreSQL sink. It is nil in the binary; the
	// integration tests that do not need a database set a fake one, defined in
	// the test files, so the whole lifecycle -- boot, drain, DOWN, FATAL, dump,
	// reingestion -- runs without a server (SPEC §15.3).
	sinkOverride sink.Sink

	// wrapRing, when a test sets it, wraps the memory ring before anything
	// uses it, so the test can act inside the shutdown's last Drain: the one
	// instant a late request must not be able to slip into the ring (SPEC
	// §10.3). A hook called after the Drain could not tell a Seal before the
	// Drain from one between the two (the review of the port back into
	// go-webhook-nats-server). nil in the binary.
	wrapRing func(*ring.Memory) ring.Ring

	// atRisk is how many accepted events the shutdown has collected out of
	// the ring and the workers and not yet saved or counted: the dump in
	// progress. collected says the collection happened, after which the
	// ring's own count no longer means anything. Whoever Swaps atRisk to zero
	// first -- the abort, or the graceful path once the dump is settled --
	// does the counting, so the same events are never lost twice
	// (independent review, 2026-10-07).
	atRisk    atomic.Int64
	collected atomic.Bool
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
		"ring_mode", config.RingMemory, "sink_type", config.SinkPostgresType,
		"config_file", a.loaded.File)

	for _, w := range a.loaded.Warnings {
		a.log.Warn(w)
	}
	a.warnMemoryLimit()
	a.log.Debug("effective configuration", "config", a.loaded.Effective())

	a.metrics = obs.NewMetrics(version, commit, runtime.Version(),
		config.RingMemory, config.SinkPostgresType)

	// SPEC §10.1 step 2: the admin port opens FIRST -- before the sink is even
	// built -- and stays open through the whole startup, so a crash loop caused
	// by an unreachable sink is still diagnosable through /readyz and /metrics.
	a.admin = server.NewAdmin(a.metrics, a.cfg.Ring.HighWater)
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
		// Closed on every exit path, so a boot that fails after this point
		// does not leave the listener behind. In the binary os.Exit makes
		// that moot; in the tests many boots share one process.
		defer a.admin.Shutdown(context.Background())
		a.log.Info("admin listener open", "addr", a.admin.Addr())
	}

	// Step 3: the sink. The pool is lazy; WaitReady below is where a
	// destination that is not up yet is waited for, within sink.boot_timeout.
	a.admin.SetBootState("connecting to the sink")
	if code := a.buildSink(); code != ExitOK {
		return code
	}
	defer a.sink.Close()

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
			// Refused at once (SPEC §8.5.5, §10.1): saying "within
			// boot_timeout" here would send the operator to the network when
			// the schema, the grants or the credentials are what is wrong.
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

	// Step 6: the public listener opens last.
	if err := a.srv.Listen(ctx); err != nil {
		a.log.Error("cannot bind the public listener", "addr", a.cfg.Server.Bind, "error", err)
		return ExitConfig
	}
	a.log.Info("public listener open",
		"addr", a.srv.Addr(), "listeners", a.cfg.Server.Listeners,
		"h2c", a.cfg.Server.HTTP2)
	// /readyz answers 200 only now, with the public port actually open
	// (SPEC §10.1 step 6).
	a.admin.SetBootState("")

	serveErr := make(chan error, 1)
	go func() { serveErr <- a.srv.Serve() }()

	select {
	case err := <-serveErr:
		if err != nil {
			a.log.Error("public listener stopped", "error", err)
			return ExitConfig
		}
		return ExitOK

	case <-a.health.Fatal():
		a.log.Error("sink became permanently unusable", "error", a.health.FatalErr())
		return a.shutdown(stopWorkers, true)

	case <-ctx.Done():
		stopSignals() // restore default handling so a second signal is not swallowed
		a.log.Info("shutdown requested")
		return a.shutdown(stopWorkers, false)
	}
}

// buildSink constructs the destination. Nothing connects here: the pool is
// lazy and WaitReady owns the boot retry loop, so the only failures are
// configuration -- a bad CA file, an address that does not parse -- and they
// exit 1 (SPEC §8.5.2).
func (a *App) buildSink() int {
	if a.sinkOverride != nil {
		a.sink = a.sinkOverride
		return ExitOK
	}
	s, err := sink.NewPostgres(a.postgresConfig(), a.log.Logger, a.metrics)
	if err != nil {
		a.log.Error("cannot configure the PostgreSQL sink", "error", err)
		return ExitConfig
	}
	a.sink = s
	a.log.Info("sink ready", "type", "postgres",
		"addr", a.cfg.SinkPG.Addr, "database", a.cfg.SinkPG.Database,
		"table", a.cfg.SinkPG.Table, "pool_size", a.cfg.SinkPG.PoolSize,
		"tls", a.cfg.SinkPG.TLS)
	return ExitOK
}

// postgresConfig maps [sink.postgres] and the [sink] deadlines onto the sink.
func (a *App) postgresConfig() sink.PostgresConfig {
	return sink.PostgresConfig{
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
		// SPEC §8.5.5: the boot validation is a dozen round trips and gets
		// the drain's deadline, not the one-second health check's.
		ValidateTimeout: a.cfg.Sink.OpTimeout,
		AppName:         a.clientName(),
	}
}

// clientName names this instance's connections to the destination (SPEC §3.5):
// application_name, visible in pg_stat_activity.
func (a *App) clientName() string {
	return "whs/" + a.id.AgentName + "@" + a.id.Hostname
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

	// The emergency dump belongs to the ring (SPEC §9). One line of a dump
	// is at most the largest envelope this configuration can produce.
	if a.cfg.Emergency.Enabled {
		a.emg = emergency.NewStore(a.cfg.Emergency.Dir, a.id.From, config.MaxEventBytes(a.cfg))
	}

	a.workers = sink.NewWorkers(a.ring, a.sink, a.health, sink.WorkerConfig{
		Workers:    a.cfg.Sink.Workers,
		Batch:      a.cfg.Sink.Batch,
		OpTimeout:  a.cfg.Sink.OpTimeout,
		BackoffMin: a.cfg.Sink.RetryBackoffMin,
		BackoffMax: a.cfg.Sink.RetryBackoffMax,
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
func (a *App) shutdown(stopWorkers context.CancelFunc, fatal bool) int {
	abort := a.abort
	if abort == nil {
		abort = make(chan os.Signal, 1)
		signal.Notify(abort, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		defer signal.Stop(abort)
	}

	// aborted stops the graceful path when the abort wins: in the binary
	// os.Exit makes that moot, but in the tests the instance would otherwise
	// keep draining and dumping into a directory the test has removed.
	abortCtx, aborted := context.WithCancel(context.Background())
	defer aborted()

	done := make(chan int, 1)
	go func() { done <- a.gracefulShutdown(abortCtx, stopWorkers, fatal) }()

	signals := 0
	for {
		select {
		case code := <-done:
			return code
		case sig := <-abort:
			signals++
			// SPEC §10.3: the SECOND signal aborts. A shutdown started by a
			// signal has had its first one; a FATAL shutdown has not, and the
			// orchestrator's ordinary SIGTERM arriving while the dump is being
			// written must not throw the dump away (independent review,
			// 2026-10-07). SIGQUIT always aborts.
			if fatal && signals == 1 && sig != syscall.SIGQUIT {
				a.log.Warn("signal during the emergency shutdown; it goes on -- a second one aborts "+
					"and accepts the loss", "signal", sig.String())
				continue
			}
			aborted()
			a.log.Warn("second signal during shutdown; exiting immediately and accepting the loss",
				"signal", sig.String())
			_ = a.srv.Close()
			lost := a.atRisk.Swap(0)
			if !a.collected.Load() {
				lost = a.ring.Stats().Items
			}
			if lost > 0 {
				a.metrics.EventsDroppedTotal[obs.DropAbort].Add(uint64(lost))
				a.log.Error("events lost to the abort", "count", lost)
			}
			return ExitAborted
		}
	}
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
	if fatal {
		code = ExitSink
	}
	if !fatal {
		a.waitDrained(abortCtx, time.Until(deadline))
	}
	if abortCtx.Err() != nil {
		return ExitAborted // the abort has already counted what was left
	}

	stopWorkers()
	a.workers.Wait()

	leftover := a.workers.Pending()
	if len(leftover) > 0 {
		a.log.Warn("workers surrendered batches they were retrying", "count", len(leftover))
	}
	// Seal the ring before the last Drain (SPEC §10.3). An upload still
	// reading its body when phase 3 ran out of time goes on after it; without
	// the seal it could finish now, land in a ring nobody reads again and be
	// answered 200 -- in neither the destination nor the dump, counted
	// nowhere. Sealed, it gets a 503. Only the memory ring seals: a ring.db
	// row written now is still drained by the next boot.
	if s, ok := a.ring.(interface{ Seal() }); ok {
		s.Seal()
	}
	leftover = append(leftover, a.ring.Drain()...)
	a.atRisk.Store(int64(len(leftover)))
	a.collected.Store(true)

	if len(leftover) > 0 {
		a.log.Error("events still pending after the drain", "count", len(leftover))
		if a.emg == nil {
			// emergency.enabled=false: the loss is the abort's (SPEC §9.3). A
			// dump that FAILED is counted by dumpEmergency as
			// emergency_failed, and the same events are not lost twice
			// (SPEC §11.1).
			a.log.Error("emergency.enabled=false; these events are lost", "count", len(leftover))
			if n := a.atRisk.Swap(0); n > 0 {
				a.metrics.EventsDroppedTotal[obs.DropAbort].Add(uint64(n))
			}
		} else {
			lost := a.dumpEmergency(leftover)
			if a.atRisk.Swap(0) > 0 && lost > 0 {
				a.metrics.EventsDroppedTotal[obs.DropEmergencyFailed].Add(uint64(lost))
			}
		}
		code = ExitSink
	}

	// Phase 5.
	_ = a.admin.Shutdown(ctx)
	a.log.Info("shutdown complete", "exit_code", code)
	return code
}

// waitDrained blocks until the ring is empty or the deadline passes.
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
