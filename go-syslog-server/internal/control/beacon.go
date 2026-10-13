// Package control is this server's slice of the LogHub control plane: the
// heartbeat beacon of SPEC §11, and only that.
//
// There is no go-loghub-driver, so the server is a driver of itself and emits
// the heartbeat the skill demands of every worker. It never answers a sonar,
// never emits or answers ping/pong and subscribes to nothing (invariant §20.21):
// a documented gap until a driver exists (SPEC §21).
package control

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// Publisher is what the beacon needs of the NATS sink: a core publish on the
// sink's own connection, and the address of the server that connection is on.
// *sink.NATS implements it; declaring it here keeps this package free of the
// sink's dependencies and lets a test publish into a slice.
type Publisher interface {
	// PublishCore publishes by core NATS, fanout, never JetStream (SPEC §11).
	PublishCore(subject string, data []byte) error
	// ServerAddr is host:port of the connected server, or "".
	ServerAddr() string
}

// DefaultNamespace is loghub.ctl_namespace's default (env LOGHUB_CTL_NAMESPACE).
const DefaultNamespace = "loghub._ctl"

// MinInterval is the shortest periodic beacon this package emits. The
// configuration refuses a shorter loghub.beacon_interval (zero aside, SPEC
// §11); the beacon holds the same line on its own, so no path into it -- a
// caller that skipped the validation, a test -- turns 1ns or 15ms (for 15s)
// into a loop that floods <ctl_namespace>.beacon and competes for the one
// connection the data plane writes on.
const MinInterval = time.Second

// Config is the [loghub] beacon settings plus what the payload reports.
type Config struct {
	Identity ident.Identity
	// Interval is loghub.beacon_interval. Zero disables only the PERIODIC
	// beacon; the one on connection and on every reconnection still goes out
	// (SPEC §11).
	Interval time.Duration
	// Namespace is loghub.ctl_namespace; the subject is <Namespace>.beacon.
	Namespace string

	// The meta of the payload.
	Version    string
	RingMode   string
	Transports []string

	// Started is the process start, for uptime. Zero uses the moment this
	// package was initialised, which is the process start to within the
	// runtime's own initialisation.
	Started time.Time
}

// processStart is the fallback for Config.Started.
var processStart = time.Now()

// Beacon emits the heartbeat: on the first connection, on every reconnection
// (Kick, from the NATS sink's OnConnect hook) and every Interval.
//
// Failures are counted (syslog_beacons_total{result="error"}) and logged with
// sampling; they NEVER touch the sink's health. A beacon is a courtesy to the
// control plane, and losing one must not stop the data plane.
type Beacon struct {
	pub     Publisher
	cfg     Config
	subject string
	log     *slog.Logger
	m       *obs.Metrics
	errLog  *obs.Sampler
	pid     uint64

	// kick has room for one pending signal: reconnections that arrive while a
	// beacon is being published coalesce into the next one.
	kick chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// New builds the beacon. It does nothing until Start.
func New(pub Publisher, cfg Config, log *slog.Logger, m *obs.Metrics) *Beacon {
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
	if cfg.Started.IsZero() {
		cfg.Started = processStart
	}
	if cfg.Interval < 0 {
		cfg.Interval = 0
	}
	if cfg.Interval > 0 && cfg.Interval < MinInterval {
		log.Warn("loghub.beacon_interval below the minimum; the beacon uses the minimum",
			"beacon_interval", cfg.Interval.String(), "minimum", MinInterval.String())
		cfg.Interval = MinInterval
	}
	pid := uint64(os.Getpid())
	if pid > jlh.MaxSID {
		pid &= jlh.MaxSID
	}
	return &Beacon{
		pub: pub, cfg: cfg, subject: cfg.Namespace + ".beacon",
		log: log, m: m, errLog: obs.NewSampler(30 * time.Second),
		pid: pid, kick: make(chan struct{}, 1),
	}
}

// Subject is <ctl_namespace>.beacon.
func (b *Beacon) Subject() string { return b.subject }

// Kick asks for a beacon now. It never blocks, so it is safe as the NATS
// sink's connect hook, which runs on the client's callback goroutine. A kick
// that arrives before Start waits for it: the first connection happens during
// the boot's WaitReady, long before the beacon starts (SPEC §13.1 steps 7 and
// 11), and its beacon goes out as soon as Start runs.
func (b *Beacon) Kick() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

// Start launches the beacon goroutine. Calling it twice is a no-op.
func (b *Beacon) Start(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel
	b.done = make(chan struct{})
	go b.run(ctx, b.done)
}

// Stop ends the goroutine and waits for it (SPEC §13.4 phase 5).
func (b *Beacon) Stop() {
	b.mu.Lock()
	cancel, done := b.cancel, b.done
	b.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func (b *Beacon) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	var tick <-chan time.Time
	if b.cfg.Interval > 0 {
		t := time.NewTicker(b.cfg.Interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.kick:
			b.Publish()
		case <-tick:
			b.Publish()
		}
	}
}

// Publish builds and sends one beacon now, and reports whether it went out.
func (b *Beacon) Publish() bool {
	env := b.Envelope(time.Now(), b.address())
	if err := b.pub.PublishCore(b.subject, env); err != nil {
		b.m.BeaconsError.Inc()
		if ok, suppressed := b.errLog.Allow(); ok {
			b.log.Warn("beacon not published; the sink is unaffected",
				"subject", b.subject, "error", err, "suppressed", suppressed)
		}
		return false
	}
	b.m.BeaconsOK.Inc()
	return true
}

// address is the local IP of the route to the NATS server: a UDP "dial" sends
// nothing, it only asks the kernel which source address it would use. Without
// a server address, or when that fails, it is 127.0.0.1 (SPEC §11).
func (b *Beacon) address() string {
	if addr := b.pub.ServerAddr(); addr != "" {
		if c, err := net.Dial("udp", addr); err == nil {
			defer c.Close()
			if ua, ok := c.LocalAddr().(*net.UDPAddr); ok && ua.IP != nil && !ua.IP.IsUnspecified() {
				return ua.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

// Envelope serialises one beacon for the instant now (SPEC §11), with the same
// writer as every data envelope (SPEC §3.3):
//
//	uuid and event   one clock sample, uuid at level 3
//	from, space      the instance's identity
//	type             "beacon"
//	level            9 (data-channel)
//	sid              the PID
//	step             0
//	payload          {name, hostname, machine, address, uptime, meta}, in that
//	                 order; meta is version, ring, sink, transports
func (b *Beacon) Envelope(now time.Time, address string) []byte {
	id := b.cfg.Identity
	dst := make([]byte, 0, 512)
	dst = jlh.AppendHeader(dst, &jlh.Header{
		UUID:  jlh.NewUUID(now),
		From:  id.From,
		Space: id.Space,
		Type:  "beacon",
		Level: 9,
		SID:   b.pid,
		Step:  0,
		Event: jlh.EventAt(now),
	})

	uptime := now.Sub(b.cfg.Started).Milliseconds()
	if uptime < 0 {
		uptime = 0
	}

	dst = append(dst, '{')
	dst = jlh.AppendKey(dst, "name")
	dst = jlh.AppendStr(dst, id.AgentName)
	dst = append(dst, ',')
	dst = jlh.AppendKey(dst, "hostname")
	dst = jlh.AppendStr(dst, id.Hostname)
	dst = append(dst, ',')
	dst = jlh.AppendKey(dst, "machine")
	dst = jlh.AppendStr(dst, id.MachineID)
	dst = append(dst, ',')
	dst = jlh.AppendKey(dst, "address")
	dst = jlh.AppendStr(dst, address)
	dst = append(dst, ',')
	dst = jlh.AppendKey(dst, "uptime")
	dst = strconv.AppendInt(dst, uptime, 10)
	dst = append(dst, ',')
	dst = jlh.AppendKey(dst, "meta")
	dst = append(dst, '[')
	dst = appendMeta(dst, "version", b.cfg.Version)
	dst = append(dst, ',')
	dst = appendMeta(dst, "ring", b.cfg.RingMode)
	dst = append(dst, ',')
	dst = appendMeta(dst, "sink", "nats")
	dst = append(dst, `,{"key":"transports","value":[`...)
	for i, t := range b.cfg.Transports {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = jlh.AppendStr(dst, t)
	}
	dst = append(dst, "]}]}"...)
	return jlh.Close(dst)
}

func appendMeta(dst []byte, key, value string) []byte {
	dst = append(dst, `{"key":`...)
	dst = jlh.AppendStr(dst, key)
	dst = append(dst, `,"value":`...)
	dst = jlh.AppendStr(dst, value)
	return append(dst, '}')
}
