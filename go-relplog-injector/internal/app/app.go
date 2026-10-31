// Package app is the command line of go-relplog-injector (SPEC §3): flags,
// validation, wiring of source, transport and scheduler, signals, report and
// exit code.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/source"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/tmpl"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/transport"
)

// Exit codes (SPEC §3.3).
const (
	ExitOK          = 0
	ExitFailures    = 1
	ExitUsage       = 2
	ExitUnreachable = 3
	ExitInterrupted = 130
)

var (
	version = "dev"
	commit  = "none"
)

// SetBuildInfo is called by main with the -ldflags values.
func SetBuildInfo(v, c string) { version, commit = v, c }

// Name is the program name.
const Name = "go-relplog-injector"

type config struct {
	target, network           string
	timeout, ackTimeout       time.Duration
	window, retries           int
	onReject                  string
	tlsCA, tlsCert, tlsKey    string
	tlsServerName, tlsMin     string
	tlsInsecure               bool
	proxy, proxySrc, proxyDst string

	count       int64
	duration    time.Duration
	rate        float64
	concurrency int
	drain       time.Duration

	template, file, pcapFile string
	format                   string
	facility, severity       string
	hostname, app, msgid     string
	size                     int
	run                      string
	loop                     bool
	pcapPorts, pcapProto     string
	pcapTiming               string
	pcapSpeed                float64

	reportFmt string
	progress  string
	showVer   bool
}

func newFlags(c *config, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.target, "target", "127.0.0.1:20514", "RELP server `host:port`")
	fs.StringVar(&c.network, "transport", "tcp", "`tcp` or tls (RELP over TLS, as omrelp tls=\"on\")")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "dial, handshake, write and open-answer timeout")
	fs.DurationVar(&c.ackTimeout, "ack-timeout", 90*time.Second, "a session whose oldest transaction has no answer for this long is broken (omrelp's timeout)")
	fs.IntVar(&c.window, "window", 128, "unanswered transactions per session (librelp's window)")
	fs.IntVar(&c.retries, "retries", 10, "sessions an unanswered message is resent on after the first (0 = never; librelp: no limit)")
	fs.StringVar(&c.onReject, "on-reject", "break", "after a non-200 answer: `break` the session as librelp does, or continue")
	fs.StringVar(&c.tlsCA, "tls-ca", "", "PEM `file` of the CAs that verify the server (default: system roots)")
	fs.StringVar(&c.tlsCert, "tls-cert", "", "client certificate PEM `file` (mTLS)")
	fs.StringVar(&c.tlsKey, "tls-key", "", "client key PEM `file` (mTLS)")
	fs.StringVar(&c.tlsServerName, "tls-server-name", "", "name verified in the server certificate (default: the target host)")
	fs.StringVar(&c.tlsMin, "tls-min-version", "1.2", "minimum TLS `version`: 1.2 or 1.3")
	fs.BoolVar(&c.tlsInsecure, "tls-insecure", false, "do not verify the server certificate")
	fs.StringVar(&c.proxy, "proxy-protocol", "off", "PROXY protocol header on TCP/TLS: off, `v1` or v2")
	fs.StringVar(&c.proxySrc, "proxy-src", "", "source `ip:port` announced in the PROXY header (default: the real one)")
	fs.StringVar(&c.proxyDst, "proxy-dst", "", "destination `ip:port` announced in the PROXY header (default: the real one)")

	fs.Int64Var(&c.count, "count", -1, "messages to send; 0 = no limit (default: 1; no limit with --duration; the whole --file/--pcap)")
	fs.DurationVar(&c.duration, "duration", 0, "stop after this `time` (0 = no limit)")
	fs.Float64Var(&c.rate, "rate", 0, "target `msg/s` over all connections (0 = as fast as possible)")
	fs.IntVar(&c.concurrency, "concurrency", 1, "parallel RELP sessions")
	fs.DurationVar(&c.drain, "drain-timeout", 10*time.Second, "at the end, wait at most this long for queued messages")

	fs.StringVar(&c.template, "template", "", "message body `template` with markers {seq} {run} {uuid} {ts} {unix} {unixms} {host} {pid} {rand:N}")
	fs.StringVar(&c.file, "file", "", "send one message per line of this `file` (- = stdin)")
	fs.StringVar(&c.pcapFile, "pcap", "", "replay the syslog messages of this tcpdump capture `file` (pcap or pcapng)")
	fs.StringVar(&c.format, "format", "", "header around the body: `5424`, 3164 or raw (default: 5424, raw for --file/--pcap)")
	fs.StringVar(&c.facility, "facility", "user", "facility `name` or number 0-23")
	fs.StringVar(&c.severity, "severity", "notice", "severity `name` or number 0-7")
	fs.StringVar(&c.hostname, "hostname", "", "HOSTNAME of the header (default: this host)")
	fs.StringVar(&c.app, "app", Name, "APP-NAME (5424) or TAG (3164)")
	fs.StringVar(&c.msgid, "msgid", "", "MSGID of the 5424 header (default: -)")
	fs.IntVar(&c.size, "size", 0, "pad generated messages to this many `bytes`")
	fs.StringVar(&c.run, "run", "", "run `id` for {run} (default: 8 random hex characters)")
	fs.BoolVar(&c.loop, "loop", false, "restart --file/--pcap at its end (bounded by --count/--duration)")
	fs.StringVar(&c.pcapPorts, "pcap-port", "", "comma-separated destination `ports` to replay; given, the content check is off")
	fs.StringVar(&c.pcapProto, "pcap-proto", "udp,tcp", "protocols to replay from the capture: `udp,tcp`, udp or tcp")
	fs.StringVar(&c.pcapTiming, "pcap-timing", "asap", "`asap` (paced by --rate) or original (the capture's own intervals)")
	fs.Float64Var(&c.pcapSpeed, "pcap-speed", 1, "speed `factor` of --pcap-timing original (2 = twice as fast)")

	fs.StringVar(&c.reportFmt, "report", "text", "final report on stdout: `text` or json")
	fs.StringVar(&c.progress, "progress", "auto", "progress line on stderr every `interval`; 0 = off; auto = 1s on a terminal")
	fs.BoolVar(&c.showVer, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: %s [flags]\n\nSends syslog messages to a RELP server, to test it or load it (docs/SPEC.md).\n\nFlags:\n", Name)
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nExit: 0 all acknowledged, 1 some failed, 2 usage, 3 target unreachable, 130 interrupted.\n")
	}
	return fs
}

// Main runs the program and returns the exit code.
func Main(args []string, stdout, stderr io.Writer) int {
	var c config
	fs := newFlags(&c, stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK
		}
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "%s: unexpected argument %q\n", Name, fs.Arg(0))
		return ExitUsage
	}
	if c.showVer {
		fmt.Fprintf(stdout, "%s %s (%s)\n", Name, version, commit)
		return ExitOK
	}
	r, err := build(&c)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", Name, err)
		return ExitUsage
	}
	defer r.src.Close()
	return r.run(stdout, stderr)
}

type runner struct {
	c      *config
	src    source.Source
	dial   sched.Dialer
	plan   sched.Plan
	info   map[string]string
	report string
}

func build(c *config) (*runner, error) {
	sources := 0
	for _, s := range []string{c.template, c.file, c.pcapFile} {
		if s != "" {
			sources++
		}
	}
	if sources > 1 {
		return nil, errors.New("--template, --file and --pcap are mutually exclusive")
	}
	if c.concurrency < 1 {
		return nil, errors.New("--concurrency must be >= 1")
	}
	if c.rate < 0 || c.size < 0 || c.duration < 0 || c.count < -1 {
		return nil, errors.New("--rate, --size, --duration and --count cannot be negative")
	}
	if c.timeout <= 0 || c.drain <= 0 {
		return nil, errors.New("--timeout and --drain-timeout must be > 0")
	}
	if c.reportFmt != "text" && c.reportFmt != "json" {
		return nil, fmt.Errorf("--report: want text or json, got %q", c.reportFmt)
	}
	if c.run == "" {
		c.run = tmpl.NewRunID()
	}
	if c.hostname == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			h = "localhost"
		}
		c.hostname = strings.SplitN(h, ".", 2)[0]
	}
	fac, err := facility(c.facility)
	if err != nil {
		return nil, err
	}
	sev, err := severity(c.severity)
	if err != nil {
		return nil, err
	}
	finite := c.file != "" || c.pcapFile != ""
	fmtName := c.format
	if fmtName == "" {
		fmtName = "5424"
		if finite {
			fmtName = "raw"
		}
	}
	f, err := source.ParseFormat(fmtName)
	if err != nil {
		return nil, err
	}
	if f == source.FormatRaw && !finite && c.template == "" {
		return nil, errors.New("--format raw needs --template, --file or --pcap: the generator has no header of its own")
	}
	h := source.Header{Format: f, PRI: fac*8 + sev, Hostname: c.hostname, App: c.app, PID: os.Getpid(), MsgID: c.msgid}
	if c.size > 0 && finite {
		return nil, errors.New("--size pads generated messages only, not --file/--pcap")
	}
	if c.loop && !finite {
		return nil, errors.New("--loop applies to --file and --pcap")
	}
	r := &runner{c: c, report: c.reportFmt, info: map[string]string{}}
	switch {
	case c.pcapFile != "":
		opt := source.PcapOptions{Loop: c.loop, Header: h}
		if opt.Ports, err = ports(c.pcapPorts); err != nil {
			return nil, err
		}
		for _, p := range strings.Split(c.pcapProto, ",") {
			switch strings.TrimSpace(p) {
			case "udp":
				opt.UDP = true
			case "tcp":
				opt.TCP = true
			default:
				return nil, fmt.Errorf("--pcap-proto: want udp and/or tcp, got %q", p)
			}
		}
		r.src, err = source.NewPcap(c.pcapFile, opt)
		r.info["source"] = "pcap " + c.pcapFile
	case c.file != "":
		r.src, err = source.NewFile(c.file, c.loop, h)
		r.info["source"] = "file " + c.file
	default:
		body := source.GenBody
		r.info["source"] = "generator"
		if c.template != "" {
			body = c.template
			r.info["source"] = "template"
		}
		var t *tmpl.Template
		if t, err = tmpl.Compile(body); err == nil {
			r.src = &source.Template{T: t, H: h, Run: c.run, Size: c.size}
		}
	}
	if err != nil {
		return nil, err
	}
	switch c.pcapTiming {
	case "asap":
	case "original":
		if c.pcapFile == "" {
			return nil, errors.New("--pcap-timing original needs --pcap")
		}
		if c.rate > 0 {
			return nil, errors.New("--rate and --pcap-timing original are exclusive: the capture sets the pace")
		}
		if c.pcapSpeed <= 0 {
			return nil, errors.New("--pcap-speed must be > 0")
		}
		r.plan.Original, r.plan.Speed = true, c.pcapSpeed
	default:
		return nil, fmt.Errorf("--pcap-timing: want asap or original, got %q", c.pcapTiming)
	}
	count := c.count
	if count < 0 {
		// Unset: one message, unless something else bounds the run -- the
		// whole --file/--pcap, or --duration.
		count = 1
		if finite || c.duration > 0 {
			count = 0
		}
	}
	r.plan.Count = uint64(count)
	r.plan.Duration, r.plan.Rate, r.plan.Workers, r.plan.Drain = c.duration, c.rate, c.concurrency, c.drain

	if c.window < 1 || c.window > transport.MaxWindow {
		return nil, fmt.Errorf("--window must be 1..%d", transport.MaxWindow)
	}
	if c.retries < 0 {
		return nil, errors.New("--retries cannot be negative")
	}
	if c.ackTimeout <= 0 {
		return nil, errors.New("--ack-timeout must be > 0")
	}
	if c.network != "tcp" && c.network != "tls" {
		return nil, fmt.Errorf("--transport: want tcp or tls, got %q", c.network)
	}
	o := transport.Options{Network: c.network, Target: c.target, Timeout: c.timeout, AckTimeout: c.ackTimeout,
		Window: c.window, Software: Name + "," + version + ",https://github.com/patrickbrandao/loghub-paradise"}
	switch c.onReject {
	case "break":
		o.BreakOnReject = true
	case "continue":
	default:
		return nil, fmt.Errorf("--on-reject: want break or continue, got %q", c.onReject)
	}
	r.plan.Retries = c.retries
	if c.network == "tls" {
		if o.TLS, err = tlsConfig(c); err != nil {
			return nil, err
		}
	} else if c.tlsCA != "" || c.tlsCert != "" || c.tlsKey != "" || c.tlsInsecure {
		return nil, errors.New("--tls-* flags need --transport tls")
	}
	switch c.proxy {
	case "off":
		if c.proxySrc != "" || c.proxyDst != "" {
			return nil, errors.New("--proxy-src/--proxy-dst need --proxy-protocol v1 or v2")
		}
	case "v1", "v2":
		ph := &transport.ProxyHeader{Version: int(c.proxy[1] - '0')}
		if c.proxySrc != "" {
			if ph.Src, err = netip.ParseAddrPort(c.proxySrc); err != nil {
				return nil, fmt.Errorf("--proxy-src: %w", err)
			}
		}
		if c.proxyDst != "" {
			if ph.Dst, err = netip.ParseAddrPort(c.proxyDst); err != nil {
				return nil, fmt.Errorf("--proxy-dst: %w", err)
			}
		}
		o.Proxy = ph
	default:
		return nil, fmt.Errorf("--proxy-protocol: want off, v1 or v2, got %q", c.proxy)
	}
	if r.dial, err = transport.Dialer(o); err != nil {
		return nil, err
	}
	r.info["injector"] = Name + " " + version
	r.info["run"] = c.run
	r.info["target"] = "relp+" + c.network + "://" + c.target
	r.info["window"] = strconv.Itoa(c.window)
	r.info["format"] = fmtName
	r.info["plan"] = planText(r.plan)
	return r, nil
}

func planText(p sched.Plan) string {
	var b []string
	if p.Count > 0 {
		b = append(b, "count "+strconv.FormatUint(p.Count, 10))
	}
	if p.Duration > 0 {
		b = append(b, "duration "+p.Duration.String())
	}
	switch {
	case p.Original:
		b = append(b, "original timing x"+strconv.FormatFloat(p.Speed, 'g', -1, 64))
	case p.Rate > 0:
		b = append(b, "rate "+strconv.FormatFloat(p.Rate, 'g', -1, 64)+"/s")
	default:
		b = append(b, "max rate")
	}
	b = append(b, "concurrency "+strconv.Itoa(p.Workers))
	return strings.Join(b, ", ")
}

func (r *runner) run(stdout, stderr io.Writer) int {
	rep := report.New()
	rep.Info = r.info
	rep.OKOutcomes = []string{transport.Acked}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	var interrupted atomic.Bool
	go func() {
		if _, ok := <-sig; !ok {
			return
		}
		interrupted.Store(true)
		fmt.Fprintf(stderr, "%s: interrupted, draining (again to abort)\n", Name)
		cancel()
		if _, ok := <-sig; ok {
			rep.End = time.Now()
			r.write(rep, stdout)
			os.Exit(ExitInterrupted)
		}
	}()
	rep.Start = time.Now()
	stopProg := make(chan struct{})
	if every := r.progressEvery(stderr); every > 0 {
		go rep.Progress(stderr, every, stopProg)
	}
	st := sched.Run(ctx, r.plan, r.src, r.dial, rep)
	close(stopProg)
	rep.End = time.Now()
	rep.Source = r.src.Stats()
	if st.Behind > 0 {
		rep.Event(fmt.Sprintf("behind_schedule (>100ms late): %d messages", st.Behind))
	}
	if st.SourceErr != nil {
		rep.Event("source_error: " + st.SourceErr.Error())
	}
	r.write(rep, stdout)
	ok, failed := rep.Totals()
	switch {
	case interrupted.Load():
		return ExitInterrupted
	case ok+failed > 0 && rep.Dials() == 0:
		return ExitUnreachable
	case failed > 0 || st.SourceErr != nil:
		return ExitFailures
	}
	return ExitOK
}

func (r *runner) write(rep *report.Report, w io.Writer) {
	if r.report == "json" {
		_ = rep.WriteJSON(w)
		return
	}
	_ = rep.WriteText(w)
}

func (r *runner) progressEvery(stderr io.Writer) time.Duration {
	switch r.c.progress {
	case "auto":
		if f, ok := stderr.(*os.File); ok {
			if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
				return time.Second
			}
		}
		return 0
	case "0", "off":
		return 0
	}
	d, err := time.ParseDuration(r.c.progress)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

func tlsConfig(c *config) (*tls.Config, error) {
	tc := &tls.Config{ServerName: c.tlsServerName, InsecureSkipVerify: c.tlsInsecure}
	switch c.tlsMin {
	case "1.2":
		tc.MinVersion = tls.VersionTLS12
	case "1.3":
		tc.MinVersion = tls.VersionTLS13
	default:
		return nil, fmt.Errorf("--tls-min-version: want 1.2 or 1.3, got %q", c.tlsMin)
	}
	if tc.ServerName == "" {
		host := c.target
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		tc.ServerName = strings.Trim(host, "[]")
	}
	if c.tlsCA != "" {
		pem, err := os.ReadFile(c.tlsCA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("--tls-ca %s: no PEM certificate", c.tlsCA)
		}
		tc.RootCAs = pool
	}
	if (c.tlsCert == "") != (c.tlsKey == "") {
		return nil, errors.New("--tls-cert and --tls-key go together")
	}
	if c.tlsCert != "" {
		cert, err := tls.LoadX509KeyPair(c.tlsCert, c.tlsKey)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func ports(s string) (map[uint16]bool, error) {
	if s == "" {
		return nil, nil
	}
	m := map[uint16]bool{}
	for _, p := range strings.Split(s, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(p), 10, 16)
		if err != nil || v == 0 {
			return nil, fmt.Errorf("--pcap-port: bad port %q", p)
		}
		m[uint16(v)] = true
	}
	return m, nil
}

var facilities = map[string]int{
	"kern": 0, "user": 1, "mail": 2, "daemon": 3, "auth": 4, "syslog": 5, "lpr": 6, "news": 7,
	"uucp": 8, "cron": 9, "authpriv": 10, "ftp": 11, "ntp": 12, "security": 13, "console": 14,
	"solaris-cron": 15, "local0": 16, "local1": 17, "local2": 18, "local3": 19, "local4": 20,
	"local5": 21, "local6": 22, "local7": 23,
}

var severities = map[string]int{
	"emerg": 0, "panic": 0, "alert": 1, "crit": 2, "err": 3, "error": 3, "warning": 4, "warn": 4,
	"notice": 5, "info": 6, "debug": 7,
}

func facility(s string) (int, error) { return level("--facility", s, facilities, 23) }
func severity(s string) (int, error) { return level("--severity", s, severities, 7) }

func level(flagName, s string, names map[string]int, max int) (int, error) {
	if v, ok := names[strings.ToLower(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 || v > max {
		return 0, fmt.Errorf("%s: unknown %q", flagName, s)
	}
	return v, nil
}
