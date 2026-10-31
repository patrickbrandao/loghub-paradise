// Package app is the command line of go-gelf-injector (SPEC §3): flags,
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
	"net"
	"net/netip"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/gelf"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/source"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/tmpl"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/transport"
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
const Name = "go-gelf-injector"

// DefaultTargets per transport (SPEC §3.1): the GELF ports of the servers
// (go-gelf-server DESIGN decision 5).
var DefaultTargets = map[string]string{
	"udp":   "127.0.0.1:12201",
	"tcp":   "127.0.0.1:12201",
	"tls":   "127.0.0.1:12203",
	"http":  "127.0.0.1:12202",
	"https": "127.0.0.1:12204",
}

type config struct {
	target, network           string
	compress                  string
	chunkSize                 int
	timeout                   time.Duration
	tlsCA, tlsCert, tlsKey    string
	tlsServerName, tlsMin     string
	tlsInsecure               bool
	proxy, proxySrc, proxyDst string

	httpPath, httpHost string
	pipeline           int
	perConn            uint64
	retries            int
	retryMaxWait       time.Duration
	respTimeout        time.Duration

	count       int64
	duration    time.Duration
	rate        float64
	concurrency int
	drain       time.Duration

	template, file, pcapFile string
	hostname                 string
	level                    int
	payloadSize              int
	run                      string
	loop                     bool
	pcapPorts, pcapProto     string
	pcapTiming               string
	pcapSpeed                float64

	reportFmt string
	progress  string
	showVer   bool

	set map[string]bool // flags given on the command line
}

func newFlags(c *config, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.target, "target", "", "server `host:port` (default: 127.0.0.1 and the GELF port of --transport: 12201 udp/tcp, 12203 tls, 12202 http, 12204 https)")
	fs.StringVar(&c.network, "transport", "tcp", "`tcp`, udp, tls, http or https")
	fs.StringVar(&c.compress, "compress", "", "payload compression on udp/http/https: `none`, gzip or zlib (default: gzip on udp, none otherwise; never on tcp/tls)")
	fs.IntVar(&c.chunkSize, "chunk-size", 8192, "udp: largest datagram in `bytes`, chunk header included; a larger message goes in GELF chunks (at most 128)")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "dial, handshake and write timeout")
	fs.StringVar(&c.tlsCA, "tls-ca", "", "PEM `file` of the CAs that verify the server (default: system roots)")
	fs.StringVar(&c.tlsCert, "tls-cert", "", "client certificate PEM `file` (mTLS)")
	fs.StringVar(&c.tlsKey, "tls-key", "", "client key PEM `file` (mTLS)")
	fs.StringVar(&c.tlsServerName, "tls-server-name", "", "name verified in the server certificate (default: the target host)")
	fs.StringVar(&c.tlsMin, "tls-min-version", "1.2", "minimum TLS `version`: 1.2 or 1.3")
	fs.BoolVar(&c.tlsInsecure, "tls-insecure", false, "do not verify the server certificate")
	fs.StringVar(&c.proxy, "proxy-protocol", "off", "PROXY protocol header on tcp/tls/http/https: off, `v1` or v2")
	fs.StringVar(&c.proxySrc, "proxy-src", "", "source `ip:port` announced in the PROXY header (default: the real one)")
	fs.StringVar(&c.proxyDst, "proxy-dst", "", "destination `ip:port` announced in the PROXY header (default: the real one)")

	fs.StringVar(&c.httpPath, "http-path", "/gelf", "http/https: request `path` of the POST")
	fs.StringVar(&c.httpHost, "http-host", "", "http/https: Host header (default: the target)")
	fs.IntVar(&c.pipeline, "pipeline", 1, "http/https: requests in flight per connection (pipelining when > 1)")
	fs.Uint64Var(&c.perConn, "requests-per-conn", 0, "http/https: close and reopen the connection after this many `requests` (0 = keep it)")
	fs.IntVar(&c.retries, "retries", 0, "http/https: resend a request answered 429 or 503 up to this many `times`, after its Retry-After")
	fs.DurationVar(&c.retryMaxWait, "retry-max-wait", 5*time.Second, "http/https: longest wait before a resend, whatever Retry-After says")
	fs.DurationVar(&c.respTimeout, "response-timeout", 30*time.Second, "http/https: wait for each response at most this long")

	fs.Int64Var(&c.count, "count", -1, "messages to send; 0 = no limit (default: 1; no limit with --duration; the whole --file/--pcap)")
	fs.DurationVar(&c.duration, "duration", 0, "stop after this `time` (0 = no limit)")
	fs.Float64Var(&c.rate, "rate", 0, "target `msg/s` over all connections (0 = as fast as possible)")
	fs.IntVar(&c.concurrency, "concurrency", 1, "parallel connections (sockets)")
	fs.DurationVar(&c.drain, "drain-timeout", 10*time.Second, "at the end, wait at most this long for queued messages, responses and retries")

	fs.StringVar(&c.template, "template", "", "GELF document `template` (JSON) with markers {seq} {run} {uuid} {ts} {unix} {unixms} {host} {pid} {rand:N}")
	fs.StringVar(&c.file, "file", "", "send one GELF document per line of this `file` (- = stdin)")
	fs.StringVar(&c.pcapFile, "pcap", "", "replay the GELF messages of this tcpdump capture `file` (pcap or pcapng): UDP, TCP and HTTP")
	fs.StringVar(&c.hostname, "hostname", "", "the generator's \"host\" and {host} (default: this host, without domain)")
	fs.IntVar(&c.level, "level", 6, "the generator's \"level\", 0-7")
	fs.IntVar(&c.payloadSize, "payload-size", 0, "pad the generator's documents to this many `bytes` with a random \"_pad\" field")
	fs.StringVar(&c.run, "run", "", "run `id` for {run} (default: 8 random hex characters)")
	fs.BoolVar(&c.loop, "loop", false, "restart --file/--pcap at its end (bounded by --count/--duration)")
	fs.StringVar(&c.pcapPorts, "pcap-port", "", "comma-separated destination `ports` to replay; given, the content check is off")
	fs.StringVar(&c.pcapProto, "pcap-proto", "udp,tcp", "protocols to replay from the capture: `udp,tcp`, udp or tcp (tcp covers HTTP)")
	fs.StringVar(&c.pcapTiming, "pcap-timing", "asap", "`asap` (paced by --rate) or original (the capture's own intervals)")
	fs.Float64Var(&c.pcapSpeed, "pcap-speed", 1, "speed `factor` of --pcap-timing original (2 = twice as fast)")

	fs.StringVar(&c.reportFmt, "report", "text", "final report on stdout: `text` or json")
	fs.StringVar(&c.progress, "progress", "auto", "progress line on stderr every `interval`; 0 = off; auto = 1s on a terminal")
	fs.BoolVar(&c.showVer, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: %s [flags]\n\nSends GELF messages to a server, to test it or load it (docs/SPEC.md).\n\nFlags:\n", Name)
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nExit: 0 all sent (or answered 2xx), 1 some failed, 2 usage, 3 target unreachable, 130 interrupted.\n")
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
	c.set = map[string]bool{}
	fs.Visit(func(f *flag.Flag) { c.set[f.Name] = true })
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
	opt    transport.Options
	plan   sched.Plan
	info   map[string]string
	report string
}

func (c *config) any(names ...string) string {
	for _, n := range names {
		if c.set[n] {
			return n
		}
	}
	return ""
}

var runRE = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

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
	if c.concurrency < 1 || c.pipeline < 1 {
		return nil, errors.New("--concurrency and --pipeline must be >= 1")
	}
	if c.rate < 0 || c.payloadSize < 0 || c.duration < 0 || c.count < -1 || c.retries < 0 {
		return nil, errors.New("--rate, --payload-size, --duration, --count and --retries cannot be negative")
	}
	if c.timeout <= 0 || c.drain <= 0 || c.respTimeout <= 0 || c.retryMaxWait < 0 {
		return nil, errors.New("--timeout, --response-timeout and --drain-timeout must be > 0, --retry-max-wait >= 0")
	}
	if c.progress != "auto" && c.progress != "0" && c.progress != "off" {
		if d, err := time.ParseDuration(c.progress); err != nil || d < 0 {
			return nil, fmt.Errorf("--progress: want auto, off or a duration, got %q", c.progress)
		}
	}
	if c.reportFmt != "text" && c.reportFmt != "json" {
		return nil, fmt.Errorf("--report: want text or json, got %q", c.reportFmt)
	}
	if c.level < 0 || c.level > 7 {
		return nil, fmt.Errorf("--level: want 0-7, got %d", c.level)
	}
	isHTTP := c.network == "http" || c.network == "https"
	if _, ok := DefaultTargets[c.network]; !ok {
		return nil, fmt.Errorf("--transport: want tcp, udp, tls, http or https, got %q", c.network)
	}
	if c.target == "" {
		c.target = DefaultTargets[c.network]
	}
	if _, _, err := net.SplitHostPort(c.target); err != nil {
		return nil, fmt.Errorf("--target: want host:port, got %q", c.target)
	}
	if c.run == "" {
		c.run = tmpl.NewRunID()
	} else if !runRE.MatchString(c.run) {
		// A label: the generator puts it unescaped in a JSON string (SPEC
		// §5.1), and a quote or backslash would make the document invalid.
		return nil, fmt.Errorf("--run: want 1-64 of [A-Za-z0-9._:-], got %q", c.run)
	}
	if c.hostname == "" {
		h, err := os.Hostname()
		if err != nil || h == "" {
			h = "localhost"
		}
		c.hostname = strings.SplitN(h, ".", 2)[0]
	}

	// Transport options.
	o := transport.Options{Network: c.network, Target: c.target, Timeout: c.timeout, ChunkSize: c.chunkSize}
	comp := c.compress
	if comp == "" {
		comp = "none"
		if c.network == "udp" {
			comp = "gzip"
		}
	}
	var err error
	if o.Compress, err = gelf.ParseCompression(comp); err != nil {
		return nil, err
	}
	if (c.network == "tcp" || c.network == "tls") && o.Compress != gelf.None {
		return nil, errors.New("--compress: GELF over TCP/TLS is never compressed (the receiver frames on NUL bytes)")
	}
	if c.network != "udp" && c.set["chunk-size"] {
		return nil, errors.New("--chunk-size applies to --transport udp")
	}
	if c.chunkSize < gelf.MinChunkSize || c.chunkSize > transport.MaxDatagram {
		return nil, fmt.Errorf("--chunk-size: want %d..%d", gelf.MinChunkSize, transport.MaxDatagram)
	}
	if !isHTTP {
		if f := c.any("http-path", "http-host", "pipeline", "requests-per-conn", "retries", "retry-max-wait", "response-timeout"); f != "" {
			return nil, fmt.Errorf("--%s applies to --transport http or https", f)
		}
	} else {
		if !strings.HasPrefix(c.httpPath, "/") || strings.ContainsAny(c.httpPath, " \r\n#") {
			return nil, fmt.Errorf("--http-path: want an absolute path without spaces, got %q", c.httpPath)
		}
		if strings.ContainsAny(c.httpHost, " \r\n") {
			return nil, errors.New("--http-host: no spaces or line breaks")
		}
		o.Path, o.Host = c.httpPath, c.httpHost
		if o.Host == "" {
			o.Host = c.target
		}
		o.ResponseTimeout, o.Pipeline, o.PerConn = c.respTimeout, c.pipeline, c.perConn
		o.Retries, o.RetryMaxWait = c.retries, c.retryMaxWait
	}
	if c.network == "tls" || c.network == "https" {
		if o.TLS, err = tlsConfig(c); err != nil {
			return nil, err
		}
	} else if f := c.any("tls-ca", "tls-cert", "tls-key", "tls-insecure", "tls-server-name", "tls-min-version"); f != "" {
		return nil, fmt.Errorf("--%s needs --transport tls or https", f)
	}
	switch c.proxy {
	case "off":
		if c.proxySrc != "" || c.proxyDst != "" {
			return nil, errors.New("--proxy-src/--proxy-dst need --proxy-protocol v1 or v2")
		}
	case "v1", "v2":
		if c.network == "udp" {
			return nil, errors.New("--proxy-protocol needs a stream transport: tcp, tls, http or https")
		}
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

	// Source.
	finite := c.file != "" || c.pcapFile != ""
	if c.payloadSize > 0 && (finite || c.template != "") {
		return nil, errors.New("--payload-size pads the generator's documents only, not --template/--file/--pcap")
	}
	if c.set["level"] && (finite || c.template != "") {
		return nil, errors.New("--level sets the generator's level only")
	}
	if c.loop && !finite {
		return nil, errors.New("--loop applies to --file and --pcap")
	}
	if c.pcapFile == "" {
		if f := c.any("pcap-port", "pcap-proto", "pcap-speed"); f != "" {
			return nil, fmt.Errorf("--%s needs --pcap", f)
		}
	}
	r := &runner{c: c, report: c.reportFmt, info: map[string]string{}}
	switch {
	case c.pcapFile != "":
		opt := source.PcapOptions{Loop: c.loop}
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
		r.src, err = source.NewFile(c.file, c.loop)
		r.info["source"] = "file " + c.file
	default:
		body := source.GenBody(c.hostname, c.level)
		r.info["source"] = "generator"
		if c.template != "" {
			body = c.template
			r.info["source"] = "template"
		}
		var t *tmpl.Template
		if t, err = tmpl.Compile(body); err == nil {
			r.src = &source.Template{T: t, Run: c.run, Host: c.hostname, PID: os.Getpid(), Size: c.payloadSize}
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
		// whole --file/--pcap, or --duration (SPEC §7; before 2026-10-31
		// the default of 1 also applied with --duration, in all injectors).
		count = 1
		if finite || c.duration > 0 {
			count = 0
		}
	}
	r.plan.Count = uint64(count)
	r.plan.Duration, r.plan.Rate, r.plan.Workers, r.plan.Drain = c.duration, c.rate, c.concurrency, c.drain
	r.opt = o

	r.info["injector"] = Name + " " + version
	r.info["run"] = c.run
	r.info["target"] = c.network + "://" + c.target
	if isHTTP {
		r.info["target"] += c.httpPath
		r.info["protocol"] = "http/1.1 keep-alive, pipeline " + strconv.Itoa(c.pipeline)
		if c.perConn > 0 {
			r.info["protocol"] += ", " + strconv.FormatUint(c.perConn, 10) + " requests per connection"
		}
	}
	if c.network == "udp" || isHTTP {
		r.info["compression"] = o.Compress.String()
	}
	if c.network == "udp" {
		r.info["chunk_size"] = strconv.Itoa(c.chunkSize)
	}
	if o.Proxy != nil {
		r.info["proxy_protocol"] = c.proxy
	}
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
	rep.OKOutcomes = []string{transport.Sent, "200", "201", "202", "204"}
	r.opt.OnDial = rep.Dial
	dial, err := transport.Dialer(r.opt)
	if err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", Name, err)
		return ExitUsage
	}
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
	st := sched.Run(ctx, r.plan, r.src, dial, rep)
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
	d, _ := time.ParseDuration(r.c.progress) // checked in build
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
		host, _, _ := net.SplitHostPort(c.target)
		tc.ServerName = host
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
