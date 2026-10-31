// Package app is the command line of go-webhook-injector (SPEC §3): flags,
// validation, wiring of source, request builder, transport and scheduler,
// signals, report and exit code.
package app

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/report"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/sched"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/source"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/tmpl"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/transport"
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
const Name = "go-webhook-injector"

// multi is a repeatable string flag.
type multi []string

func (m *multi) String() string     { return strings.Join(*m, ", ") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

type config struct {
	url, method, host, contentType string
	headers                        multi
	http2                          bool
	pipeline, streams              int
	perConn                        uint64
	retries                        int
	retryMaxWait                   time.Duration
	timeout, respTimeout           time.Duration
	tlsCA, tlsCert, tlsKey         string
	tlsServerName, tlsMin          string
	tlsInsecure                    bool
	uuidOut                        string

	count       int64
	duration    time.Duration
	rate        float64
	concurrency int
	drain       time.Duration

	template, file, pcapFile string
	size                     int
	run                      string
	loop                     bool
	pcapPorts                string
	pcapTiming               string
	pcapSpeed                float64
	pcapPath, pcapHost       string
	pcapXFF                  bool

	reportFmt string
	progress  string
	showVer   bool
}

func newFlags(c *config, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.url, "url", "http://127.0.0.1:8080/", "target `URL`, http:// or https://; the path and query take markers {seq} {run} {uuid} ...")
	fs.StringVar(&c.method, "method", "POST", "request `method` (uppercase)")
	fs.Var(&c.headers, "header", "extra request `header` \"Name: value\" (repeatable; the value takes markers)")
	fs.StringVar(&c.host, "host", "", "Host header (default: the host of --url)")
	fs.StringVar(&c.contentType, "content-type", "application/json", "Content-Type of generated, template and file bodies (empty: none)")
	fs.BoolVar(&c.http2, "http2", false, "HTTP/2: h2c by prior knowledge on http://, ALPN h2 on https://")
	fs.IntVar(&c.pipeline, "pipeline", 1, "HTTP/1.1 requests in flight per connection (pipelining when > 1)")
	fs.IntVar(&c.streams, "streams", 100, "HTTP/2 requests in flight per connection")
	fs.Uint64Var(&c.perConn, "requests-per-conn", 0, "HTTP/1.1: close and reopen the connection after this many `requests` (0 = keep it)")
	fs.IntVar(&c.retries, "retries", 0, "resend a request answered 429 or 503 up to this many `times`, after its Retry-After")
	fs.DurationVar(&c.retryMaxWait, "retry-max-wait", 5*time.Second, "longest wait before a resend, whatever Retry-After says")
	fs.DurationVar(&c.timeout, "timeout", 10*time.Second, "dial, TLS handshake and write timeout")
	fs.DurationVar(&c.respTimeout, "response-timeout", 30*time.Second, "wait for each response at most this long")
	fs.StringVar(&c.tlsCA, "tls-ca", "", "PEM `file` of the CAs that verify the server (default: system roots)")
	fs.StringVar(&c.tlsCert, "tls-cert", "", "client certificate PEM `file` (mTLS)")
	fs.StringVar(&c.tlsKey, "tls-key", "", "client key PEM `file` (mTLS)")
	fs.StringVar(&c.tlsServerName, "tls-server-name", "", "name verified in the server certificate (default: the host of --url)")
	fs.StringVar(&c.tlsMin, "tls-min-version", "1.2", "minimum TLS `version`: 1.2 or 1.3")
	fs.BoolVar(&c.tlsInsecure, "tls-insecure", false, "do not verify the server certificate")
	fs.StringVar(&c.uuidOut, "uuid-out", "", "write \"seq status X-LogHub-UUID\" of every final response to this `file`")

	fs.Int64Var(&c.count, "count", -1, "requests to send; 0 = no limit (default: 1; no limit with --duration; the whole --file/--pcap)")
	fs.DurationVar(&c.duration, "duration", 0, "stop after this `time` (0 = no limit)")
	fs.Float64Var(&c.rate, "rate", 0, "target `req/s` over all connections (0 = as fast as possible)")
	fs.IntVar(&c.concurrency, "concurrency", 1, "parallel connections")
	fs.DurationVar(&c.drain, "drain-timeout", 10*time.Second, "at the end, wait at most this long for responses and retries")

	fs.StringVar(&c.template, "template", "", "body `template` with markers {seq} {run} {uuid} {ts} {unix} {unixms} {host} {pid} {rand:N}")
	fs.StringVar(&c.file, "file", "", "send one body per line of this `file` (- = stdin), or one per file of a directory")
	fs.StringVar(&c.pcapFile, "pcap", "", "replay the HTTP/1.x requests of this tcpdump capture `file` (pcap or pcapng)")
	fs.IntVar(&c.size, "size", 0, "pad generated JSON bodies to this many `bytes`")
	fs.StringVar(&c.run, "run", "", "run `id` for {run} (default: 8 random hex characters)")
	fs.BoolVar(&c.loop, "loop", false, "restart --file/--pcap at its end (bounded by --count/--duration)")
	fs.StringVar(&c.pcapPorts, "pcap-port", "", "comma-separated destination `ports` to replay (default: any HTTP/1.x)")
	fs.StringVar(&c.pcapTiming, "pcap-timing", "asap", "`asap` (paced by --rate) or original (the capture's own intervals)")
	fs.Float64Var(&c.pcapSpeed, "pcap-speed", 1, "speed `factor` of --pcap-timing original (2 = twice as fast)")
	fs.StringVar(&c.pcapPath, "pcap-path", "keep", "request target of a replayed request: `keep` the captured one, or url (the path of --url)")
	fs.StringVar(&c.pcapHost, "pcap-host", "keep", "Host of a replayed request: `keep` the captured one, or url")
	fs.BoolVar(&c.pcapXFF, "pcap-xff", false, "add the captured client address to X-Forwarded-For (the server honors it only from a trusted proxy)")

	fs.StringVar(&c.reportFmt, "report", "text", "final report on stdout: `text` or json")
	fs.StringVar(&c.progress, "progress", "auto", "progress line on stderr every `interval`; 0 = off; auto = 1s on a terminal")
	fs.BoolVar(&c.showVer, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: %s [flags]\n\nSends HTTP requests to a webhook server, to test it or load it (docs/SPEC.md).\n\nFlags:\n", Name)
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nExit: 0 all answered 2xx, 1 some failed, 2 usage, 3 target unreachable, 130 interrupted.\n")
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
	opt    transport.Options
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
	if c.concurrency < 1 || c.pipeline < 1 || c.streams < 1 {
		return nil, errors.New("--concurrency, --pipeline and --streams must be >= 1")
	}
	if c.rate < 0 || c.size < 0 || c.duration < 0 || c.count < -1 || c.retries < 0 {
		return nil, errors.New("--rate, --size, --duration, --count and --retries cannot be negative")
	}
	if c.timeout <= 0 || c.drain <= 0 || c.respTimeout <= 0 || c.retryMaxWait < 0 {
		return nil, errors.New("--timeout, --response-timeout and --drain-timeout must be > 0")
	}
	if c.reportFmt != "text" && c.reportFmt != "json" {
		return nil, fmt.Errorf("--report: want text or json, got %q", c.reportFmt)
	}
	if c.http2 && (c.pipeline != 1 || c.perConn != 0) {
		return nil, errors.New("--pipeline and --requests-per-conn are HTTP/1.1; with --http2 use --streams")
	}
	if !validMethod(c.method) {
		return nil, fmt.Errorf("--method: want an uppercase token, got %q", c.method)
	}
	if c.method == "CONNECT" {
		return nil, errors.New("--method CONNECT: the webhook servers refuse it (405) and it is not a request with a target")
	}
	if c.run == "" {
		c.run = tmpl.NewRunID()
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}
	hostname = strings.SplitN(hostname, ".", 2)[0]

	u, pathT, err := parseURL(c.url)
	if err != nil {
		return nil, err
	}
	b := &builder{method: c.method, path: pathT, host: u.Host, run: c.run, hostname: hostname,
		pid: os.Getpid(), pcapPath: c.pcapPath == "url", pcapHost: c.pcapHost == "url", xff: c.pcapXFF}
	if c.host != "" {
		if strings.ContainsAny(c.host, "\r\n ") {
			return nil, errors.New("--host: no spaces or line breaks")
		}
		b.host = c.host
	}
	for _, h := range c.headers {
		hd, err := parseHeader(h)
		if err != nil {
			return nil, err
		}
		b.extra = append(b.extra, hd)
	}
	switch c.pcapPath {
	case "keep", "url":
	default:
		return nil, fmt.Errorf("--pcap-path: want keep or url, got %q", c.pcapPath)
	}
	switch c.pcapHost {
	case "keep", "url":
	default:
		return nil, fmt.Errorf("--pcap-host: want keep or url, got %q", c.pcapHost)
	}

	finite := c.file != "" || c.pcapFile != ""
	if c.size > 0 && (finite || c.template != "") {
		return nil, errors.New("--size pads the generator's JSON only, not --template/--file/--pcap")
	}
	if c.loop && !finite {
		return nil, errors.New("--loop applies to --file and --pcap")
	}
	if c.pcapFile == "" && (c.pcapPorts != "" || c.pcapXFF || c.pcapPath != "keep" || c.pcapHost != "keep") {
		return nil, errors.New("--pcap-port, --pcap-path, --pcap-host and --pcap-xff need --pcap")
	}
	if c.pcapFile == "" && c.contentType != "" {
		if strings.ContainsAny(c.contentType, "\r\n") {
			return nil, errors.New("--content-type: no line breaks")
		}
		if !b.hasExtra("Content-Type") {
			b.ctype = c.contentType
		}
	}

	r := &runner{c: c, report: c.reportFmt, info: map[string]string{}}
	switch {
	case c.pcapFile != "":
		opt := source.PcapOptions{Loop: c.loop}
		if opt.Ports, err = ports(c.pcapPorts); err != nil {
			return nil, err
		}
		r.src, err = source.NewPcap(c.pcapFile, opt)
		r.info["source"] = "pcap " + c.pcapFile
	case c.file != "":
		r.src, err = source.NewFile(c.file, c.loop)
		r.info["source"] = "file " + c.file
	default:
		body := source.GenBody
		r.info["source"] = "generator"
		size := c.size
		if c.template != "" {
			body = c.template
			r.info["source"] = "template"
		}
		var t *tmpl.Template
		if t, err = tmpl.Compile(body); err == nil {
			r.src = &source.Template{T: t, Run: c.run, Host: hostname, PID: os.Getpid(), Size: size}
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

	r.opt = transport.Options{URL: u, Timeout: c.timeout, ResponseTimeout: c.respTimeout, HTTP2: c.http2,
		Pipeline: c.pipeline, Streams: c.streams, PerConn: c.perConn, Retries: c.retries,
		RetryMaxWait: c.retryMaxWait, Build: b.build}
	if u.Scheme == "https" {
		if r.opt.TLS, err = tlsConfig(c, u.Hostname()); err != nil {
			return nil, err
		}
	} else if c.tlsCA != "" || c.tlsCert != "" || c.tlsKey != "" || c.tlsInsecure || c.tlsServerName != "" {
		return nil, errors.New("--tls-* flags need an https:// --url")
	}
	r.info["injector"] = Name + " " + version
	r.info["run"] = c.run
	r.info["target"] = u.Scheme + "://" + u.Host
	switch {
	case c.http2 && u.Scheme == "https":
		r.info["protocol"] = "h2 (ALPN), streams " + strconv.Itoa(c.streams)
	case c.http2:
		r.info["protocol"] = "h2c (prior knowledge), streams " + strconv.Itoa(c.streams)
	default:
		r.info["protocol"] = "http/1.1 keep-alive, pipeline " + strconv.Itoa(c.pipeline)
		if c.perConn > 0 {
			r.info["protocol"] += ", " + strconv.FormatUint(c.perConn, 10) + " requests per connection"
		}
	}
	r.info["plan"] = planText(r.plan)
	return r, nil
}

// parseURL splits --url into the dialed part and the request-target template.
// The target is split by hand: url.Parse would escape the braces of markers.
func parseURL(s string) (*url.URL, *tmpl.Template, error) {
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok || (scheme != "http" && scheme != "https") {
		return nil, nil, fmt.Errorf("--url: want http:// or https://, got %q", s)
	}
	authority, target := rest, "/"
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		authority, target = rest[:i], rest[i:]
		if target[0] == '?' {
			target = "/" + target
		}
	}
	if strings.ContainsAny(target, " \r\n#") {
		return nil, nil, errors.New("--url: the path cannot have spaces, line breaks or a fragment")
	}
	u, err := url.Parse(scheme + "://" + authority)
	if err != nil || u.Hostname() == "" || u.User != nil {
		return nil, nil, fmt.Errorf("--url: bad host %q", authority)
	}
	t, err := tmpl.Compile(target)
	if err != nil {
		return nil, nil, fmt.Errorf("--url: %w", err)
	}
	return u, t, nil
}

func validMethod(m string) bool {
	if m == "" {
		return false
	}
	for i := 0; i < len(m); i++ {
		if m[i] < 'A' || m[i] > 'Z' {
			return false
		}
	}
	return true
}

type header struct {
	name   string
	value  *tmpl.Template
	static string
}

// framing headers are the transport's; a user value would desynchronize the
// connection or lie about the body.
var framing = map[string]bool{"Host": true, "Content-Length": true, "Transfer-Encoding": true,
	"Connection": true, "Keep-Alive": true, "Upgrade": true, "Te": true, "Trailer": true,
	"Expect": true, "Proxy-Connection": true, "Http2-Settings": true}

func parseHeader(s string) (header, error) {
	name, value, ok := strings.Cut(s, ":")
	name, value = strings.TrimSpace(name), strings.TrimSpace(value)
	if !ok || name == "" {
		return header{}, fmt.Errorf("--header: want \"Name: value\", got %q", s)
	}
	for i := 0; i < len(name); i++ {
		if !tokenChar(name[i]) {
			return header{}, fmt.Errorf("--header: bad name %q", name)
		}
	}
	if strings.ContainsAny(value, "\r\n") {
		return header{}, fmt.Errorf("--header %s: no line breaks in the value", name)
	}
	if framing[canonical(name)] {
		return header{}, fmt.Errorf("--header %s: set by the injector (use --host for Host)", name)
	}
	t, err := tmpl.Compile(value)
	if err != nil {
		return header{}, fmt.Errorf("--header %s: %w", name, err)
	}
	h := header{name: name, value: t}
	if t.Static() {
		h.static = string(t.Expand(nil, &tmpl.Ctx{}))
	}
	return h, nil
}

func tokenChar(c byte) bool {
	return c > ' ' && c < 0x7f && !strings.ContainsRune("()<>@,;:\\\"/[]?={}", rune(c))
}

func canonical(name string) string {
	b := []byte(strings.ToLower(name))
	up := true
	for i, c := range b {
		if up && c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
		up = c == '-'
	}
	return string(b)
}

// builder turns a message into a request (SPEC §5.5).
type builder struct {
	method   string
	path     *tmpl.Template
	host     string
	ctype    string
	extra    []header
	run      string
	hostname string
	pid      int
	pcapPath bool // replay with the path of --url
	pcapHost bool // replay with the host of --url
	xff      bool
}

func (b *builder) hasExtra(name string) bool {
	for _, h := range b.extra {
		if strings.EqualFold(h.name, name) {
			return true
		}
	}
	return false
}

func (b *builder) build(seq uint64, m *source.Message) transport.Req {
	ctx := tmpl.Ctx{Seq: seq, Run: b.run, Host: b.hostname, PID: b.pid, Now: time.Now()}
	r := transport.Req{Method: b.method, Host: b.host, Body: m.Body}
	if m.Req == nil || b.pcapPath {
		r.URI = string(b.path.Expand(make([]byte, 0, 64), &ctx))
	}
	if m.Req != nil {
		r.Method = m.Req.Method
		if !b.pcapPath {
			r.URI = originForm(m.Req.URI)
		}
		if h := m.Req.Header.Get("Host"); h != "" && !b.pcapHost {
			r.Host = h
		}
		keys := make([]string, 0, len(m.Req.Header))
		for k := range m.Req.Header {
			if !framing[k] {
				keys = append(keys, k)
			}
		}
		// Header order of the capture is lost in net/http's map; sorted, a
		// replay is at least the same every time.
		sort.Strings(keys)
		for _, k := range keys {
			if b.xff && k == "X-Forwarded-For" {
				continue
			}
			for _, v := range m.Req.Header[k] {
				r.Header = append(r.Header, [2]string{k, v})
			}
		}
		if b.xff && m.Src.IsValid() {
			v := m.Src.Addr().Unmap().String()
			if prev := m.Req.Header.Values("X-Forwarded-For"); len(prev) > 0 {
				v = strings.Join(prev, ", ") + ", " + v
			}
			r.Header = append(r.Header, [2]string{"X-Forwarded-For", v})
		}
	} else if b.ctype != "" {
		r.Header = append(r.Header, [2]string{"Content-Type", b.ctype})
	}
	for _, h := range b.extra {
		v := h.static
		if h.value != nil && !h.value.Static() {
			v = string(h.value.Expand(make([]byte, 0, 64), &ctx))
		}
		r.Header = append(r.Header, [2]string{h.name, v})
	}
	return r
}

// originForm turns an absolute-form target (a request to a proxy) into the
// path and query a server expects.
func originForm(uri string) string {
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		if u, err := url.Parse(uri); err == nil {
			return u.RequestURI()
		}
	}
	if uri == "" || uri[0] != '/' {
		return "/" + uri
	}
	return uri
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

// uuidLog writes --uuid-out lines from every connection's reader.
type uuidLog struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func (l *uuidLog) add(seq uint64, status int, uuid string) {
	if uuid == "" {
		uuid = "-"
	}
	l.mu.Lock()
	l.w.WriteString(strconv.FormatUint(seq, 10))
	l.w.WriteByte(' ')
	l.w.WriteString(strconv.Itoa(status))
	l.w.WriteByte(' ')
	l.w.WriteString(uuid)
	l.w.WriteByte('\n')
	l.mu.Unlock()
}

func (l *uuidLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.w.Flush()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (r *runner) run(stdout, stderr io.Writer) int {
	rep := report.New()
	rep.Info = r.info
	rep.OKOutcomes = []string{"200", "201", "202", "204"}
	r.opt.OnDial = rep.Dial
	var ul *uuidLog
	if r.c.uuidOut != "" {
		f, err := os.Create(r.c.uuidOut)
		if err != nil {
			fmt.Fprintf(stderr, "%s: --uuid-out: %v\n", Name, err)
			return ExitUsage
		}
		ul = &uuidLog{f: f, w: bufio.NewWriterSize(f, 256<<10)}
		r.opt.OnResponse = ul.add
	}
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
			if ul != nil {
				_ = ul.close()
			}
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
		rep.Event(fmt.Sprintf("behind_schedule (>100ms late): %d requests", st.Behind))
	}
	if st.SourceErr != nil {
		rep.Event("source_error: " + st.SourceErr.Error())
	}
	if ul != nil {
		if err := ul.close(); err != nil {
			rep.Event("uuid_out_error: " + err.Error())
		}
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

func tlsConfig(c *config, host string) (*tls.Config, error) {
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
