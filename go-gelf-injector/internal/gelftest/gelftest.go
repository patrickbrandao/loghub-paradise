// Package gelftest holds strict in-process GELF receivers for the tests
// (TESTING §1): UDP (chunk reassembly by source and id, 5 s deadline,
// count 1..128, gzip/zlib detection by the first bytes), TCP and TLS
// (NUL framing, the last document accepted at EOF), and HTTP (POST /gelf,
// Content-Encoding, 202 / 503 + Retry-After / 413 / 405 / 404 / 400), each
// optionally behind a PROXY v1/v2 header.
//
// It is written apart from the injector's own gelf and extract packages on
// purpose: a test that used the sender's code to read what it sent could
// not catch a wire rule both sides got wrong.
package gelftest

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MaxDoc is the receivers' ceiling on a decompressed document, the servers'
// 256 KiB (go-gelf-server DESIGN decision 10).
var MaxDoc = 256 << 10

// Collector keeps what a receiver accepted.
type Collector struct {
	mu    sync.Mutex
	docs  [][]byte
	n     atomic.Int64
	Stats sync.Map // counter name -> *atomic.Int64
	// Proxied records the source announced by each PROXY header.
	proxied []string
}

func (c *Collector) add(b []byte) {
	d := append([]byte(nil), b...)
	c.mu.Lock()
	c.docs = append(c.docs, d)
	c.mu.Unlock()
	c.n.Add(1)
}

// Count increments a named counter.
func (c *Collector) Count(name string) {
	v, _ := c.Stats.LoadOrStore(name, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// Counter reads a named counter.
func (c *Collector) Counter(name string) int64 {
	if v, ok := c.Stats.Load(name); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// N is the number of documents accepted.
func (c *Collector) N() int { return int(c.n.Load()) }

// Docs returns a copy of the list of documents.
func (c *Collector) Docs() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.docs...)
}

// Proxied returns the PROXY sources seen.
func (c *Collector) Proxied() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.proxied...)
}

// Wait waits until n documents arrived or d passed; it reports success.
func (c *Collector) Wait(n int, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if c.N() >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return c.N() >= n
}

// Settle waits until no document arrived for quiet (or max passed).
func (c *Collector) Settle(quiet, maxWait time.Duration) {
	end := time.Now().Add(maxWait)
	last, at := c.N(), time.Now()
	for time.Now().Before(end) {
		time.Sleep(10 * time.Millisecond)
		if n := c.N(); n != last {
			last, at = n, time.Now()
		} else if time.Since(at) >= quiet {
			return
		}
	}
}

// Gen is the generator's document as the tests check it.
type Gen struct {
	Version      string          `json:"version"`
	Host         string          `json:"host"`
	ShortMessage string          `json:"short_message"`
	Level        json.RawMessage `json:"level"`
	Seq          uint64          `json:"_seq"`
	Run          string          `json:"_run"`
	Pad          string          `json:"_pad"`
}

// CheckSeqs verifies that docs hold exactly the generator documents 1..n of
// run, each once, valid GELF 1.1.
func CheckSeqs(docs [][]byte, run string, n int) error {
	seen := make([]int, n+1)
	for _, d := range docs {
		var g Gen
		if err := json.Unmarshal(d, &g); err != nil {
			return fmt.Errorf("not JSON: %v: %.80q", err, d)
		}
		if g.Version != "1.1" || g.Host == "" || g.Run != run || string(g.Level) == "" {
			return fmt.Errorf("bad document: %.120q", d)
		}
		want := "seq=" + strconv.FormatUint(g.Seq, 10) + " run=" + run + " injector=go-gelf-injector"
		if g.ShortMessage != want {
			return fmt.Errorf("short_message %q, want %q", g.ShortMessage, want)
		}
		if g.Seq < 1 || g.Seq > uint64(n) {
			return fmt.Errorf("seq %d out of 1..%d", g.Seq, n)
		}
		seen[g.Seq]++
	}
	var missing, dup int
	for i := 1; i <= n; i++ {
		switch {
		case seen[i] == 0:
			missing++
		case seen[i] > 1:
			dup += seen[i] - 1
		}
	}
	if missing+dup > 0 {
		return fmt.Errorf("%d documents: %d missing, %d duplicated", len(docs), missing, dup)
	}
	return nil
}

// CheckTail verifies the documents of a run caught in the middle: valid,
// distinct, and their seqs contiguous from the first seen to the last.
func CheckTail(docs [][]byte, run string) error {
	seen := map[uint64]bool{}
	var lo, hi uint64
	for _, d := range docs {
		var g Gen
		if err := json.Unmarshal(d, &g); err != nil || g.Run != run || g.Version != "1.1" {
			return fmt.Errorf("bad document %.100q", d)
		}
		if seen[g.Seq] {
			return fmt.Errorf("seq %d duplicated", g.Seq)
		}
		seen[g.Seq] = true
		if lo == 0 || g.Seq < lo {
			lo = g.Seq
		}
		hi = max(hi, g.Seq)
	}
	if len(docs) == 0 || uint64(len(seen)) != hi-lo+1 {
		return fmt.Errorf("%d documents for seqs %d..%d: not contiguous", len(docs), lo, hi)
	}
	return nil
}

// decode inflates a payload by its first bytes, as a GELF receiver must.
func decode(b []byte, enc string) ([]byte, error) {
	var r io.Reader
	switch {
	case enc == "gzip" || enc == "" && len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b:
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		r = zr
	case enc == "deflate" || enc == "" && len(b) >= 2 && b[0] == 0x78 && (uint16(b[0])<<8|uint16(b[1]))%31 == 0:
		zr, err := zlib.NewReader(bytes.NewReader(b))
		if err != nil {
			if enc != "deflate" {
				return nil, err
			}
			r = flate.NewReader(bytes.NewReader(b))
		} else {
			r = zr
		}
	case enc == "" || enc == "identity":
		r = bytes.NewReader(b)
	default:
		return nil, fmt.Errorf("unknown encoding %q", enc)
	}
	out, err := io.ReadAll(io.LimitReader(r, int64(MaxDoc)+1))
	if err != nil {
		return nil, err
	}
	if len(out) > MaxDoc {
		return nil, errTooLarge
	}
	return out, nil
}

var errTooLarge = errors.New("too large")

// UDP is a strict GELF UDP receiver.
type UDP struct {
	C    *Collector
	Addr string
	conn *net.UDPConn
	mu   sync.Mutex
	open map[string]*partial
	stop chan struct{}
}

type partial struct {
	first time.Time
	count int
	parts [][]byte
	have  int
}

// ListenUDP starts a receiver on 127.0.0.1 (port 0 = any).
func ListenUDP(addr string) (*UDP, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	_ = c.SetReadBuffer(8 << 20)
	u := &UDP{C: &Collector{}, Addr: c.LocalAddr().String(), conn: c, open: map[string]*partial{}, stop: make(chan struct{})}
	go u.loop()
	go u.sweep()
	return u, nil
}

// Close stops the receiver.
func (u *UDP) Close() { close(u.stop); u.conn.Close() }

func (u *UDP) sweep() {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-u.stop:
			return
		case now := <-t.C:
			u.mu.Lock()
			for k, p := range u.open {
				if now.Sub(p.first) > 5*time.Second {
					delete(u.open, k)
					u.C.Count("incomplete")
				}
			}
			u.mu.Unlock()
		}
	}
}

type dgram struct {
	src string
	b   []byte
}

// loop only reads: decompression runs in workers, or the socket buffer
// overflows under load while one goroutine inflates gzip.
func (u *UDP) loop() {
	ch := make(chan dgram, 1<<16)
	defer close(ch)
	for range 4 {
		go func() {
			for d := range ch {
				if len(d.b) >= 2 && d.b[0] == 0x1e && d.b[1] == 0x0f {
					u.chunk(d.src, d.b)
					continue
				}
				u.deliver(d.b)
			}
		}()
	}
	buf := make([]byte, 65536)
	for {
		n, src, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		ch <- dgram{src: src.String(), b: append([]byte(nil), buf[:n]...)}
	}
}

func (u *UDP) chunk(src string, b []byte) {
	u.C.Count("chunks")
	if len(b) < 12 {
		u.C.Count("invalid")
		return
	}
	seq, count := int(b[10]), int(b[11])
	if count < 1 || count > 128 || seq >= count {
		u.C.Count("invalid")
		return
	}
	key := src + "/" + string(b[2:10])
	u.mu.Lock()
	p := u.open[key]
	if p == nil {
		p = &partial{first: time.Now(), count: count, parts: make([][]byte, count)}
		u.open[key] = p
	}
	if p.count != count {
		u.mu.Unlock()
		u.C.Count("invalid")
		return
	}
	if p.parts[seq] != nil {
		u.mu.Unlock()
		u.C.Count("duplicate")
		return
	}
	p.parts[seq] = b[12:] // b is the worker's own copy
	p.have++
	if p.have < p.count {
		u.mu.Unlock()
		return
	}
	delete(u.open, key)
	u.mu.Unlock()
	u.C.Count("chunked")
	u.C.Count("chunks_" + strconv.Itoa(count))
	u.deliver(bytes.Join(p.parts, nil))
}

func (u *UDP) deliver(b []byte) {
	d, err := decode(b, "")
	if err != nil {
		u.C.Count("bad_payload")
		return
	}
	if len(b) > 0 && b[0] == 0x1f {
		u.C.Count("gzip")
	} else if len(b) > 0 && b[0] == 0x78 {
		u.C.Count("zlib")
	}
	d = bytes.TrimSpace(d)
	if len(d) == 0 || d[0] != '{' {
		u.C.Count("not_json_object")
	}
	u.C.add(d)
}

// Stream is a strict GELF TCP (or TLS) receiver.
type Stream struct {
	C     *Collector
	Addr  string
	CA    string // PEM file of the CA, for TLS
	ln    net.Listener
	Proxy bool // a PROXY header is required on every connection
	// CloseAfter, when > 0, makes the receiver close each connection after
	// that many documents (a server going away).
	CloseAfter int
	conns      atomic.Int64
}

// ListenTCP starts a TCP receiver; withTLS wraps it in TLS with a fresh CA
// written to dir.
func ListenTCP(withTLS bool, dir string, proxy bool) (*Stream, error) {
	return listenTCP("127.0.0.1:0", withTLS, dir, proxy)
}

func listenTCPAt(addr string) (*Stream, error) { return listenTCP(addr, false, "", false) }

func listenTCP(addr string, withTLS bool, dir string, proxy bool) (*Stream, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Stream{C: &Collector{}, Addr: ln.Addr().String(), Proxy: proxy}
	var cfg *tls.Config
	if withTLS {
		if cfg, s.CA, err = selfSigned(dir); err != nil {
			ln.Close()
			return nil, err
		}
	}
	s.ln = ln
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go s.serve(c, cfg)
		}
	}()
	return s, nil
}

// Conns is the number of connections accepted.
func (s *Stream) Conns() int { return int(s.conns.Load()) }

// Close stops the receiver.
func (s *Stream) Close() { s.ln.Close() }

func (s *Stream) serve(c net.Conn, cfg *tls.Config) {
	defer c.Close()
	br := bufio.NewReaderSize(c, 64<<10)
	if s.Proxy {
		src, err := readProxy(br)
		if err != nil {
			s.C.Count("proxy_error")
			return
		}
		s.C.mu.Lock()
		s.C.proxied = append(s.C.proxied, src)
		s.C.mu.Unlock()
	}
	if cfg != nil {
		tc := tls.Server(&bufConn{Conn: c, r: br}, cfg)
		if err := tc.Handshake(); err != nil {
			s.C.Count("tls_error")
			return
		}
		br = bufio.NewReaderSize(tc, 64<<10)
	}
	n := 0
	for {
		frame, err := br.ReadBytes(0)
		if err == nil {
			frame = frame[:len(frame)-1]
		}
		if f := bytes.TrimSpace(frame); len(f) > 0 {
			if len(f) > MaxDoc {
				s.C.Count("too_large")
			} else {
				s.C.add(f)
				n++
			}
		}
		if err != nil {
			return // EOF: the last document without NUL was taken above
		}
		if s.CloseAfter > 0 && n >= s.CloseAfter {
			return
		}
	}
}

type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (b *bufConn) Read(p []byte) (int, error) { return b.r.Read(p) }

var proxyV2Sig = []byte("\r\n\r\n\x00\r\nQUIT\n")

// readProxy reads a PROXY v1 or v2 header and returns the announced source.
func readProxy(br *bufio.Reader) (string, error) {
	sig, err := br.Peek(12)
	if err != nil {
		return "", err
	}
	if bytes.Equal(sig, proxyV2Sig) {
		h := make([]byte, 16)
		if _, err := io.ReadFull(br, h); err != nil {
			return "", err
		}
		if h[12] != 0x21 {
			return "", errors.New("proxy v2: not PROXY command")
		}
		body := make([]byte, binary.BigEndian.Uint16(h[14:]))
		if _, err := io.ReadFull(br, body); err != nil {
			return "", err
		}
		switch h[13] {
		case 0x11:
			if len(body) < 12 {
				return "", errors.New("proxy v2: short")
			}
			ip := net.IP(body[0:4])
			return net.JoinHostPort(ip.String(), strconv.Itoa(int(binary.BigEndian.Uint16(body[8:])))), nil
		case 0x21:
			if len(body) < 36 {
				return "", errors.New("proxy v2: short")
			}
			ip := net.IP(body[0:16])
			return net.JoinHostPort(ip.String(), strconv.Itoa(int(binary.BigEndian.Uint16(body[32:])))), nil
		}
		return "", errors.New("proxy v2: family")
	}
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	f := strings.Fields(strings.TrimSpace(line))
	if len(f) != 6 || f[0] != "PROXY" || (f[1] != "TCP4" && f[1] != "TCP6") {
		return "", fmt.Errorf("proxy v1: %q", line)
	}
	return net.JoinHostPort(f[2], f[4]), nil
}

// selfSigned writes a CA-signed-by-itself certificate for 127.0.0.1 and
// localhost to dir and returns the server config and the PEM path.
func selfSigned(dir string) (*tls.Config, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "gelftest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, "", err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, "", err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, "", err
	}
	p := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(p, certPEM, 0o600); err != nil {
		return nil, "", err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, p, nil
}

// HTTP is a strict GELF HTTP receiver (the Graylog HTTP input, with the
// LogHub answers of DESIGN decision 13).
type HTTP struct {
	C    *Collector
	Addr string
	CA   string
	srv  *http.Server
	// Busy, when set, is asked before each request: true answers 503 with
	// Retry-After: 0 (the ring full).
	Busy func() bool
	// Status overrides the success status (202 by default).
	Status int
	// MaxPerConn, when > 0, answers the N-th request of each connection with
	// "Connection: close" (a keep-alive server with a per-connection cap; 1
	// is a server without keep-alive). Set before the first request.
	MaxPerConn int
}

type connCount struct{}

// ListenHTTP starts the receiver; withTLS serves HTTPS with a fresh CA in
// dir; proxy requires a PROXY header on every connection.
func ListenHTTP(withTLS bool, dir string, proxy bool) (*HTTP, error) {
	return listenHTTP("127.0.0.1:0", withTLS, dir, proxy)
}

func listenHTTPAt(addr string) (*HTTP, error) { return listenHTTP(addr, false, "", false) }

func listenHTTP(addr string, withTLS bool, dir string, proxy bool) (*HTTP, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	h := &HTTP{C: &Collector{}, Addr: ln.Addr().String(), Status: http.StatusAccepted}
	if proxy {
		ln = &proxyListener{Listener: ln, c: h.C}
	}
	h.srv = &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second,
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return context.WithValue(ctx, connCount{}, new(atomic.Int64))
		}}
	if withTLS {
		var cfg *tls.Config
		if cfg, h.CA, err = selfSigned(dir); err != nil {
			ln.Close()
			return nil, err
		}
		cfg.NextProtos = []string{"http/1.1"}
		ln = tls.NewListener(ln, cfg)
	}
	go h.srv.Serve(ln)
	return h, nil
}

// Close stops the receiver.
func (h *HTTP) Close() { h.srv.Close() }

// ServeHTTP implements http.Handler.
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.C.Count("requests")
	if n, ok := r.Context().Value(connCount{}).(*atomic.Int64); ok && h.MaxPerConn > 0 && n.Add(1) >= int64(h.MaxPerConn) {
		w.Header().Set("Connection", "close")
	}
	if r.URL.Path != "/gelf" {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	if h.Busy != nil && h.Busy() {
		h.C.Count("503")
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(MaxDoc)+1))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	enc := strings.ToLower(r.Header.Get("Content-Encoding"))
	if enc != "" {
		h.C.Count("encoding_" + enc)
	}
	d, err := decode(body, enc)
	switch {
	case errors.Is(err, errTooLarge) || len(body) > MaxDoc:
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	case err != nil:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	d = bytes.TrimSpace(d)
	if len(d) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	h.C.add(d)
	w.WriteHeader(h.Status)
}

type proxyListener struct {
	net.Listener
	c *Collector
}

func (p *proxyListener) Accept() (net.Conn, error) {
	for {
		c, err := p.Listener.Accept()
		if err != nil {
			return nil, err
		}
		br := bufio.NewReader(c)
		src, err := readProxy(br)
		if err != nil {
			p.c.Count("proxy_error")
			c.Close()
			continue
		}
		p.c.mu.Lock()
		p.c.proxied = append(p.c.proxied, src)
		p.c.mu.Unlock()
		return &bufConn{Conn: c, r: br}, nil
	}
}
