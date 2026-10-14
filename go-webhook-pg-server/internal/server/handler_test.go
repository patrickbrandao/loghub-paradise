package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/sink"
)

// ------------------------------------------------------------------ harness

// testFrom is the AgentUUID every test instance stamps as from.
const testFrom = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"

// stubRing answers Enqueue with whatever the test needs, which is how every
// error path of step 13 is reachable without a destination -- including the
// ones the memory ring never takes, which SPEC §12.3 still guards.
type stubRing struct {
	err   error
	stats ring.Stats

	// mu guards what Enqueue records: on a real socket the handler runs on
	// the server's goroutine, and the response arriving over TCP is no
	// happens-before edge the race detector can see.
	mu sync.Mutex
	// last is the item the handler handed over, so a test can check what the
	// handler did with its buffers afterwards.
	last *envelope.Item
	// env is last.Env copied INSIDE Enqueue: on the error paths the handler
	// releases the item right after, and the envelope it wrote must still be
	// checked against the contract.
	env []byte
}

func (r *stubRing) Enqueue(ctx context.Context, it *envelope.Item) error {
	r.mu.Lock()
	r.last, r.env = it, bytes.Clone(it.Env)
	r.mu.Unlock()
	return r.err
}
func (r *stubRing) Dequeue(ctx context.Context, n int) ([]*envelope.Item, error) {
	return nil, errors.New("stub: no backlog")
}
func (r *stubRing) Commit(items []*envelope.Item) error { return nil }
func (r *stubRing) Drain() []*envelope.Item             { return nil }
func (r *stubRing) Stats() ring.Stats                   { return r.stats }
func (r *stubRing) Mode() string                        { return "stub" }
func (r *stubRing) Close() error                        { return nil }

// taken returns what the handler handed over, if anything.
func (r *stubRing) taken() (*envelope.Item, []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.env
}

type healthySink struct{}

func (healthySink) Write(context.Context, []*envelope.Item) error { return nil }
func (healthySink) Health(context.Context) error                  { return nil }
func (healthySink) Close() error                                  { return nil }
func (healthySink) Type() string                                  { return "stub" }

func testServer(t *testing.T, r ring.Ring, cfg Config) (*Server, *obs.Metrics) {
	t.Helper()
	return testServerTrusting(t, r, cfg, nil)
}

// testServerTrusting is testServer with proxy.trusted set.
func testServerTrusting(t testing.TB, r ring.Ring, cfg Config, trusted []netip.Prefix) (*Server, *obs.Metrics) {
	t.Helper()
	m := obs.NewMetrics("test", "none", "go", "stub", "stub")
	log := obs.NewLogger(io.Discard, "text", "error")
	h := sink.NewHealth(healthySink{}, sink.HealthConfig{}, log.Logger, m)

	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 5 << 20
	}
	if cfg.MaxURIBytes == 0 {
		cfg.MaxURIBytes = 8 << 10
	}
	if cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = 64 << 10
	}
	if cfg.From == "" {
		cfg.From = testFrom
	}
	if cfg.Space == "" {
		cfg.Space = "default"
	}
	if cfg.SID == 0 {
		cfg.SID = 4242
	}
	if cfg.Mode == "" {
		cfg.Mode = envelope.ModeFast
	}
	if cfg.Stage == "" {
		cfg.Stage = "ring"
	}
	return New(cfg, r, h, m, log, trusted, 32, nil), m
}

// onSocket makes a test request look like what net/http hands the handler on a
// TCP connection: the peer in RemoteAddr and the accepting socket in the
// context. describe() reads both (SPEC §3.2, network).
func onSocket(r *http.Request, peer, local string) *http.Request {
	r.RemoteAddr = peer
	l := net.TCPAddrFromAddrPort(netip.MustParseAddrPort(local))
	return r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, l))
}

// envelopeOf checks the JLH the handler wrote against the contract of SPEC
// §15.1 -- both schemas and the semantic rules -- and the header this
// instance stamps on every event (§3.1).
func envelopeOf(t *testing.T, s *Server, stub *stubRing) *jlhcheck.Envelope {
	t.Helper()
	it, env := stub.taken()
	if it == nil {
		t.Fatal("the handler never reached Enqueue")
	}
	e, err := jlhcheck.Validate(env)
	if err != nil {
		t.Fatalf("the handler wrote an envelope that breaks the contract: %v\n%s", err, env)
	}
	if e.UUID != it.ID {
		t.Errorf("envelope uuid %s, item id %s", e.UUID, it.ID)
	}
	if e.From != s.cfg.From || e.Space != s.cfg.Space || uint64(e.SID) != s.cfg.SID || e.Payload.Mode != s.cfg.Mode {
		t.Errorf("header from=%s space=%s sid=%d mode=%s; want %s %s %d %s", e.From, e.Space, e.SID,
			e.Payload.Mode, s.cfg.From, s.cfg.Space, s.cfg.SID, s.cfg.Mode)
	}
	return e
}

// answer is the JSON body of SPEC §4.4.
type answer struct {
	UUID   string `json:"uuid"`
	Mode   string `json:"mode"`
	Status string `json:"status"`
}

// jsonAnswer checks an answer given after the uuid existed: X-LogHub-UUID, a
// JSON body built from the fixed template, and the uuid of the item the ring
// was handed.
func jsonAnswer(t *testing.T, s *Server, stub *stubRing, w *httptest.ResponseRecorder, status string) {
	t.Helper()
	it, _ := stub.taken()
	if it == nil {
		t.Fatal("the handler never reached Enqueue")
	}
	if got := w.Header().Get("X-LogHub-UUID"); got != it.ID {
		t.Errorf("X-LogHub-UUID = %q, want the envelope's uuid %q", got, it.ID)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	// stage is only on a 200 (SPEC §4.4): a refused envelope is nowhere.
	stage := ""
	if w.Code == http.StatusOK {
		stage = `","stage":"` + s.cfg.Stage
	}
	want := `{"uuid":"` + it.ID + `","mode":"` + s.cfg.Mode + `","status":"` + status + stage + `"}` + "\n"
	if got := w.Body.String(); got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
	var a answer
	if err := json.Unmarshal(w.Body.Bytes(), &a); err != nil || a.UUID != it.ID || a.Status != status {
		t.Errorf("body does not decode to the answer: %+v, %v", a, err)
	}
	if w.Header().Get("X-Event-Id") != "" {
		t.Error("X-Event-Id is the v1 header; it no longer exists")
	}
}

// plainAnswer checks a refusal decided before the uuid existed: one line of
// text/plain, no uuid anywhere, and nothing handed to the ring.
func plainAnswer(t *testing.T, stub *stubRing, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	if w.Code != code {
		t.Fatalf("status = %d, want %d (body %q)", w.Code, code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if w.Header().Get("X-LogHub-UUID") != "" {
		t.Errorf("a refusal before the envelope carried X-LogHub-UUID %q", w.Header().Get("X-LogHub-UUID"))
	}
	if b := w.Body.String(); strings.Count(b, "\n") != 1 || !strings.HasSuffix(b, "\n") || strings.HasPrefix(b, "{") {
		t.Errorf("body = %q, want one line of text", b)
	}
	if it, _ := stub.taken(); it != nil {
		t.Error("a request refused before the envelope reached the ring")
	}
}

// ------------------------------------------------------------- header size

func TestHeaderSize(t *testing.T) {
	r := httptest.NewRequest("POST", "/hook", nil)
	r.Host = "api.example.com"
	base := headerSize(r)
	if base <= 0 {
		t.Fatalf("headerSize of a minimal request = %d", base)
	}

	// Host is lifted out of r.Header by net/http, so it has to be counted
	// explicitly or a request that is nothing but a huge Host slips through.
	r.Host = strings.Repeat("h", 1000)
	want := int64(1000 - len("api.example.com"))
	if got := headerSize(r) - base; got != want {
		t.Errorf("growing Host by %d bytes moved the count by %d", want, got)
	}

	// Every value of a repeated header counts, not just the first: duplicates
	// are exactly how a header flood is built.
	r = httptest.NewRequest("POST", "/hook", nil)
	before := headerSize(r)
	for i := 0; i < 10; i++ {
		r.Header.Add("X-Pad", strings.Repeat("p", 100))
	}
	after := headerSize(r)
	if got, want := after-before, int64(10*(len("X-Pad")+100+4)); got != want {
		t.Errorf("ten repeated headers added %d bytes, want %d", got, want)
	}
}

// SPEC §4.3 step 2 and §4.4: headers above server.max_header_bytes are 431.
//
// This limit is enforced HERE rather than left to net/http's MaxHeaderBytes,
// which answers by writing 431 straight to the socket before any handler runs
// -- invisible to whs_requests_total and to whs_rejected_total both.
func TestOversizedHeadersAre431(t *testing.T) {
	stub := &stubRing{}
	s, m := testServer(t, stub, Config{MaxHeaderBytes: 2048})

	r := httptest.NewRequest("POST", "/hook", nil)
	r.Header.Set("X-Pad", strings.Repeat("p", 4096))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)

	plainAnswer(t, stub, w, http.StatusRequestHeaderFieldsTooLarge)

	exposed := scrape(t, m)
	if !strings.Contains(exposed, `whs_requests_total{code="431"} 1`) {
		t.Error(`whs_requests_total{code="431"} did not move`)
	}
	if !strings.Contains(exposed, `whs_rejected_total{reason="bad_request"} 1`) {
		t.Error(`whs_rejected_total{reason="bad_request"} did not move`)
	}
}

func TestHeadersUnderTheCeilingAreAccepted(t *testing.T) {
	stub := &stubRing{}
	s, _ := testServer(t, stub, Config{MaxHeaderBytes: 2048})

	r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
	r.Header.Set("X-Pad", strings.Repeat("p", 512))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, onSocket(r, "198.51.100.20:40000", "192.0.2.10:8080"))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	jsonAnswer(t, s, stub, w, "accepted")
	e := envelopeOf(t, s, stub)
	found := false
	for _, h := range e.Payload.Headers {
		found = found || (h.Name == "x-pad" && len(h.Value) == 512)
	}
	if !found {
		t.Error("the padded header is not in the envelope")
	}
}

// ------------------------------------------------------------------ the 200

// SPEC §4.4: 200, X-LogHub-UUID, and {"uuid","mode":"fast","status":"accepted",
// "stage"} from a fixed template. stage is where the envelope was when the
// client was answered: always the ring here.
func TestAcceptedAnswer(t *testing.T) {
	for _, tc := range []struct{ mode, status, stage string }{
		{envelope.ModeFast, "accepted", "ring"},
	} {
		t.Run(tc.stage, func(t *testing.T) {
			stub := &stubRing{}
			s, m := testServer(t, stub, Config{Mode: tc.mode, Stage: tc.stage})

			r := httptest.NewRequest("POST", "/hook?a=1", strings.NewReader(`{"n":1}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			s.ServeHTTP(w, onSocket(r, "198.51.100.20:40000", "192.0.2.10:8080"))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", w.Code)
			}
			jsonAnswer(t, s, stub, w, tc.status)
			if cl := w.Header().Get("Content-Length"); cl != itoa(w.Body.Len()) {
				t.Errorf("Content-Length = %q, body is %d bytes", cl, w.Body.Len())
			}

			// The answer and the envelope agree: one uuid, one mode.
			e := envelopeOf(t, s, stub)
			if e.Payload.Mode != tc.mode {
				t.Errorf("payload mode = %q, want %q", e.Payload.Mode, tc.mode)
			}
			body, err := e.Payload.Body()
			if err != nil || string(body) != `{"n":1}` || e.Payload.Content.Type != "application/json" {
				t.Errorf("content = %+v (%v)", e.Payload.Content, err)
			}
			// The ring owns the item now: the handler must not have touched it.
			if it, _ := stub.taken(); it.Released() {
				t.Error("the handler released an item the ring accepted")
			}
			if !strings.Contains(scrape(t, m), `whs_requests_total{code="200"} 1`) {
				t.Error(`whs_requests_total{code="200"} did not move`)
			}
		})
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// ------------------------------------------------------- answers by uuid

// SPEC §4.4: once the uuid exists (steps 13-14), a failure carries the same
// header and a JSON body with status error, so the client can match it with
// the server's log; the envelope was dropped. Each code keeps its Retry-After.
func TestErrorAnswersAfterTheUUIDAreJSON(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		code       int
		retryAfter string
	}{
		{"ring full", ring.ErrFull, http.StatusTooManyRequests, "1"},
		{"ring closed", ring.ErrClosed, http.StatusServiceUnavailable, "5"},
		{"any other error", errors.New("a future ring's own failure"),
			http.StatusServiceUnavailable, "5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{err: tc.err}
			s, _ := testServer(t, stub, Config{Mode: envelope.ModeFast})

			r := httptest.NewRequest("POST", "/hook", strings.NewReader("payload"))
			w := httptest.NewRecorder()
			s.ServeHTTP(w, onSocket(r, "198.51.100.20:40000", "192.0.2.10:8080"))

			if w.Code != tc.code {
				t.Fatalf("status = %d, want %d", w.Code, tc.code)
			}
			jsonAnswer(t, s, stub, w, "error")
			if got := w.Header().Get("Retry-After"); got != tc.retryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tc.retryAfter)
			}
			// The envelope that was refused was still a valid one.
			envelopeOf(t, s, stub)
		})
	}
}

// SPEC §4.2-§4.4: before step 10 there is no uuid, and the refusal is one line
// of text/plain -- the same for the new LogHub refusals (a method outside
// ^[A-Z]+$, a target that is not a path) as for the v1 ones.
func TestErrorAnswersBeforeTheUUIDArePlainText(t *testing.T) {
	cases := []struct {
		name  string
		cfg   Config
		stub  *stubRing
		setup func(s *Server)
		req   func() *http.Request
		code  int
	}{
		{name: "431 headers", cfg: Config{MaxHeaderBytes: 1024}, code: http.StatusRequestHeaderFieldsTooLarge,
			req: func() *http.Request {
				r := httptest.NewRequest("POST", "/hook", nil)
				r.Header.Set("X-Pad", strings.Repeat("p", 2048))
				return r
			}},
		{name: "414 uri", cfg: Config{MaxURIBytes: 64}, code: http.StatusRequestURITooLong,
			req: func() *http.Request { return httptest.NewRequest("POST", "/"+strings.Repeat("a", 100), nil) }},
		{name: "405 connect", code: http.StatusMethodNotAllowed,
			req: func() *http.Request { return httptest.NewRequest("CONNECT", "example.com:443", nil) }},
		{name: "405 lowercase method", code: http.StatusMethodNotAllowed,
			req: func() *http.Request { return httptest.NewRequest("post", "/hook", strings.NewReader("x")) }},
		{name: "405 mixed-case method", code: http.StatusMethodNotAllowed,
			req: func() *http.Request { return httptest.NewRequest("Post", "/hook", strings.NewReader("x")) }},
		{name: "405 method with a dash and a digit", code: http.StatusMethodNotAllowed,
			req: func() *http.Request { return httptest.NewRequest("M-SEARCH2", "/hook", nil) }},
		{name: "400 OPTIONS *", code: http.StatusBadRequest,
			req: func() *http.Request { return httptest.NewRequest("OPTIONS", "*", nil) }},
		{name: "400 POST *", code: http.StatusBadRequest,
			req: func() *http.Request { return httptest.NewRequest("POST", "*", strings.NewReader("x")) }},
		{name: "413 declared", cfg: Config{MaxBodyBytes: 4}, code: http.StatusRequestEntityTooLarge,
			req: func() *http.Request { return httptest.NewRequest("POST", "/hook", strings.NewReader("too large")) }},
		{name: "413 read", cfg: Config{MaxBodyBytes: 4}, code: http.StatusRequestEntityTooLarge,
			req: func() *http.Request {
				// No declared length: the ceiling is only found while reading.
				return httptest.NewRequest("POST", "/hook", io.MultiReader(strings.NewReader("too large")))
			}},
		{name: "400 unreadable body", code: http.StatusBadRequest,
			req: func() *http.Request {
				return httptest.NewRequest("POST", "/hook", iotest.ErrReader(errors.New("connection reset")))
			}},
		{name: "503 sink down", code: http.StatusServiceUnavailable,
			setup: func(s *Server) { s.health.MarkFatal(errors.New("destination gone")) },
			req:   func() *http.Request { return httptest.NewRequest("POST", "/hook", strings.NewReader("x")) }},
		{name: "503 shutdown", code: http.StatusServiceUnavailable,
			setup: func(s *Server) { s.StartDraining() },
			req:   func() *http.Request { return httptest.NewRequest("POST", "/hook", strings.NewReader("x")) }},
		{name: "429 ring full, fast check", code: http.StatusTooManyRequests,
			stub: &stubRing{stats: ring.Stats{Items: 8, CapItems: 8}},
			req:  func() *http.Request { return httptest.NewRequest("POST", "/hook", strings.NewReader("x")) }},
		// SPEC §4.3 step 9: bytes count too, against the declared length as
		// a lower bound of the event.
		{name: "429 ring full by bytes, fast check", code: http.StatusTooManyRequests,
			stub: &stubRing{stats: ring.Stats{Bytes: 1 << 20, CapBytes: 1 << 20, CapItems: 8}},
			req:  func() *http.Request { return httptest.NewRequest("POST", "/hook", strings.NewReader("x")) }},
		{name: "429 ring full by bytes, no declared length", code: http.StatusTooManyRequests,
			stub: &stubRing{stats: ring.Stats{Bytes: 1 << 20, CapBytes: 1 << 20, CapItems: 8}},
			req: func() *http.Request {
				return httptest.NewRequest("POST", "/hook", io.MultiReader(strings.NewReader("x")))
			}},
		{name: "429 declared body does not fit", code: http.StatusTooManyRequests,
			stub: &stubRing{stats: ring.Stats{Bytes: 1<<20 - 10, CapBytes: 1 << 20, CapItems: 8}},
			req: func() *http.Request {
				return httptest.NewRequest("POST", "/hook", strings.NewReader(strings.Repeat("b", 100)))
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := tc.stub
			if stub == nil {
				stub = &stubRing{}
			}
			s, _ := testServer(t, stub, tc.cfg)
			if tc.setup != nil {
				tc.setup(s)
			}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, tc.req())
			plainAnswer(t, stub, w, tc.code)
		})
	}

	// The controls: an invented method of capitals, an OPTIONS on a path and a
	// body at the ceiling are events, so the refusals above are about the one
	// thing each case changed.
	for _, r := range []*http.Request{
		httptest.NewRequest("PROPFIND", "/hook", nil),
		httptest.NewRequest("OPTIONS", "/hook", nil),
		httptest.NewRequest("POST", "/hook", io.MultiReader(strings.NewReader("1234"))),
	} {
		stub := &stubRing{}
		s, _ := testServer(t, stub, Config{MaxBodyBytes: 4})
		w := httptest.NewRecorder()
		s.ServeHTTP(w, onSocket(r, "198.51.100.20:40000", "192.0.2.10:8080"))
		if w.Code != http.StatusOK {
			t.Errorf("control %s %s: status %d, want 200", r.Method, r.RequestURI, w.Code)
			continue
		}
		if e := envelopeOf(t, s, stub); e.Payload.Method != r.Method {
			t.Errorf("control: method %q, want %q", e.Payload.Method, r.Method)
		}
	}

	// The control of the byte check: the same 100 bytes with room to spare
	// reach the ring, so the refusal above is about the declared length.
	stub := &stubRing{stats: ring.Stats{Bytes: 1<<20 - 10000, CapBytes: 1 << 20, CapItems: 8}}
	s, _ := testServer(t, stub, Config{})
	w := httptest.NewRecorder()
	s.ServeHTTP(w, onSocket(httptest.NewRequest("POST", "/hook", strings.NewReader(strings.Repeat("b", 100))),
		"198.51.100.20:40000", "192.0.2.10:8080"))
	if w.Code != http.StatusOK {
		t.Errorf("control: 100 bytes with 10000 free: status %d, want 200", w.Code)
	}
}

// ------------------------------------------------- destination failure codes

// SPEC §4.4: an error the ring was not expected to return is a retryable 503
// with Retry-After, never a 500 -- this server has no path where a
// destination's own refusal reaches the client.
func TestSinkNetworkErrorIs503(t *testing.T) {
	netErr := &sink.Error{Class: sink.ClassNetwork, Op: "xadd", Err: errors.New("broken pipe")}
	stub := &stubRing{err: netErr}
	s, _ := testServer(t, stub, Config{})

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("POST", "/hook", strings.NewReader("x")))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	// Retryable, and the client must be told so: without it the sender backs
	// off on its own schedule or not at all.
	if w.Header().Get("Retry-After") != "5" {
		t.Errorf("Retry-After = %q, want 5", w.Header().Get("Retry-After"))
	}
	jsonAnswer(t, s, stub, w, "error")
}

// SPEC §12.3, error paths. Which errors let the handler pool the JLH buffer
// and which must leave it to the GC: the ring refusing outright means nobody
// below saw the item; any other error -- which the memory ring never returns --
// leaves the buffer to the GC, because nobody can say who still reads it.
func TestEnqueueErrorDecidesWhetherBuffersArePooled(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		pooled bool
	}{
		{"ring full releases", ring.ErrFull, http.StatusTooManyRequests, true},
		{"ring closed releases", ring.ErrClosed, http.StatusServiceUnavailable, true},
		// No 500 any more (SPEC §4.4): with the memory ring a destination's
		// refusal never reaches the handler, and an unexpected error of any
		// class is treated as "something below may still read the buffer".
		{"a logic-class error discards", &sink.Error{Class: sink.ClassLogic, Op: "insert", Err: errors.New("22P05")},
			http.StatusServiceUnavailable, false},
		{"op_timeout discards", &sink.Error{Class: sink.ClassNetwork, Op: "xadd", Err: context.DeadlineExceeded},
			http.StatusServiceUnavailable, false},
		{"cancellation discards", &sink.Error{Class: sink.ClassNetwork, Op: "xadd", Err: context.Canceled},
			http.StatusServiceUnavailable, false},
		{"connection failure discards", &sink.Error{Class: sink.ClassNetwork, Op: "xadd", Err: errors.New("broken pipe")},
			http.StatusServiceUnavailable, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubRing{err: tc.err}
			s, _ := testServer(t, stub, Config{})

			r := httptest.NewRequest("POST", "/hook", strings.NewReader("payload"))
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)

			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d", w.Code, tc.status)
			}
			it, _ := stub.taken()
			if it == nil {
				t.Fatal("the handler never reached Enqueue")
			}
			if !it.Released() {
				t.Fatal("the item was neither released nor discarded: a leak of the ownership rule")
			}
			if tc.pooled && it.Env != nil {
				t.Error("Release must clear the buffer it pooled")
			}
			if !tc.pooled && it.Env == nil {
				t.Error("Discard must leave the buffer alone; something below may still read it")
			}
		})
	}
}

// SPEC §5.2, repeated lines. A proxy that INSERTS its own X-Forwarded-For line
// (HAProxy's option forwardfor, any add-header rule) leaves the client's forged
// line first. Reading only that line would resolve the forged address; the
// lines must be joined in arrival order so the walk from the right reaches the
// address the proxy saw.
func TestRepeatedForwardingHeaderLinesAreJoined(t *testing.T) {
	newTrustedServer := func(t *testing.T) (*Server, *stubRing) {
		t.Helper()
		stub := &stubRing{}
		// httptest.NewRequest's peer is 192.0.2.1: trusted here, so the headers
		// are believed at all.
		s, _ := testServerTrusting(t, stub, Config{MaxBodyBytes: 1 << 20},
			[]netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")})
		return s, stub
	}
	src := func(t *testing.T, s *Server, stub *stubRing) (string, string) {
		t.Helper()
		e := envelopeOf(t, s, stub)
		source, _ := e.Payload.Meta("src-source")
		return e.Payload.Network.IP.Src, source.(string)
	}

	t.Run("second X-Forwarded-For line wins over the client's", func(t *testing.T) {
		s, stub := newTrustedServer(t)
		r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
		r.Header.Add("X-Forwarded-For", "203.0.113.9")  // forged by the client
		r.Header.Add("X-Forwarded-For", "198.51.100.7") // added by the proxy
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d", w.Code)
		}
		if got, source := src(t, s, stub); got != "198.51.100.7" || source != "xff" {
			t.Fatalf("network.ip.src = %s (%s), want the proxy's line 198.51.100.7 (xff)", got, source)
		}
	})

	t.Run("a single line is unchanged", func(t *testing.T) {
		s, stub := newTrustedServer(t)
		r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
		r.Header.Set("X-Forwarded-For", "203.0.113.9, 198.51.100.7")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if got, _ := src(t, s, stub); got != "198.51.100.7" {
			t.Fatalf("network.ip.src = %s, want 198.51.100.7", got)
		}
	})

	t.Run("last X-Real-IP line wins", func(t *testing.T) {
		s, stub := newTrustedServer(t)
		r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
		r.Header.Add("X-Real-IP", "203.0.113.9")
		r.Header.Add("X-Real-IP", "198.51.100.7")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if got, source := src(t, s, stub); got != "198.51.100.7" || source != "xri" {
			t.Fatalf("network.ip.src = %s (%s), want the last line 198.51.100.7 (xri)", got, source)
		}
	})

	t.Run("padding the first line past max_xff_entries does not help", func(t *testing.T) {
		s, stub := newTrustedServer(t)
		r := httptest.NewRequest("POST", "/hook", strings.NewReader("x"))
		r.Header.Add("X-Forwarded-For", strings.Repeat("203.0.113.9,", 40)+"203.0.113.9")
		r.Header.Add("X-Forwarded-For", "198.51.100.7")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if got, _ := src(t, s, stub); got != "198.51.100.7" {
			t.Fatalf("network.ip.src = %s, want 198.51.100.7", got)
		}
	})
}

func scrape(t *testing.T, m *obs.Metrics) string {
	t.Helper()
	var sb strings.Builder
	if err := m.WritePrometheus(&sb); err != nil {
		t.Fatal(err)
	}
	return sb.String()
}

// ------------------------------------------------------------- CloseWrite

// net/http finds the half-close by type assertion. A wrapper that does not
// forward it makes the assertion fail, so no FIN is sent and the connection
// goes straight to a full Close while the client is still writing -- an RST
// that also discards what the client had not yet read, which is the 431 or 400
// net/http had just written.
func TestLimitConnForwardsCloseWrite(t *testing.T) {
	inner := &fakeCloseWriteConn{}
	c := &limitConn{Conn: inner, release: func() {}}

	if _, ok := any(c).(interface{ CloseWrite() error }); !ok {
		t.Fatal("limitConn does not satisfy net/http's closeWriter")
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if !inner.closedWrite {
		t.Error("CloseWrite was not forwarded to the underlying connection")
	}
}

// A connection that cannot half-close must not turn that into an error: the
// caller in net/http ignores the result and carries on either way.
func TestLimitConnCloseWriteOnPlainConn(t *testing.T) {
	c := &limitConn{Conn: &plainConn{}, release: func() {}}
	if err := c.CloseWrite(); err != nil {
		t.Errorf("CloseWrite on a connection without one returned %v", err)
	}
}

type plainConn struct{ net.Conn }

type fakeCloseWriteConn struct {
	net.Conn
	closedWrite bool
}

func (c *fakeCloseWriteConn) CloseWrite() error { c.closedWrite = true; return nil }

// A body that exactly fills its buffer is not grown just to read the EOF that
// HTTP/2 and chunked bodies report on a Read of their own (bytes.Reader does
// the same): growing allocated twice the body and copied it for nothing.
func TestReadBodyDoesNotGrowAFullBufferAtEOF(t *testing.T) {
	s, _ := testServer(t, &stubRing{}, Config{})
	for _, size := range []int{4 << 10, 16 << 10, 64 << 10} {
		body := bytes.Repeat([]byte{'x'}, size)
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.ContentLength = int64(size)
		buf, class, err := s.readBody(r)
		if err != nil || !bytes.Equal(buf, body) {
			t.Fatalf("%d: err %v, %d bytes read", size, err, len(buf))
		}
		if cap(buf) != size {
			t.Errorf("%d: buffer grown to cap %d", size, cap(buf))
		}
		envelope.PutBody(buf, class)
	}
	// An unknown length still grows, and the probed byte is kept.
	body := make([]byte, 70000)
	for i := range body {
		body[i] = byte(i)
	}
	r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	r.ContentLength = -1
	buf, class, err := s.readBody(r)
	if err != nil || !bytes.Equal(buf, body) {
		t.Fatalf("unknown length: err %v, %d bytes read, equal %v", err, len(buf), bytes.Equal(buf, body))
	}
	envelope.PutBody(buf, class)
}

// shortReader hands out its data and then EOF, and records the largest read
// it was asked for: the size of the buffer the handler offered.
type shortReader struct {
	data    []byte
	maxRead int
}

func (r *shortReader) Read(p []byte) (int, error) {
	r.maxRead = max(r.maxRead, len(p))
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// SPEC §12.2: the declared Content-Length only pre-sizes the body up to 64
// KiB; past that the buffer grows with bytes that ARRIVED, doubling and never
// past the declared length. Sizing by the header let a request that declared 5
// MiB and sent nothing hold 5 MiB until read_timeout -- 250 of them per h2c
// connection.
func TestReadBodyAllocatesOnlyWhatArrives(t *testing.T) {
	s, _ := testServer(t, &stubRing{}, Config{})

	// A peer that promises 5 MiB and sends 10 bytes.
	rd := &shortReader{data: []byte("0123456789")}
	r := httptest.NewRequest("POST", "/", rd)
	r.ContentLength = 5 << 20
	buf, class, err := s.readBody(r)
	if err != nil || string(buf) != "0123456789" {
		t.Fatalf("err %v, body %q", err, buf)
	}
	if cap(buf) > 64<<10 || rd.maxRead > 64<<10 {
		t.Errorf("declared 5 MiB, sent 10 bytes: buffer cap %d, largest read offered %d; want both <= 64 KiB",
			cap(buf), rd.maxRead)
	}
	envelope.PutBody(buf, class)

	// Honest bodies above the pre-size: the growth lands on the declared
	// length exactly, not on the next doubling past it.
	for _, size := range []int{100 << 10, 1 << 20, 5 << 20} {
		body := make([]byte, size)
		for i := range body {
			body[i] = byte(i * 7)
		}
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.ContentLength = int64(size)
		buf, class, err := s.readBody(r)
		if err != nil || !bytes.Equal(buf, body) {
			t.Fatalf("%d: err %v, %d bytes read, equal %v", size, err, len(buf), bytes.Equal(buf, body))
		}
		if size > envelope.PoolMaxBytes && cap(buf) != size {
			t.Errorf("%d: buffer cap %d, want exactly the declared length", size, cap(buf))
		}
		if cap(buf) > max(size, envelope.PoolMaxBytes) {
			t.Errorf("%d: buffer cap %d grew past the declared length", size, cap(buf))
		}
		envelope.PutBody(buf, class)
	}
}
