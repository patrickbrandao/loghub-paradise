package server

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/clientip"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/internal/ring"
)

// ServeHTTP implements the request pipeline of SPEC §4.3.
//
// The order of the checks is the whole point: every rejection that can be
// decided from the headers alone happens BEFORE the body is read, so an
// overloaded or attacked server spends no bandwidth, memory or CPU on traffic
// it is about to discard.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.noteProtocol(r)

	// 2. Header ceiling.
	if n := headerSize(r); n > s.cfg.MaxHeaderBytes {
		s.reject(w, r, start, http.StatusRequestHeaderFieldsTooLarge, obs.ReasonBadRequest,
			"request header fields too large", 0)
		return
	}

	// 3. URI ceiling. r.RequestURI is the raw line as it arrived. It comes
	// before the method check because SPEC §4.3 says so and calls the order a
	// contract: a CONNECT with an oversized target is a 414, not a 405.
	if int64(len(r.RequestURI)) > s.cfg.MaxURIBytes {
		s.reject(w, r, start, http.StatusRequestURITooLong, obs.ReasonBadRequest,
			"request uri too long", 0)
		return
	}

	// 4. CONNECT does not become an event: answering it would get this host
	// catalogued as an open proxy and attract permanent scanning. A method
	// outside ^[A-Z]+$ is refused rather than upper-cased, which would lie
	// about what arrived, and a target that is not a path (OPTIONS *) has no
	// path for the envelope to carry (SPEC §4.2).
	if r.Method == http.MethodConnect {
		s.reject(w, r, start, http.StatusMethodNotAllowed, obs.ReasonBadRequest,
			"connect not supported", 0)
		return
	}
	if !validMethod(r.Method) {
		s.reject(w, r, start, http.StatusMethodNotAllowed, obs.ReasonBadRequest,
			"method must be uppercase letters", 0)
		return
	}
	if p := r.URL.EscapedPath(); p != "" && p[0] != '/' {
		s.reject(w, r, start, http.StatusBadRequest, obs.ReasonBadRequest,
			"request target must be a path", 0)
		return
	}

	// 5. Resolve the client address.
	peer, peerPort := clientip.PeerAddr(r.RemoteAddr)
	ipres := s.resolver.Resolve(peer, forwardedFor(r), realIP(r))

	if ipres.UntrustedForwarding {
		// Almost always a misconfigured proxy.trusted. Counted always, logged
		// at most once every 30s so a misconfigured deployment does not write a
		// line per request.
		s.metrics.ClientIPUntrustedForwardedTotal.Inc()
		if ok, suppressed := s.untrustedSampler.Allow(); ok {
			s.log.Warn("forwarding header from an untrusted peer was ignored; "+
				"add this address to proxy.trusted if it is your reverse proxy",
				"peer", peer.String(), "suppressed_since_last", suppressed)
		}
	}

	// 6. The rate limit of go-webhook-fast-server is not here: it belongs to
	// the reverse proxy (SPEC §6). The step keeps its number so the pipeline
	// reads the same in both programs.

	// 7. Declared body size. Rejecting on Content-Length costs nothing and
	// avoids reading a body we already know is too large.
	if r.ContentLength > s.cfg.MaxBodyBytes {
		s.reject(w, r, start, http.StatusRequestEntityTooLarge, obs.ReasonTooLarge,
			"body too large", 0)
		return
	}

	// 8. Sink DOWN: fail fast, even with an empty ring.
	if !s.health.Up() {
		s.reject(w, r, start, http.StatusServiceUnavailable, obs.ReasonSinkDown,
			"upstream unavailable", 5*time.Second)
		return
	}

	// Shutting down: refuse new work on connections that are still open.
	if s.draining.Load() {
		s.reject(w, r, start, http.StatusServiceUnavailable, obs.ReasonShutdown,
			"server is shutting down", 5*time.Second)
		return
	}

	// 9. Fast ring check. This is an optimisation only; the authoritative
	// answer comes from Enqueue below, which cannot race.
	if st := s.ring.Stats(); st.CapItems > 0 && st.Items >= st.CapItems {
		s.reject(w, r, start, http.StatusTooManyRequests, obs.ReasonRingFull,
			"ring full", time.Second)
		return
	}

	// 10. ONE clock sample, the end of header reading, before the body: the
	// uuid and the event are two views of it (SPEC §3.1). The uuid exists from
	// here on, and every answer below carries it.
	now := time.Now()
	uuid := envelope.NewUUID(now)
	id := uuid.String()

	// 11. Read the body under a hard ceiling.
	body, bodyClass, err := s.readBody(r)
	if err != nil {
		envelope.PutBody(body, bodyClass)
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.reject(w, r, start, http.StatusRequestEntityTooLarge, obs.ReasonTooLarge,
				"body too large", 0)
			return
		}
		// A truncated or timed-out body is a client problem, not ours.
		s.reject(w, r, start, http.StatusBadRequest, obs.ReasonBadRequest,
			"could not read body", 0)
		return
	}
	s.metrics.BodyBytes.Observe(int64(len(body)))

	// 12. Serialise the JLH ONCE, into a buffer sized by the pre-pass over the
	// body (SPEC §12.2). The body is inside it now, so its own buffer goes
	// back to the pool at once: nothing else ever saw it.
	peerTrusted := s.resolver.Trusts(peer)
	req := describe(r, peer, peerPort, ipres, peerTrusted, s.cfg.Mode, s.headerExclude)
	req.Body = body
	plan := envelope.PlanBody(body)
	head := envelope.Header{
		UUID: uuid, From: s.cfg.From, Space: s.cfg.Space,
		Type: envelope.Type, Level: envelope.Level, SID: s.cfg.SID, Step: 0,
		Event: envelope.EventAt(now),
	}
	envBuf, envClass := envelope.GetEnv(envelope.SizeHint(plan, headerSize(r)))
	before := cap(envBuf)
	envBuf = envelope.AppendWebhook(envBuf, &head, &req, plan)
	if cap(envBuf) != before {
		// The estimate fell short and append reallocated: this buffer is not
		// the pool's (the pooled one is left to the GC).
		envClass = -1
	}
	envelope.PutBody(body, bodyClass)
	item := envelope.NewItem(id, envBuf, envClass)

	// 13/14. Hand to the ring. This returns only once the item is in it
	// (SPEC §2.2).
	if err := s.ring.Enqueue(r.Context(), item); err != nil {
		switch {
		case errors.Is(err, ring.ErrFull):
			// Refused before the ring took the item: this goroutine still owns
			// it and nothing below has seen its buffer (SPEC §12.3).
			item.Release()
			s.rejectWithID(w, r, start, http.StatusTooManyRequests, obs.ReasonRingFull, id, time.Second)
		case errors.Is(err, ring.ErrClosed):
			item.Release()
			s.rejectWithID(w, r, start, http.StatusServiceUnavailable, obs.ReasonShutdown, id, 5*time.Second)

		default:
			// The memory ring returns nothing else. Should a future ring
			// return some other error, nobody can say whether something below
			// still reads the buffer, so it goes to the GC rather than back to
			// the pool -- pooling a buffer still in use is how another
			// client's body ends up inside this event (SPEC §12.3).
			item.Discard()
			s.log.Error("enqueue failed", "id", id, "error", err)
			s.rejectWithID(w, r, start, http.StatusServiceUnavailable, obs.ReasonSinkDown, id, 5*time.Second)
		}
		return
	}

	// 15. Accepted (SPEC §4.4).
	s.writeAnswer(w, http.StatusOK, id, "accepted")

	s.metrics.ObserveRequest(http.StatusOK, obs.Accepted, time.Since(start))
	s.accessLog(r, ipres, http.StatusOK, int64(len(body)), start)
}

// writeAnswer writes the JSON answer of SPEC §4.4 with X-LogHub-UUID. It is
// built from a fixed template: the uuid is canonical and mode, status and
// stage are constants, so nothing needs escaping. stage is only on a 200: a
// refused envelope is nowhere.
func (s *Server) writeAnswer(w http.ResponseWriter, code int, id, status string) {
	var buf [160]byte
	b := append(buf[:0], `{"uuid":"`...)
	b = append(b, id...)
	b = append(b, `","mode":"`...)
	b = append(b, s.cfg.Mode...)
	b = append(b, `","status":"`...)
	b = append(b, status...)
	if code == http.StatusOK {
		b = append(b, `","stage":"`...)
		b = append(b, s.cfg.Stage...)
	}
	b = append(b, "\"}\n"...)
	h := w.Header()
	h.Set("X-LogHub-UUID", id)
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

// rejectWithID answers a refusal decided after the uuid existed (SPEC §4.4):
// the envelope was dropped, and the uuid only lets the client match the
// server's log. The body was read in full, so there is nothing to drain.
func (s *Server) rejectWithID(w http.ResponseWriter, r *http.Request, start time.Time,
	code int, reason obs.Reason, id string, retryAfter time.Duration) {

	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
	}
	s.writeAnswer(w, code, id, "error")

	s.metrics.Reject(reason)
	s.metrics.ObserveRequest(code, obs.Rejected, time.Since(start))
	s.rejectLog(r, code, reason, start)
}

// headerSlackBytes is how much room net/http is given above the configured
// server.max_header_bytes, so that the limit this server actually enforces is
// its own (SPEC §4.3 step 2) rather than the standard library's silent one.
//
// 16 KiB comfortably covers the realistic case -- a browser or an API client
// carrying more cookies or a longer bearer token than the operator budgeted --
// while keeping the extra per-connection header buffer bounded and small. A
// request that overshoots even that is not a misconfigured client but an
// attack, and net/http shedding it on the socket without allocating a handler
// is the right answer to those.
const headerSlackBytes = 16 << 10

// headerSize reconstructs the wire size of the request head.
//
// It is an approximation and does not need to be anything else: it is compared
// against an operator's budget, not used to reassemble the request. The exact
// bytes are not recoverable anyway -- net/http normalises header casing and
// folded whitespace before the handler ever sees them -- and net/http's own
// ceiling, sitting headerSlackBytes higher, is what actually bounds the memory.
//
// Host is added explicitly because net/http lifts it out of r.Header into
// r.Host, and on HTTP/2 the request line does not exist on the wire at all; the
// few bytes of difference are far below the granularity of any sane limit.
func headerSize(r *http.Request) int64 {
	// method SP request-target SP HTTP-version CRLF
	n := int64(len(r.Method) + len(r.RequestURI) + len(r.Proto) + 4)
	if r.Host != "" {
		n += int64(len("Host: ") + len(r.Host) + 2)
	}
	for name, values := range r.Header {
		for _, v := range values {
			// name ": " value CRLF
			n += int64(len(name) + len(v) + 4)
		}
	}
	return n + 2 // the empty line that terminates the head
}

// readBody reads the whole body into a pooled buffer.
//
// The buffer is sized from Content-Length when it is present and trustworthy;
// a chunked request with no declared length starts in the smallest class and
// grows. MaxBytesReader enforces the ceiling during the read, which is the only
// defence against a chunked body that lies about its size.
func (s *Server) readBody(r *http.Request) ([]byte, int, error) {
	if r.Body == nil {
		return nil, -1, nil
	}

	want := r.ContentLength
	if want < 0 || want > s.cfg.MaxBodyBytes {
		want = 0 // unknown length: start small and grow
	}
	buf, class := envelope.GetBody(want)

	limited := http.MaxBytesReader(nil, r.Body, s.cfg.MaxBodyBytes)

	for {
		if len(buf) == cap(buf) {
			// A full buffer is often the whole body: HTTP/2 and chunked
			// bodies report EOF on a Read of its own, after the data.
			// Growing first allocated twice the body and copied it just to
			// learn there was nothing more -- 10 MiB extra for a 5 MiB h2
			// upload. One byte on the stack tells.
			var probe [1]byte
			n, err := limited.Read(probe[:])
			if n == 0 {
				if err == io.EOF {
					return buf, class, nil
				}
				if err != nil {
					return buf, class, err
				}
				continue
			}
			// Grow. The replacement is not pooled if it outgrows the classes,
			// and the old buffer is returned immediately since nothing else has
			// seen it yet.
			grown, gclass := envelope.GetBody(int64(cap(buf))*2 + 1)
			grown = append(grown, buf...)
			envelope.PutBody(buf, class)
			buf, class = append(grown, probe[0]), gclass
			if err == io.EOF {
				return buf, class, nil
			}
			if err != nil {
				return buf, class, err
			}
			continue
		}
		n, err := limited.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, class, nil
		}
		if err != nil {
			return buf, class, err
		}
	}
}

// reject writes an error response.
//
// Rejections decided before the body is read drain and discard up to
// reject_drain_bytes first, so an HTTP/1.1 client that is still sending can
// actually READ the response on the same connection. Without that it gets
// ECONNRESET instead of the status, never sees Retry-After, and retries with no
// backoff -- making the very overload the rejection was meant to shed worse.
func (s *Server) reject(w http.ResponseWriter, r *http.Request, start time.Time,
	code int, reason obs.Reason, msg string, retryAfter time.Duration) {

	closeConn := s.drainForReject(r)

	h := w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	if retryAfter > 0 {
		h.Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
	}
	if closeConn {
		h.Set("Connection", "close")
	}
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")

	s.metrics.Reject(reason)
	s.metrics.ObserveRequest(code, obs.Rejected, time.Since(start))
	s.rejectLog(r, code, reason, start)
}

// drainForReject discards up to reject_drain_bytes of the request body and
// reports whether the connection must be closed because more remained.
func (s *Server) drainForReject(r *http.Request) bool {
	if r.Body == nil || s.cfg.RejectDrainBytes <= 0 {
		return false
	}
	// Read one byte past the limit: if it arrives, there is more than we are
	// willing to absorb and the connection cannot be reused.
	n, _ := io.CopyN(io.Discard, r.Body, s.cfg.RejectDrainBytes+1)
	return n > s.cfg.RejectDrainBytes
}

// forwardedFor joins every X-Forwarded-For line, in arrival order, into the
// single list SPEC §5.1 walks from the right.
//
// Header.Get would return only the FIRST line. Proxies that INSERT a new header
// line instead of appending to the existing one -- HAProxy's option forwardfor,
// any add-header rule -- leave the client's forged line first; reading just
// that line means walking a list the attacker wrote end to end, while the
// honest line with the address the proxy saw is never opened. RFC 9110 §5.3
// allows repeated list fields to be combined with commas in the order received,
// which is exactly the chain those proxies built. With a single line the result
// is byte-identical to Header.Get (SPEC §5.2).
func forwardedFor(r *http.Request) string {
	v := r.Header.Values("X-Forwarded-For")
	if len(v) == 1 {
		return v[0]
	}
	return strings.Join(v, ",")
}

// realIP takes the LAST X-Real-IP line: it is not a list field, and the last
// line is the one the nearest proxy added; any earlier one came from further
// out, possibly from the client (SPEC §5.2).
func realIP(r *http.Request) string {
	v := r.Header.Values("X-Real-IP")
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}
