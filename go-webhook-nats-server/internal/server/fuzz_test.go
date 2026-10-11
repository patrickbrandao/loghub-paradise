package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
)

// FuzzServeHTTP: whatever bytes a client writes on the socket, the server
// either refuses them or turns them into an envelope that passes the contract
// oracle -- both schemas and the url, query and content rules of the skill's
// validate.py (SPEC §15.1). FuzzEnvelope builds the Request by hand; this one
// goes through net/http's own parser and describe(), where a '#' in the target,
// an absolute-form host and the trusted proxy's headers live.
func FuzzServeHTTP(f *testing.F) {
	for _, s := range []string{
		"POST /hook?a=1&b HTTP/1.1\r\nHost: a.b\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
		"GET /p?a=1#b HTTP/1.1\r\nHost: x\r\n\r\n",
		"GET /p?#x HTTP/1.1\r\nHost: x\r\n\r\n",
		"GET /p?%ff=1&%fe=2&a=%e2%80&b=%f0%9f%98 HTTP/1.1\r\nHost: x\r\n\r\n",
		"GET /p?\xff=1&%ef%bf%bd=2&c=%C3\xa9 HTTP/1.1\r\nHost: x\r\n\r\n",
		"GET http://%E2%84%AAexample.com/ HTTP/1.1\r\nHost: y\r\n\r\n",
		"GET /p?a=\"<>{}|\\^`[] HTTP/1.1\r\nHost: x\r\n\r\n",
		"GET /a[b]?x=%zz%%41 HTTP/1.1\r\nHost: [::1]:8080\r\n\r\n",
		"POST /x HTTP/1.1\r\nHost: pub.example\r\nX-Forwarded-Proto: https\r\nX-Forwarded-Port: 443\r\n" +
			"Connection: close, content-type\r\nContent-Type: text/plain\r\nContent-Length: 1\r\n\r\nx",
		"POST /x HTTP/1.1\r\nHost: i.svc\r\nX-Forwarded-For: 203.0.113.9\r\nX-Forwarded-Host: A.B:9\r\n\r\n",
		"POST /p HTTP/1.0\r\nContent-Length: 3\r\n\r\n\x00\x01\x02",
	} {
		f.Add(s, false)
		f.Add(s, true)
	}

	// Two servers for the whole run: one that trusts the loopback peer as a
	// proxy, one that does not.
	start := func(trusted []netip.Prefix) (*Server, *stubRing) {
		stub := &stubRing{}
		s, _ := testServerTrusting(f, stub, Config{Bind: "127.0.0.1:0", Listeners: 1}, trusted)
		if err := s.Listen(context.Background()); err != nil {
			f.Fatal(err)
		}
		go func() { _ = s.Serve() }()
		f.Cleanup(func() { _ = s.Close() })
		return s, stub
	}
	plain, plainStub := start(nil)
	proxied, proxiedStub := start([]netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")})

	f.Fuzz(func(t *testing.T, raw string, viaProxy bool) {
		s, stub := plain, plainStub
		if viaProxy {
			s, stub = proxied, proxiedStub
		}
		c, err := net.DialTimeout("tcp", s.Addr(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		// Close with a reset: thousands of executions a second would leave as
		// many sockets in TIME_WAIT and run out of ephemeral ports.
		_ = c.(*net.TCPConn).SetLinger(0)
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.WriteString(c, raw); err != nil {
			t.Skip()
		}
		// No more bytes are coming: a body shorter than its Content-Length
		// ends the request instead of waiting for the deadline.
		_ = c.(*net.TCPConn).CloseWrite()
		resp, err := http.ReadResponse(bufio.NewReader(c), nil)
		if err != nil {
			t.Skip() // net/http closed the connection without an answer
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return
		}
		id := resp.Header.Get("X-LogHub-UUID")
		it, env := stub.taken()
		if it == nil || it.ID != id {
			t.Skip() // a pipelined second request got there first
		}
		if _, err := jlhcheck.Validate(env); err != nil {
			t.Fatalf("%q: the handler wrote an envelope that breaks the contract: %v\n%s", raw, err, env)
		}
		// SPEC §9.1: one envelope is one line of the emergency dump.
		if bytes.IndexByte(env, '\n') >= 0 {
			t.Fatalf("%q: the envelope has a raw newline:\n%s", raw, env)
		}
	})
}
