package transport

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/netip"
	"testing"
)

// --http-chunked: the body in chunks of N bytes, the zero chunk at the end,
// and no Content-Length; a stdlib server reads it back (SPEC §4.3).
func TestAppendRequestChunked(t *testing.T) {
	r := Req{Method: "POST", URI: "/gelf", Host: "h", Body: []byte(`{"a":"0123456789"}`), Chunk: 7,
		Header: [][2]string{{"Content-Type", "application/json"}}}
	got := string(AppendRequest(nil, &r, true))
	want := "POST /gelf HTTP/1.1\r\nHost: h\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n" +
		"Connection: close\r\n\r\n7\r\n{\"a\":\"0\r\n7\r\n1234567\r\n4\r\n89\"}\r\n0\r\n\r\n"
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader([]byte(got))))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(req.Body)
	if string(body) != string(r.Body) || req.ContentLength != -1 || req.TransferEncoding[0] != "chunked" {
		t.Fatalf("server read %q, length %d, %v", body, req.ContentLength, req.TransferEncoding)
	}
	r.Chunk = 0
	if b := string(AppendRequest(nil, &r, false)); !bytes.Contains([]byte(b), []byte("Content-Length: 18\r\n")) {
		t.Fatalf("Content-Length form: %q", b)
	}
}

// The adaptation needs two connections in a row closed at their first
// answer, and a connection with a second answer restores --pipeline.
func TestAdaptive(t *testing.T) {
	var ev []string
	a := &adaptive{pipe: 16, event: func(s string) { ev = append(ev, s) }}
	a.closedAtFirst()
	if a.pipeline() != 16 {
		t.Fatal("adapted after one close")
	}
	a.keptAlive()
	a.closedAtFirst()
	if a.pipeline() != 16 {
		t.Fatal("a kept-alive connection between the closes did not reset the count")
	}
	a.closedAtFirst()
	if a.pipeline() != 1 || len(ev) != 1 {
		t.Fatalf("not adapted: %d %v", a.pipeline(), ev)
	}
	a.closedAtFirst()
	a.keptAlive()
	if a.pipeline() != 16 || len(ev) != 2 {
		t.Fatalf("not restored: %d %v", a.pipeline(), ev)
	}
	one := &adaptive{pipe: 1, event: func(s string) { ev = append(ev, s) }}
	one.closedAtFirst()
	one.closedAtFirst()
	if len(ev) != 2 {
		t.Fatal("pipeline 1 adapted")
	}
}

// Each flow's dialer announces its captured addresses; without PROXY the
// flows still get dialers.
func TestFlowDialer(t *testing.T) {
	addr := server(t, nil, 0)
	o := Options{Network: "tcp", Target: addr, Timeout: 1e9, Proxy: &ProxyHeader{Version: 1}}
	fd, err := FlowDialer(o)
	if err != nil {
		t.Fatal(err)
	}
	if fd(netip.MustParseAddrPort("10.0.0.1:1"), netip.MustParseAddrPort("10.0.0.2:2")) == nil {
		t.Fatal("no dialer")
	}
	if _, err := FlowDialer(Options{Network: "nope"}); err == nil {
		t.Fatal("bad options accepted")
	}
}
