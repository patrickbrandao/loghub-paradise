package app

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-injector/internal/transport"
)

// relpServer is a fake RELP server whose answers the test chooses.
type relpServer struct {
	addr string
	mu   sync.Mutex
	got  []string // every syslog DATA received, retransmissions included
	acks []string // what was answered 200
	// behavior, read under mu
	reject   func(msg string) bool // answer 500 to these
	dropAt   int                   // close the session (no answer, no serverclose) at this frame count of the session; 0 = never
	closeAt  int                   // send serverclose at this frame count; 0 = never
	holdAll  bool                  // never answer syslog
	sessions atomic.Int32
	offers   []string // the open offers received
	heads    []string
}

func newRELPServer(t *testing.T, tlsConf *tls.Config, proxy bool) *relpServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	s := &relpServer{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c, tlsConf, proxy)
		}
	}()
	return s
}

func (s *relpServer) serve(raw net.Conn, tlsConf *tls.Config, proxy bool) {
	defer raw.Close()
	n := int(s.sessions.Add(1))
	r := bufio.NewReader(raw)
	var c net.Conn = raw
	if proxy {
		h := readProxy(r)
		s.mu.Lock()
		s.heads = append(s.heads, h)
		s.mu.Unlock()
	}
	if tlsConf != nil {
		tc := tls.Server(&prefixConn{Conn: raw, r: r}, tlsConf)
		if err := tc.Handshake(); err != nil {
			return
		}
		c, r = tc, bufio.NewReader(tc)
	}
	w := bufio.NewWriter(c)
	frames := 0
	for {
		f, err := transport.ReadFrame(r)
		if err != nil {
			return
		}
		switch f.Cmd {
		case "open":
			s.mu.Lock()
			s.offers = append(s.offers, string(f.Data))
			s.mu.Unlock()
			body := "200 OK\nrelp_version=0\nrelp_software=fake,1\ncommands=syslog"
			fmt.Fprintf(w, "%d rsp %d %s\n", f.Txnr, len(body), body)
		case "syslog":
			frames++
			msg := string(f.Data)
			s.mu.Lock()
			s.got = append(s.got, msg)
			drop := s.dropAt > 0 && frames >= s.dropAt && n == 1
			closeNow := s.closeAt > 0 && frames >= s.closeAt && n == 1
			hold := s.holdAll
			rej := s.reject != nil && s.reject(msg)
			if !drop && !closeNow && !hold && !rej {
				s.acks = append(s.acks, msg)
			}
			s.mu.Unlock()
			switch {
			case drop:
				w.Flush()
				return
			case closeNow:
				fmt.Fprintf(w, "0 serverclose 0\n")
				w.Flush()
				return
			case hold:
			case rej:
				fmt.Fprintf(w, "%d rsp 25 500 refused by the server\n", f.Txnr)
			default:
				fmt.Fprintf(w, "%d rsp 6 200 OK\n", f.Txnr)
			}
		case "close":
			fmt.Fprintf(w, "%d rsp 0\n", f.Txnr)
			w.Flush()
			return
		}
		if r.Buffered() == 0 {
			w.Flush()
		}
	}
}

func (s *relpServer) received() (got, acks []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.got...), append([]string(nil), s.acks...)
}

func (s *relpServer) set(f func(*relpServer)) {
	s.mu.Lock()
	f(s)
	s.mu.Unlock()
}

func countSeq(msgs []string) map[string]int {
	m := map[string]int{}
	for _, x := range msgs {
		if g := seqRE.FindStringSubmatch(x + " "); g != nil {
			m[g[1]]++
		}
	}
	return m
}

func waitAcks(t *testing.T, s *relpServer, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, a := s.received(); len(a) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, a := s.received()
	t.Fatalf("server acked %d, want %d", len(a), n)
}

var _ = strings.Contains
