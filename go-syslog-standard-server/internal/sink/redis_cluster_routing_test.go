package sink

// A server-free lock of where the maxmemory-policy and the identity are read
// in a Redis Cluster (SPEC §10.3; revisão independente de 2026-11-07). A
// one-node cluster cannot tell the right node from an arbitrary one (there is
// only one), so this fakes a two-primary cluster: two RESP2 listeners that
// split the 16384 slots, answer the handshake and CLUSTER SLOTS, and record
// every command. The node that receives XLEN of the stream is, by the
// client's own key routing, the stream's primary; every INFO and CONFIG GET
// must land there too.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

type fakeClusterNode struct {
	ln       net.Listener
	port     int
	slots    [2]int
	peer     *fakeClusterNode
	noPolicy bool // INFO memory without the maxmemory_policy line

	mu   sync.Mutex
	seen []string // "XLEN", "INFO memory", "INFO server", "CONFIG GET", ...
}

func newFakeClusterNode(t *testing.T, lo, hi int, noPolicy bool) *fakeClusterNode {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	n := &fakeClusterNode{ln: ln, port: ln.Addr().(*net.TCPAddr).Port, slots: [2]int{lo, hi}, noPolicy: noPolicy}
	return n
}

func (n *fakeClusterNode) serve() {
	for {
		c, err := n.ln.Accept()
		if err != nil {
			return
		}
		go n.conn(c)
	}
}

func (n *fakeClusterNode) record(s string) {
	n.mu.Lock()
	n.seen = append(n.seen, s)
	n.mu.Unlock()
}

func (n *fakeClusterNode) count(s string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := 0
	for _, x := range n.seen {
		if x == s {
			k++
		}
	}
	return k
}

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }

func slotEntry(lo, hi, port int) string {
	return "*3\r\n:" + strconv.Itoa(lo) + "\r\n:" + strconv.Itoa(hi) + "\r\n*3\r\n" +
		bulk("127.0.0.1") + ":" + strconv.Itoa(port) + "\r\n" + bulk(fmt.Sprintf("node%d", port))
}

func (n *fakeClusterNode) conn(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readRESPArray(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(args[0])
		var out string
		switch cmd {
		case "HELLO":
			out = "-ERR unknown command 'HELLO'\r\n" // RESP2, CLUSTER SLOTS
		case "CLIENT", "AUTH", "SELECT", "READONLY":
			out = "+OK\r\n"
		case "PING":
			out = "+PONG\r\n"
		case "CLUSTER":
			out = "*2\r\n" + slotEntry(n.slots[0], n.slots[1], n.port) +
				slotEntry(n.peer.slots[0], n.peer.slots[1], n.peer.port)
		case "XLEN":
			n.record("XLEN")
			out = ":0\r\n"
		case "XADD":
			n.record("XADD")
			out = "$-1\r\n" // NOMKSTREAM on an absent key
		case "INFO":
			sec := ""
			if len(args) > 1 {
				sec = strings.ToLower(args[1])
			}
			n.record("INFO " + sec)
			body := "# Server\r\nredis_version:7.4.0\r\n"
			if sec == "memory" {
				body = "# Memory\r\nused_memory:1\r\n"
				if !n.noPolicy {
					body += "maxmemory_policy:noeviction\r\n"
				}
			}
			out = bulk(body)
		case "CONFIG":
			n.record("CONFIG GET")
			out = "*2\r\n" + bulk("maxmemory-policy") + bulk("noeviction")
		default:
			out = "-ERR unknown command '" + args[0] + "'\r\n"
		}
		if _, err := io.WriteString(c, out); err != nil {
			return
		}
	}
}

// readRESPArray reads one command: an array of bulk strings.
func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("not an array: %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		h, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		l, err := strconv.Atoi(strings.TrimRight(h, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		b := make([]byte, l+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		args = append(args, string(b[:l]))
	}
	return args, nil
}

func TestRedisClusterPolicyIsReadOnTheStreamPrimary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		noPolicy bool
		read     string
	}{
		{"info", false, "INFO memory"},
		{"config", true, "CONFIG GET"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newFakeClusterNode(t, 0, 8191, tc.noPolicy)
			b := newFakeClusterNode(t, 8192, 16383, tc.noPolicy)
			a.peer, b.peer = b, a
			go a.serve()
			go b.serve()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			// Several boots: INFO server, and the first CONFIG GET, are sent
			// once per client; a lone Do would reach the right node half the
			// time.
			for boot := 0; boot < 12; boot++ {
				r, err := NewRedis(ctx, RedisConfig{
					Addrs:  []string{fmt.Sprintf("127.0.0.1:%d", a.port)},
					Stream: "slstd-routing",
				}, discard(), metrics())
				if err != nil {
					t.Fatal(err)
				}
				if r.client.Mode() != rueidis.ClientModeCluster {
					r.Close()
					t.Fatalf("the fake did not put the client in cluster mode: %v", r.client.Mode())
				}
				for i := 0; i < 5; i++ {
					if err := r.Health(ctx); err != nil {
						r.Close()
						t.Fatalf("boot %d, check %d: %v", boot+1, i+1, err)
					}
				}
				r.Close()
			}

			primary, other := a, b
			if b.count("XLEN") > 0 {
				primary, other = b, a
			}
			if other.count("XLEN") != 0 {
				t.Fatal("XLEN of one stream reached both nodes")
			}
			for _, cmd := range []string{tc.read, "INFO server"} {
				if n := other.count(cmd); n != 0 {
					t.Fatalf("%s reached the node that does not hold the stream %d time(s): "+
						"the policy (or the identity) of another shard was read", cmd, n)
				}
				if primary.count(cmd) == 0 {
					t.Fatalf("%s never reached the stream's primary", cmd)
				}
			}
		})
	}
}
