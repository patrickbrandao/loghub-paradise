package app

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
)

// Invariant §20.5 under concurrency and -race: many connections and
// datagrams at once; after SIGTERM every ACCEPTED message is exactly one row
// of the queue -- no loss, no duplicate -- and every envelope is valid.
func TestConcurrentTrafficEveryAcceptedMessageIsOneRow(t *testing.T) {
	in := start(t, t.TempDir())
	tcp, udp := in.app.TransportAddr("tcp"), in.app.TransportAddr("udp")
	const conns, perConn, udpSenders, perSender = 12, 60, 4, 40
	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", tcp, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			w := bufio.NewWriter(conn)
			for i := 0; i < perConn; i++ {
				m := fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app %d - - tcp-%d-%d", c, c, i)
				if c%2 == 0 {
					fmt.Fprintf(w, "%d %s", len(m), m)
				} else {
					fmt.Fprintf(w, "%s\n", m)
				}
			}
			w.Flush()
		}(c)
	}
	for s := 0; s < udpSenders; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			conn, err := net.Dial("udp", udp)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			for i := 0; i < perSender; i++ {
				fmt.Fprintf(conn, "<13>1 2026-09-12T14:33:01Z host app - - - udp-%d-%d", s, i)
			}
		}(s)
	}
	wg.Wait()
	m := in.app.metrics
	waitFor(t, 20*time.Second, "every TCP message accepted", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == conns*perConn
	})
	// UDP may lose in the kernel (SPEC §2.3); what was read is either
	// accepted or counted.
	waitFor(t, 10*time.Second, "every UDP datagram read is settled", func() bool {
		return m.MessagesTotal[obs.TransportUDP].Load() ==
			m.MessagesAcceptedTotal[obs.TransportUDP].Load()+m.MessagesDroppedTotal[obs.DropRingFull].Load()
	})
	if code := in.stop(); code != ExitOK {
		t.Fatalf("exit %d", code)
	}
	// Read after the exit: a datagram still in the socket buffer may be
	// accepted up to the moment the transport closes.
	accepted := int(m.MessagesAcceptedTotal[obs.TransportTCP].Load() + m.MessagesAcceptedTotal[obs.TransportUDP].Load())
	rows := in.rows()
	if len(rows) != accepted {
		t.Fatalf("%d rows for %d accepted messages", len(rows), accepted)
	}
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		e := checkRow(t, r)
		if seen[e.Payload.message()] {
			t.Errorf("%q stored twice", e.Payload.message())
		}
		seen[e.Payload.message()] = true
	}
	for c := 0; c < conns; c++ {
		for i := 0; i < perConn; i++ {
			if !seen[fmt.Sprintf("tcp-%d-%d", c, i)] {
				t.Errorf("tcp-%d-%d is missing", c, i)
			}
		}
	}
}
