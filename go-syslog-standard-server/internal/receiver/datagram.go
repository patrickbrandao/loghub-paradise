package receiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// udpMaxDatagram is the largest payload a UDP datagram can carry: 65535 minus
// the 8-byte UDP header and the 20-byte minimum IPv4 header.
//
// Nothing configurable belongs here. A datagram bigger than this does not exist,
// and the kernel truncates anything that does not fit the read buffer -- which is
// what the Truncated flag records.
const udpMaxDatagram = 65507

// MaxDatagramBytes is the longest raw line the UDP and unix transports can hand
// to the ring: both read into a buffer of udpMaxDatagram. The destinations with
// a per-message ceiling are checked against it at boot (SPEC §3.9).
const MaxDatagramBytes = udpMaxDatagram

// DatagramConfig configures a UDP or unix-datagram receiver.
type DatagramConfig struct {
	Addr       string
	Listeners  int
	ReadBuffer int64

	// Unix selects the unix-datagram transport: Addr is a socket path, the
	// socket is created and removed by this server, and Mode is its file mode.
	Unix bool
	Mode int
}

// Datagram receives one message per datagram (SPEC §4.2 and §4.3).
//
// There is no framing: the length of the datagram IS the length of the message.
// And there is no backpressure and no confirmation -- a full ring, a down sink, a
// filtered facility all end in a counted drop, because the protocol gives nobody
// to tell.
//
// That is not this server being lossy. It is UDP. What the server CAN do is make
// the loss visible, which is why every drop increments a reason.
type Datagram struct {
	cfg  DatagramConfig
	in   *Ingest
	m    *obs.Metrics
	log  *obs.Logger
	kind envelope.Transport

	conns []net.PacketConn
	wg    sync.WaitGroup

	// errLog samples the receive-error line. A socket erroring on every read
	// would otherwise write one line per attempt, forever.
	errLog *obs.Sampler
}

// NewDatagram opens the sockets. For the unix transport it also creates the
// socket file.
func NewDatagram(ctx context.Context, cfg DatagramConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Datagram, error) {
	kind := envelope.TransportUDP
	if cfg.Unix {
		kind = envelope.TransportUnix
	}
	d := &Datagram{cfg: cfg, in: in, m: m, log: log, kind: kind,
		errLog: obs.NewSampler(30 * time.Second)}

	n := cfg.Listeners
	if cfg.Unix || n < 1 {
		// A unix datagram socket has no SO_REUSEPORT equivalent: one path is one
		// socket. Several readers on it would be possible but would reorder a
		// single host's own log stream for no gain.
		n = 1
	}

	for i := 0; i < n; i++ {
		c, err := d.listen(ctx)
		if err != nil {
			d.closeAll()
			return nil, err
		}
		d.conns = append(d.conns, c)
	}
	return d, nil
}

func (d *Datagram) listen(ctx context.Context) (net.PacketConn, error) {
	if d.cfg.Unix {
		return d.listenUnix()
	}
	lc := net.ListenConfig{}
	if d.cfg.Listeners > 1 {
		// SO_REUSEPORT is the only way to receive UDP on more than one core: the
		// kernel hashes the 4-tuple across the sockets. The ring stays single,
		// so backpressure remains fair across all of them.
		lc.Control = reuseControl
	}
	c, err := lc.ListenPacket(ctx, "udp", d.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("receiver udp: %s: %w", d.cfg.Addr, err)
	}
	d.setReadBuffer(c)
	return c, nil
}

// listenUnix creates the datagram socket, taking care not to steal a live one.
func (d *Datagram) listenUnix() (net.PacketConn, error) {
	path := d.cfg.Addr
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("receiver unix: %s: %w", path, err)
	}

	// A socket left by a previous run has to go before bind can succeed. But a
	// socket somebody is SERVING must not: dialing it tells the two apart, and
	// stealing /dev/log from a running syslogd is not a mistake to make quietly.
	if _, err := os.Stat(path); err == nil {
		if c, derr := net.Dial("unixgram", path); derr == nil {
			c.Close()
			return nil, fmt.Errorf("receiver unix: %s is already served by another "+
				"process; stop it or choose another path", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("receiver unix: removing the stale socket %s: %w", path, err)
		}
		d.log.Warn("removed a stale unix socket from a previous run", "path", path)
	}

	c, err := net.ListenPacket("unixgram", path)
	if err != nil {
		return nil, fmt.Errorf("receiver unix: %s: %w", path, err)
	}
	mode := d.cfg.Mode
	if mode == 0 {
		mode = 0o666
	}
	// 0666 is what /dev/log has on every system, and it has to be: any process
	// on the host needs to be able to log. A tighter mode silently disables
	// logging for everything that is not the owner.
	if err := os.Chmod(path, os.FileMode(mode)); err != nil {
		c.Close()
		os.Remove(path)
		return nil, fmt.Errorf("receiver unix: chmod %s: %w", path, err)
	}
	d.setReadBuffer(c)
	return c, nil
}

// setReadBuffer applies SO_RCVBUF.
//
// This is the most important parameter of the datagram transports. What does not
// fit is dropped by the KERNEL, before this process sees it: the loss appears in
// /proc/net/udp and in the interface counters, and in no metric of ours. A
// failure to raise it is a warning, not an error -- the socket still works, just
// with less headroom.
func (d *Datagram) setReadBuffer(c net.PacketConn) {
	if d.cfg.ReadBuffer <= 0 {
		return
	}
	type rbuf interface{ SetReadBuffer(int) error }
	s, ok := c.(rbuf)
	if !ok {
		return
	}
	if err := s.SetReadBuffer(int(d.cfg.ReadBuffer)); err != nil {
		d.log.Warn("could not raise the socket receive buffer; the kernel will "+
			"drop datagrams sooner under load",
			"transport", d.kind.String(), "want_bytes", d.cfg.ReadBuffer, "error", err)
	}
}

// Addr reports the bound address, which is what the envelope's listener field
// carries.
func (d *Datagram) Addr() string {
	if len(d.conns) == 0 {
		return d.cfg.Addr
	}
	return d.conns[0].LocalAddr().String()
}

// Start launches one reader goroutine per socket.
func (d *Datagram) Start(ctx context.Context) {
	listener := d.Addr()
	for i := range d.conns {
		d.wg.Add(1)
		go func(c net.PacketConn) {
			defer d.wg.Done()
			d.loop(ctx, c, listener)
		}(d.conns[i])
	}
}

func (d *Datagram) loop(ctx context.Context, c net.PacketConn, listener string) {
	// One buffer per goroutine, reused for every datagram. Accept copies what it
	// keeps into a pooled buffer, so this one never escapes.
	buf := make([]byte, udpMaxDatagram)

	for {
		if ctx.Err() != nil {
			return
		}
		n, addr, err := c.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			d.m.UDPReceiveErrorsTotal.Inc()
			if ok, suppressed := d.errLog.Allow(); ok {
				d.log.Error("datagram receive failed",
					"transport", d.kind.String(), "error", err, "suppressed", suppressed)
			}
			continue
		}
		if n == 0 {
			continue
		}

		w := Wire{
			Transport: d.kind,
			Framing:   envelope.FramingDatagram,
			Listener:  listener,
			// A datagram that filled the buffer exactly was almost certainly cut
			// by the kernel. Marking it is the only honest signal available:
			// recvfrom does not report the original length.
			Truncated: n == len(buf),
		}
		if ua, ok := addr.(*net.UDPAddr); ok {
			if a, ok := netip.AddrFromSlice(ua.IP); ok {
				w.PeerAddr = a.Unmap()
			}
			w.PeerPort = uint16(ua.Port)
		}

		// There is no backpressure to apply and nobody to answer, so the counter
		// IS the report (SPEC §2.3). On the unix transport the kernel already
		// blocked the sender's send() before the datagram reached us, so the
		// pressure existed -- it just did not exist here.
		d.in.RecordDrop(d.in.Accept(ctx, buf[:n], w))
	}
}

// Close stops the readers and removes the unix socket.
func (d *Datagram) Close() error {
	err := d.closeAll()
	d.wg.Wait()
	return err
}

func (d *Datagram) closeAll() error {
	var errs []error
	for _, c := range d.conns {
		errs = append(errs, c.Close())
	}
	if d.cfg.Unix && d.cfg.Addr != "" {
		// The socket file outlives the process otherwise, and the next boot
		// would have to decide whether it is stale.
		if err := os.Remove(d.cfg.Addr); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// reuseControl sets SO_REUSEPORT on a listening socket.
func reuseControl(network, address string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) { serr = setReusePort(fd) })
	if err != nil {
		return err
	}
	return serr
}
