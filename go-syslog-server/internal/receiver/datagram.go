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

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// udpReadBuffer is the userland buffer of one UDP reader: larger than any
// datagram (65 507 bytes over IPv4, 65 527 over IPv6), so the kernel never cuts
// one in silence and size is always the datagram's real length (SPEC §4.2).
// Nothing configurable belongs here; syslog.max_message_bytes cuts AFTER the
// read, in Ingest, with size kept (SPEC §5.5).
const udpReadBuffer = 65536

// oobBuffer holds the control messages of one datagram: an IPv6 or an IPv4
// packet-info message, with room to spare for a kernel that adds another.
const oobBuffer = 128

// UDPConfig is [listen] udp, udp_listeners and udp_read_buffer (SPEC §4.2,
// §16.5).
type UDPConfig struct {
	// Addr is listen.udp ("host:port"). ":1514" and "0.0.0.0:1514" both open
	// Go's dual-stack socket; dst-ip then comes from PKTINFO.
	Addr string
	// Listeners is listen.udp_listeners; above 1 the sockets share Addr
	// through SO_REUSEPORT, the only way to receive UDP on more than one core.
	Listeners int
	// ReadBuffer is listen.udp_read_buffer (8 MiB), applied with SO_RCVBUF.
	// The effective value is read back and exposed. Zero leaves the kernel
	// default.
	ReadBuffer int
}

// UnixConfig is [listen] unix, unix_mode and unix_read_buffer (SPEC §4.3,
// §16.5).
type UnixConfig struct {
	// Path is listen.unix: an ABSOLUTE path no longer than sun_path (107
	// bytes on Linux, 103 on the BSDs and macOS). The server creates the
	// socket and removes it on Close.
	Path string
	// Mode is listen.unix_mode; zero means 0666, which is what /dev/log has
	// everywhere and has to have.
	Mode os.FileMode
	// ReadBuffer is listen.unix_read_buffer (8 MiB), applied with SO_RCVBUF.
	// Zero leaves the kernel default.
	ReadBuffer int
}

// udpSocket is one UDP listener and what is known about its local end.
type udpSocket struct {
	conn *net.UDPConn
	// localIP is the bound address, canonical: the zero Addr for a wildcard.
	// It is dst-ip when PKTINFO is not available (SPEC §4.2).
	localIP   netip.Addr
	localPort uint16
	// pktinfo reports that the kernel will tell us each datagram's
	// destination address.
	pktinfo bool
}

// Datagram receives one message per datagram: UDP (RFC 5426, SPEC §4.2) or
// unix datagram (SPEC §4.3). Build it with NewUDP or NewUnix.
//
// There is no framing: the length of the datagram IS the length of the message;
// a datagram is never split on LF. And there is no backpressure and no
// confirmation -- a full ring, a down sink, a filtered facility all end in a
// counted drop, because the protocol gives nobody to tell (SPEC §2.3).
//
// That is not this server being lossy. It is UDP. What the server CAN do is make
// the loss visible, which is why every drop increments a reason.
type Datagram struct {
	transport item.Transport
	in        *Ingest
	m         *obs.Metrics
	log       *obs.Logger

	udp     []udpSocket
	unix    *net.UnixConn
	path    string // the unix socket file, removed on Close
	readBuf int    // effective SO_RCVBUF, read back; 0 when unknown

	wg sync.WaitGroup

	// errLog samples the receive-error line. A socket erroring on every read
	// would otherwise write one line per attempt, forever.
	errLog *obs.Sampler

	closeOnce sync.Once
	closeErr  error
}

// NewUDP opens the UDP sockets (SPEC §4.2): SO_REUSEPORT for more than one
// listener, SO_RCVBUF read back, and the per-datagram destination address
// (PKTINFO) where the platform has it. A platform without it, or a setsockopt
// that fails, is a WARN here and dst-ip falls back to the listener's address
// when that is concrete, and is omitted otherwise.
func NewUDP(ctx context.Context, cfg UDPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Datagram, error) {
	d := &Datagram{transport: item.TransportUDP, in: in, m: m, log: log,
		errLog: obs.NewSampler(30 * time.Second)}

	n := cfg.Listeners
	if n < 1 {
		n = 1
	}
	lc := net.ListenConfig{}
	if n > 1 {
		// The kernel hashes the 4-tuple across the sockets. The ring stays
		// single, so the drops remain fair across all of them.
		lc.Control = reuseControl
	}
	warned := false
	for i := 0; i < n; i++ {
		pc, err := lc.ListenPacket(ctx, "udp", cfg.Addr)
		if err != nil {
			d.closeAll()
			return nil, fmt.Errorf("receiver udp: %s: %w", cfg.Addr, err)
		}
		uc := pc.(*net.UDPConn)
		ip, port := addrPortOf(uc.LocalAddr())
		s := udpSocket{conn: uc, localIP: ip, localPort: port}
		d.udp = append(d.udp, s)

		if eff := d.setReadBuffer(uc, cfg.ReadBuffer); eff > 0 && (d.readBuf == 0 || eff < d.readBuf) {
			d.readBuf = eff
		}
		if err := enablePktinfo(uc); err != nil {
			if !warned {
				warned = true
				what := "omitted"
				if ip.IsValid() {
					what = "the listener's own address " + ip.String()
				}
				log.Warn("the platform will not report each datagram's destination address "+
					"(PKTINFO); dst-ip will be "+what,
					"transport", "udp", "addr", cfg.Addr, "error", err)
			}
		} else {
			d.udp[i].pktinfo = true
		}
	}
	if d.readBuf > 0 {
		m.UDPReadBufferBytes.Set(int64(d.readBuf))
	}
	return d, nil
}

// NewUnix creates the unix datagram socket (SPEC §4.3), taking care not to
// steal a live one.
func NewUnix(ctx context.Context, cfg UnixConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*Datagram, error) {
	path := cfg.Path
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("receiver unix: listen.unix %q must be an absolute path", path)
	}
	if len(path) > maxSunPath {
		return nil, fmt.Errorf("receiver unix: listen.unix %q has %d bytes; this platform's "+
			"sun_path holds %d", path, len(path), maxSunPath)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("receiver unix: %s: %w", path, err)
	}

	// A socket left by a previous run has to go before bind can succeed. But a
	// socket somebody is SERVING must not: dialing it tells the two apart, and
	// stealing /dev/log from a running syslogd is not a mistake to make
	// quietly. The orphan is removed only AFTER a connect failed (SPEC §4.3).
	removedStale, err := removeStaleSocket(path)
	if err != nil {
		return nil, err
	}
	if removedStale {
		log.Warn("removed a stale unix socket from a previous run", "path", path)
	}

	lc := net.ListenConfig{}
	pc, err := lc.ListenPacket(ctx, "unixgram", path)
	if err != nil {
		return nil, fmt.Errorf("receiver unix: %s: %w", path, err)
	}
	uc := pc.(*net.UnixConn)
	mode := cfg.Mode
	if mode == 0 {
		mode = 0o666
	}
	// 0666 is what /dev/log has on every system, and it has to be: any process
	// on the host needs to be able to log. A tighter mode silently disables
	// logging for everything that is not the owner.
	if err := os.Chmod(path, mode); err != nil {
		uc.Close()
		os.Remove(path)
		return nil, fmt.Errorf("receiver unix: chmod %s: %w", path, err)
	}
	d := &Datagram{transport: item.TransportUnix, in: in, m: m, log: log,
		unix: uc, path: path, errLog: obs.NewSampler(30 * time.Second)}
	d.readBuf = d.setReadBuffer(uc, cfg.ReadBuffer)
	return d, nil
}

// removeStaleSocket clears listen.unix for bind, and reports whether it
// removed anything.
//
// It removes ONLY a stale socket: the file must be a socket (Lstat, so a
// symlink is not followed) AND a unixgram connect to it must be REFUSED, which
// is what the kernel answers when nobody is bound there. Anything else is left
// alone and refused with a message that says what is there:
//
//   - the connect succeeds: a live datagram socket, someone else's /dev/log;
//   - EPROTOTYPE or similar: a live socket of another type (a SOCK_STREAM
//     /dev/log, as some systemd setups have), which a blanket "dial failed,
//     remove it" would have deleted from under its owner;
//   - not a socket at all: a regular file or a directory, which is a
//     configuration mistake and never ours to delete.
//
// ENOENT -- the file vanished between Lstat and connect -- needs no removal.
func removeStaleSocket(path string) (bool, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("receiver unix: %s: %w", path, err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return false, fmt.Errorf("receiver unix: %s exists and is not a socket (%s); "+
			"refusing to replace it", path, fi.Mode().Type())
	}
	c, derr := net.Dial("unixgram", path)
	switch {
	case derr == nil:
		c.Close()
		return false, fmt.Errorf("receiver unix: %s is already served by another "+
			"process; stop it or choose another path", path)
	case errors.Is(derr, syscall.ENOENT):
		return false, nil
	case !errors.Is(derr, syscall.ECONNREFUSED):
		return false, fmt.Errorf("receiver unix: %s is a socket that is not a stale "+
			"datagram socket (%v); refusing to remove it", path, derr)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("receiver unix: removing the stale socket %s: %w", path, err)
	}
	return true, nil
}

// setReadBuffer applies SO_RCVBUF and returns the effective value read back.
//
// This is the most important parameter of the datagram transports. What does
// not fit is dropped by the KERNEL, before this process sees it: the loss
// appears in /proc/net/udp and in the interface counters, and in no metric of
// ours. Linux caps the request at net.core.rmem_max WITHOUT an error, so the
// only way to know what was granted is to ask (SPEC §4.2). A shortfall is a
// warning, not an error -- the socket still works, just with less headroom.
//
// On Linux the value read back is twice what was granted (the kernel doubles
// it for its bookkeeping): it is exposed as is, because it is what the kernel
// reports, but the shortfall test divides it back (rcvbufReadbackFactor), or a
// request capped at half by net.core.rmem_max would read back as fully granted.
func (d *Datagram) setReadBuffer(c interface {
	SetReadBuffer(int) error
	SyscallConn() (syscall.RawConn, error)
}, want int) int {
	var setErr error
	if want > 0 {
		setErr = c.SetReadBuffer(want)
	}
	eff, getErr := socketReadBuffer(c)
	if want > 0 && (setErr != nil || (getErr == nil && eff/rcvbufReadbackFactor < want)) {
		d.log.Warn("the socket receive buffer is smaller than requested; the kernel "+
			"will drop datagrams sooner under load (raise net.core.rmem_max)",
			"transport", d.transport.String(), "want_bytes", want, "effective_bytes", eff,
			"error", setErr)
	}
	if getErr != nil {
		return 0
	}
	return eff
}

// Addr reports the bound address (the socket path for unix), for the boot log.
func (d *Datagram) Addr() string {
	if d.unix != nil {
		return d.path
	}
	if len(d.udp) == 0 {
		return ""
	}
	return d.udp[0].conn.LocalAddr().String()
}

// Transport is udp or unix.
func (d *Datagram) Transport() item.Transport { return d.transport }

// PktinfoEnabled reports whether every UDP socket reports each datagram's
// destination address. Always false for unix, which has no addresses.
func (d *Datagram) PktinfoEnabled() bool {
	if len(d.udp) == 0 {
		return false
	}
	for _, s := range d.udp {
		if !s.pktinfo {
			return false
		}
	}
	return true
}

// ReadBufferBytes is the effective SO_RCVBUF read back (the smallest across
// the sockets), or 0 when the platform cannot tell.
func (d *Datagram) ReadBufferBytes() int { return d.readBuf }

// Start launches one reader goroutine per socket.
func (d *Datagram) Start(ctx context.Context) {
	if d.unix != nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.unixLoop(ctx)
		}()
		return
	}
	for i := range d.udp {
		d.wg.Add(1)
		go func(s udpSocket) {
			defer d.wg.Done()
			d.udpLoop(ctx, s)
		}(d.udp[i])
	}
}

func (d *Datagram) udpLoop(ctx context.Context, s udpSocket) {
	// One buffer per goroutine, reused for every datagram. Accept copies what
	// it keeps into a pooled buffer, so this one never escapes.
	buf := make([]byte, udpReadBuffer)
	oob := make([]byte, oobBuffer)
	var w Wire

	for {
		n, oobn, _, src, err := s.conn.ReadMsgUDPAddrPort(buf, oob)
		// The one clock sample: recvmsg has just returned the datagram, and
		// nothing has looked at it (SPEC §3.1).
		now := time.Now()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			d.m.UDPReceiveErrorsTotal.Inc()
			if ok, suppressed := d.errLog.Allow(); ok {
				d.log.Error("datagram receive failed",
					"transport", "udp", "error", err, "suppressed", suppressed)
			}
			continue
		}

		dst := netip.Addr{}
		if s.pktinfo {
			dst = parsePktinfo(oob[:oobn])
		}
		if !dst.IsValid() {
			// No PKTINFO: the listener's own address when it is concrete;
			// a wildcard stays unknown and is omitted (SPEC §4.2).
			dst = s.localIP
		}
		w = Wire{
			Transport: item.TransportUDP,
			Framing:   item.FramingDatagram,
			SrcIP:     item.Canonical(src.Addr()),
			SrcPort:   src.Port(),
			DstIP:     item.Canonical(dst),
			DstPort:   s.localPort,
			// The whole datagram is read; Ingest cuts at max_message_bytes
			// and keeps this as the original size (SPEC §5.5).
			Size: n,
			Time: now,
		}

		// There is no backpressure to apply and nobody to answer, so the
		// counter IS the report (SPEC §2.3).
		d.in.RecordDrop(d.in.Accept(ctx, buf[:n], &w))
	}
}

func (d *Datagram) unixLoop(ctx context.Context) {
	// max(65536, max_message_bytes+1): one byte more than the limit, so a
	// datagram longer than the limit is always SEEN to be longer, even when
	// the kernel cut it (SPEC §4.3).
	size := d.in.MaxMessageBytes() + 1
	if size < udpReadBuffer {
		size = udpReadBuffer
	}
	buf := make([]byte, size)
	var w Wire

	for {
		n, _, _, _, err := d.unix.ReadMsgUnix(buf, nil)
		now := time.Now()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			if ok, suppressed := d.errLog.Allow(); ok {
				d.log.Error("datagram receive failed",
					"transport", "unix", "error", err, "suppressed", suppressed)
			}
			continue
		}
		// A datagram larger than buf comes back cut, with MSG_TRUNC, and n is
		// then only a LOWER BOUND of its real size: recvmsg does not report
		// the original length. size = n is still the honest record, and the
		// converter's "truncated" follows from size > len(raw), because Ingest
		// keeps max_message_bytes < n. The flag itself is not consulted: with
		// buf one byte past the limit, it changes nothing that n does not
		// already say.
		//
		// No network fields: the payload schema forbids them on unix, and the
		// envelope's sid is 0 there (SPEC §4.3, §6.10).
		w = Wire{
			Transport: item.TransportUnix,
			Framing:   item.FramingDatagram,
			Size:      n,
			Time:      now,
		}
		// The kernel already blocked the sender's send() before the datagram
		// reached us when the socket buffer was full, so the pressure existed
		// -- it just did not exist here.
		d.in.RecordDrop(d.in.Accept(ctx, buf[:n], &w))
	}
}

// Close stops the readers and removes the unix socket. It is idempotent.
func (d *Datagram) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.closeAll()
		d.wg.Wait()
	})
	return d.closeErr
}

func (d *Datagram) closeAll() error {
	var errs []error
	for _, s := range d.udp {
		errs = append(errs, s.conn.Close())
	}
	if d.unix != nil {
		errs = append(errs, d.unix.Close())
		// The socket file outlives the process otherwise, and the next boot
		// would have to decide whether it is stale.
		if err := os.Remove(d.path); err != nil && !os.IsNotExist(err) {
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

// enablePktinfo asks the kernel for each datagram's destination address, on
// whichever family the socket turned out to be (Go opens a dual-stack IPv6
// socket for ":port" and "0.0.0.0:port"; there, IPv4 datagrams arrive with an
// IPv4-mapped destination, which item.Canonical turns back into IPv4).
func enablePktinfo(c *net.UDPConn) error {
	rc, err := c.SyscallConn()
	if err != nil {
		return err
	}
	var serr error
	if err := rc.Control(func(fd uintptr) { serr = setPktinfo(fd) }); err != nil {
		return err
	}
	return serr
}

// socketReadBuffer reads SO_RCVBUF back.
func socketReadBuffer(c interface {
	SyscallConn() (syscall.RawConn, error)
}) (int, error) {
	rc, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var v int
	var gerr error
	if err := rc.Control(func(fd uintptr) { v, gerr = getRcvbuf(fd) }); err != nil {
		return 0, err
	}
	return v, gerr
}
