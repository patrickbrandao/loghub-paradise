package receiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/obs"
)

// udpReadBuffer is the userland buffer of one UDP reader: larger than any
// datagram (65 507 bytes over IPv4, 65 527 over IPv6), so the kernel never cuts
// one in silence and size is always the datagram's real length (SPEC §4.2).
// Nothing configurable belongs here: convert.max_message_bytes is checked on
// the decompressed document, after the read (SPEC §5.5).
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
	// ReassemblyMaxBytes and ReassemblyMaxMessages are
	// listen.udp_reassembly_max_bytes and _max_messages (SPEC §5.2).
	ReassemblyMaxBytes    int
	ReassemblyMaxMessages int
	// ChunkTimeout shortens the 5 s reassembly deadline; tests only.
	ChunkTimeout time.Duration
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

// Datagram receives GELF over UDP (SPEC §4.2): one document per datagram, or
// per message reassembled from chunks (SPEC §5.2), possibly gzip or zlib
// (SPEC §5.4). Build it with NewUDP.
//
// There is no backpressure and no confirmation -- a full ring, a down sink, a
// filtered level, a chunk lost on the network all end in a counted drop,
// because the protocol gives nobody to tell (SPEC §2.3).
//
// That is not this server being lossy. It is UDP. What the server CAN do is make
// the loss visible, which is why every drop increments a reason.
type Datagram struct {
	transport item.Transport
	in        *Ingest
	m         *obs.Metrics
	log       *obs.Logger

	udp     []udpSocket
	readBuf int // effective SO_RCVBUF, read back; 0 when unknown

	// chunks is shared by every socket of the listener: the chunks of one
	// message may arrive on different SO_REUSEPORT sockets only if the
	// kernel hashes them apart, which it does not for one 4-tuple, but one
	// reassembler is also what makes the ceilings global (SPEC §5.2).
	chunks *Reassembler
	// stop ends the sweeper.
	stop chan struct{}

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
		errLog: obs.NewSampler(30 * time.Second), stop: make(chan struct{}),
		chunks: NewReassembler(ReassemblerConfig{
			MaxBytes: cfg.ReassemblyMaxBytes, MaxMessages: cfg.ReassemblyMaxMessages,
			MaxMessageWire: WireLimit(in.MaxMessageBytes()), Timeout: cfg.ChunkTimeout,
		})}

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

// Addr reports the bound address, for the boot log.
func (d *Datagram) Addr() string {
	if len(d.udp) == 0 {
		return ""
	}
	return d.udp[0].conn.LocalAddr().String()
}

// Transport is udp.
func (d *Datagram) Transport() item.Transport { return d.transport }

// PktinfoEnabled reports whether every UDP socket reports each datagram's
// destination address.
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

// Start launches one reader goroutine per socket, and the reassembly sweeper.
func (d *Datagram) Start(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.sweep()
	}()
	for i := range d.udp {
		d.wg.Add(1)
		go func(s udpSocket) {
			defer d.wg.Done()
			d.udpLoop(ctx, s)
		}(d.udp[i])
	}
}

func (d *Datagram) udpLoop(ctx context.Context, s udpSocket) {
	// One buffer per goroutine for the datagram, one for a reassembled
	// message and one for the decompressed document, all reused. Accept
	// copies what it keeps into a pooled buffer, so none of them escapes.
	buf := make([]byte, udpReadBuffer)
	oob := make([]byte, oobBuffer)
	var whole, doc []byte
	var w Wire

	for {
		n, oobn, _, src, err := s.conn.ReadMsgUDPAddrPort(buf, oob)
		// The one clock sample: recvmsg has just returned the datagram, and
		// nothing has looked at it (SPEC §3.1). For a chunked message it is
		// the sample of the LAST chunk, the one that completes it.
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

		data := buf[:n]
		chunks := 0
		if IsChunk(data) {
			var res ChunkResult
			whole, chunks, res = d.chunks.Add(whole[:0], src, data, now)
			switch res {
			case ChunkComplete:
				data = whole
			case ChunkPending:
				d.m.UDPReassemblyBytes.Set(int64(d.chunks.Bytes()))
				continue
			case ChunkDuplicate:
				d.m.UDPChunkDuplicatesTotal.Inc()
				continue
			case ChunkSwallowed:
				continue
			case ChunkDropInvalid:
				d.in.Reject(item.TransportUDP, ChunkInvalid)
				continue
			case ChunkDropOverflow:
				d.in.Reject(item.TransportUDP, ChunkOverflow)
				continue
			case ChunkDropTooLarge:
				d.in.Reject(item.TransportUDP, TooLarge)
				continue
			}
			d.m.UDPReassemblyBytes.Set(int64(d.chunks.Bytes()))
		}

		comp := Detect(data)
		doc, err = Decompress(doc[:0], data, comp, d.in.MaxMessageBytes())
		if err != nil {
			d.in.Reject(item.TransportUDP, decompressOutcome(err))
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
			Transport:   item.TransportUDP,
			Compression: comp,
			Chunks:      chunks,
			SrcIP:       item.Canonical(src.Addr()),
			SrcPort:     src.Port(),
			DstIP:       item.Canonical(dst),
			DstPort:     s.localPort,
			Time:        now,
		}

		// There is no backpressure to apply and nobody to answer, so the
		// counter IS the report (SPEC §2.3).
		d.in.RecordDrop(d.in.Accept(ctx, doc, &w))
	}
}

// sweepEvery is how often the reassembly deadline is checked. A message
// expires between 5 and 6 s after its first chunk.
const sweepEvery = time.Second

// sweep expires incomplete messages on a clock of its own (SPEC §5.2): an
// idle server must not keep an orphan forever.
func (d *Datagram) sweep() {
	every := sweepEvery
	if d.chunks.cfg.Timeout < every {
		every = d.chunks.cfg.Timeout / 2
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case now := <-t.C:
			for n := d.chunks.Sweep(now); n > 0; n-- {
				d.in.Reject(item.TransportUDP, ChunkExpired)
			}
			d.m.UDPReassemblyBytes.Set(int64(d.chunks.Bytes()))
		}
	}
}

// Close stops the readers and the sweeper. It is idempotent. Messages still in
// reassembly are dropped without a count: they never arrived whole.
func (d *Datagram) Close() error {
	d.closeOnce.Do(func() {
		d.closeErr = d.closeAll()
		close(d.stop)
		d.wg.Wait()
	})
	return d.closeErr
}

func (d *Datagram) closeAll() error {
	var errs []error
	for _, s := range d.udp {
		errs = append(errs, s.conn.Close())
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
