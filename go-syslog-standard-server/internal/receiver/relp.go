package receiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/envelope"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/obs"
)

// RELPConfig mirrors [listen] and [relp].
type RELPConfig struct {
	Addr                string
	Listeners           int
	MaxFrameBytes       int64
	IdleTimeout         time.Duration
	MaxConns            int
	MaxConnsPerIP       int
	Window              int
	BackpressureTimeout time.Duration

	// Version is advertised in the open handshake, so an operator reading an
	// rsyslog log can tell which build is on the other end.
	Version string
}

// RELP receives librelp streams (SPEC §4.5).
//
// It is the only transport here that can CONFIRM, so it is the only one where the
// acceptance rule of this project holds literally:
//
//	The rsp 200 OK frame goes out only after the ring's guarantee is met.
//
// A shortcut that answered first and enqueued afterwards would turn the one
// reliable transport of this server into the least reliable one, which is why the
// acknowledgement is written by a separate goroutine that only ever sees
// already-decided transactions.
type RELP struct {
	cfg RELPConfig
	in  *Ingest
	m   *obs.Metrics
	log *obs.Logger

	lns []net.Listener
	wg  sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	// closing is atomic because the read loop checks it on every frame without
	// taking mu. See armDeadline for why that check is needed at all.
	closing atomic.Bool
}

func NewRELP(ctx context.Context, cfg RELPConfig, in *Ingest, m *obs.Metrics, log *obs.Logger) (*RELP, error) {
	r := &RELP{cfg: cfg, in: in, m: m, log: log, conns: map[net.Conn]struct{}{}}
	n := cfg.Listeners
	if n < 1 {
		n = 1
	}
	// One limiter for every listener of this transport: the ceilings are per
	// transport, not per listener (SPEC §4.4).
	limiter := newConnLimiter(cfg.MaxConns, cfg.MaxConnsPerIP)
	for i := 0; i < n; i++ {
		l, err := listenStream(ctx, cfg.Addr, n > 1)
		if err != nil {
			r.closeListeners()
			return nil, fmt.Errorf("receiver relp: %s: %w", cfg.Addr, err)
		}
		r.lns = append(r.lns, limiter.wrap(l))
	}
	return r, nil
}

func (r *RELP) Addr() string {
	if len(r.lns) == 0 {
		return r.cfg.Addr
	}
	return r.lns[0].Addr().String()
}

func (r *RELP) Start(ctx context.Context) {
	listener := r.Addr()
	for i := range r.lns {
		r.wg.Add(1)
		go func(l net.Listener) {
			defer r.wg.Done()
			r.acceptLoop(ctx, l, listener)
		}(r.lns[i])
	}
}

func (r *RELP) acceptLoop(ctx context.Context, l net.Listener, listener string) {
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			r.log.Warn("accept failed", "transport", "relp", "error", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !r.track(c) {
			c.Close()
			continue
		}
		r.m.ConnsTotal[obs.TransportRELP].Inc()
		r.m.ConnsActive[obs.TransportRELP].Inc()

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer r.m.ConnsActive[obs.TransportRELP].Dec()
			defer r.untrack(c)
			r.serve(ctx, c, listener)
		}()
	}
}

func (r *RELP) track(c net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing.Load() {
		return false
	}
	r.conns[c] = struct{}{}
	return true
}

func (r *RELP) untrack(c net.Conn) {
	r.mu.Lock()
	delete(r.conns, c)
	r.mu.Unlock()
	c.Close()
}

// ack is one decided transaction on its way to the writer goroutine.
type ack struct {
	txnr int64
	body string
}

// serve runs one connection: a reader, a bounded set of in-flight transactions,
// and one writer.
//
// The window is why the in-flight work is spread over goroutines instead of being
// done inline. With ring=sqlite an Enqueue waits for a group COMMIT, so an inline
// reader would cap a single connection at roughly two hundred messages a second
// -- and rsyslog opens exactly one. The window is the protocol's own answer to
// that: the client keeps up to relp.window transactions unacknowledged, so we may
// have that many in flight and acknowledge them out of order, because the txnr
// travels in the response.
func (r *RELP) serve(ctx context.Context, c net.Conn, listener string) {
	if tc, ok := tcpConnOf(c); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	window := r.cfg.Window
	if window < 1 {
		window = 1
	}

	var (
		br         = bufio.NewReaderSize(c, tcpReadBuffer)
		rd         = NewRELPReader(br, int(r.cfg.MaxFrameBytes))
		acks       = make(chan ack, window)
		sem        = make(chan struct{}, window)
		inFlight   sync.WaitGroup
		writerDone = make(chan struct{})
	)

	addr, port := peerOf(c)

	// The writer owns the socket for writing, so acknowledgements never
	// interleave and a slow write cannot stall a reader.
	go func() {
		defer close(writerDone)
		r.writeLoop(c, acks)
	}()
	defer func() {
		inFlight.Wait() // every transaction has been decided
		close(acks)     // let the writer flush what is decided
		<-writerDone
	}()

	for {
		armDeadline(c, r.cfg.IdleTimeout, &r.closing)
		f, err := rd.ReadFrame()
		if err != nil {
			r.logFrameError(err, addr)
			return
		}

		switch f.Command {
		case "open":
			r.m.RELPFramesTotal[obs.RELPOpen].Inc()
			// The offer is answered, not negotiated: this server speaks version
			// 0 and the syslog command, which is everything librelp needs.
			send(ctx, acks, ack{f.Txnr, "200 OK\nrelp_version=0\nrelp_software=go-syslog-server," +
				r.cfg.Version + "\ncommands=syslog\n"})

		case "close":
			r.m.RELPFramesTotal[obs.RELPClose].Inc()
			send(ctx, acks, ack{f.Txnr, ""})
			// serverclose is txnr 0 by protocol. It is queued behind the
			// response above, so the client sees both in order.
			send(ctx, acks, ack{0, ""})
			return

		case "syslog":
			r.m.RELPFramesTotal[obs.RELPSyslog].Inc()
			if len(f.Data) == 0 {
				// An empty transaction is still a transaction and still needs an
				// answer, or the client's window never reopens.
				send(ctx, acks, ack{f.Txnr, "200 OK"})
				continue
			}

			// The frame buffer is reused by the next ReadFrame, so a transaction
			// that outlives this iteration needs its own copy. It is the one
			// copy RELP pays that the other transports do not, and it buys the
			// window.
			raw := make([]byte, len(f.Data))
			copy(raw, f.Data)

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			inFlight.Add(1)
			go func(txnr int64, raw []byte) {
				defer inFlight.Done()
				defer func() { <-sem }()
				body := r.accept(ctx, raw, Wire{
					Transport: envelope.TransportRELP,
					Framing:   envelope.FramingRELP,
					Listener:  listener,
					PeerAddr:  addr,
					PeerPort:  port,
				})
				settle(acks, ack{txnr, body})
			}(f.Txnr, raw)

		default:
			// An unknown command gets 500 and the connection lives on: librelp
			// only ever sends the three above, so this is a client extension we
			// do not implement, not a broken stream.
			r.m.RELPFramesTotal[obs.RELPUnknown].Inc()
			send(ctx, acks, ack{f.Txnr, "500 command not supported"})
		}
	}
}

// accept runs the pipeline and returns the response body.
//
// This is where the whole promise lives: "200 OK" is returned ONLY after
// Ingest.Accept has come back accepted, which means the ring's guarantee is met.
//
// A refusal that will clear on its own is held for backpressure_timeout first,
// exactly like TCP -- but here, instead of silence, the client gets a 500 with the
// reason at the end of the wait, and the connection stays alive. rsyslog retains
// the message and retries, which is precisely the behaviour wanted.
func (r *RELP) accept(ctx context.Context, raw []byte, w Wire) string {
	// From the frame being whole to the 200 OK being decided (SPEC §13.1).
	held := time.Now()
	out := r.in.Accept(ctx, raw, w)
	if out == Accepted {
		r.m.ConfirmDuration.ObserveDuration(time.Since(held))
		r.m.RELPAcksTotal.Inc()
		return "200 OK"
	}
	if !out.Retryable() {
		r.in.RecordDrop(out)
		// A filtered message was thrown away because the operator asked for it,
		// so telling the emitter to retry would be a lie. It is acknowledged.
		if out == Filtered {
			r.m.RELPAcksTotal.Inc()
			return "200 OK"
		}
		return "500 " + out.String()
	}

	start := time.Now()
	deadline := start.Add(r.cfg.BackpressureTimeout)
	for {
		select {
		case <-ctx.Done():
			r.in.RecordDrop(Shutdown)
			return "500 shutdown"
		case <-time.After(backpressurePoll):
		}

		out = r.in.Retry(ctx, raw, w)
		if out == Accepted {
			r.m.AddBackpressure(obs.TransportRELP, time.Since(start))
			r.m.ConfirmDuration.ObserveDuration(time.Since(held))
			r.m.RELPAcksTotal.Inc()
			return "200 OK"
		}
		if !out.Retryable() || time.Now().After(deadline) {
			r.m.AddBackpressure(obs.TransportRELP, time.Since(start))
			r.in.RecordDrop(out)
			return "500 " + out.String()
		}
	}
}

// writeLoop serialises every response onto the socket.
func (r *RELP) writeLoop(c net.Conn, acks <-chan ack) {
	bw := bufio.NewWriterSize(c, 16<<10)
	buf := make([]byte, 0, 256)

	flush := func() bool {
		if err := bw.Flush(); err != nil {
			return false
		}
		return true
	}

	broken := false
	for a := range acks {
		if broken {
			// The socket is gone, but settle may still be delivering answers.
			// Draining to the end is what keeps it from blocking forever.
			continue
		}
		body := a.body
		if a.txnr == 0 && body == "" {
			// txnr 0 with no body is the serverclose frame.
			buf = append(buf[:0], "0 serverclose 0 \n"...)
		} else {
			buf = AppendRSP(buf[:0], a.txnr, body)
		}
		_ = c.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := bw.Write(buf); err != nil {
			broken = true
			continue
		}
		// Flush when nothing else is decided, so a lone acknowledgement is not
		// held waiting for company. Under load the channel stays non-empty and
		// the writes coalesce on their own.
		if len(acks) == 0 && !flush() {
			broken = true
		}
	}
	if !broken {
		flush()
	}
}

// send queues a control response, giving up if the connection is going away.
func send(ctx context.Context, acks chan<- ack, a ack) {
	select {
	case acks <- a:
	case <-ctx.Done():
	}
}

// settle queues the answer to a transaction that has been DECIDED, and never
// gives up.
//
// This is the difference that matters at shutdown. A transaction whose item is
// already in the ring must get its "200 OK" out; dropping the answer because the
// context was cancelled would make rsyslog retransmit a message we already hold,
// and since RELP carries no message id of its own, the retransmission becomes a
// second event with a second id in the destination. The channel is sized so this
// can never block, and the writer drains it to the end even after a failed
// write.
func settle(acks chan<- ack, a ack) { acks <- a }

func (r *RELP) logFrameError(err error, addr netip.Addr) {
	switch {
	case errors.Is(err, io.EOF):
		return
	case errors.Is(err, ErrFrameTooLarge):
		r.m.MessagesDroppedTotal[obs.DropFrameTooLarge].Inc()
		r.log.Warn("closing a RELP connection whose frame passed relp.max_frame_bytes",
			"peer", addr.String())
	case errors.Is(err, ErrRELPFrame), errors.Is(err, io.ErrUnexpectedEOF):
		r.m.MessagesDroppedTotal[obs.DropMalformedFrame].Inc()
		r.log.Warn("closing a RELP connection with a malformed frame",
			"peer", addr.String(), "error", err)
	default:
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return
		}
		r.log.Debug("RELP connection ended", "peer", addr.String(), "error", err)
	}
}

// Close stops accepting and stops READING, and waits for every connection to
// finish what it already decided.
//
// It does not close the sockets itself. An expired read deadline breaks each
// reader out of ReadFrame; serve then waits for its in-flight transactions, lets
// the writer flush their answers, and only then closes the connection. Closing
// the socket here instead would discard "200 OK" frames for messages already in
// the ring, and each of those becomes a duplicate event when rsyslog retransmits.
//
// A reader held in the backpressure loop is released by the cancelled receive
// context, and answers "500 shutdown" -- which is exactly what SPEC §12.3 asks of
// this transport in phase 3.
func (r *RELP) Close() error {
	r.mu.Lock()
	r.closing.Store(true)
	conns := make([]net.Conn, 0, len(r.conns))
	for c := range r.conns {
		conns = append(conns, c)
	}
	r.mu.Unlock()

	err := r.closeListeners()
	for _, c := range conns {
		_ = c.SetReadDeadline(time.Now())
	}
	r.wg.Wait()
	return err
}

func (r *RELP) closeListeners() error {
	var errs []error
	for _, l := range r.lns {
		errs = append(errs, l.Close())
	}
	return errors.Join(errs...)
}
