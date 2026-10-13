package receiver

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
)

// Defaults of the [relp] section (SPEC §5, §16.5).
const (
	defaultWindow              = 128
	defaultMaxFrameBytes       = 16 << 20
	defaultBackpressureTimeout = 30 * time.Second
)

// relpWriteTimeout bounds one write of responses. librelp reads its socket
// all the time; a peer that does not take a few hundred bytes in this long is
// stalled, and its session ends (the answers it never read are retransmitted
// by it on the next session).
const relpWriteTimeout = 5 * time.Second

// relpReadBuffer is the bufio.Reader of one connection. The RELP reader only
// calls ReadByte and reads counted blocks, so 4 KiB is plenty; the 64 KiB of
// the syslog sibling cost every idle connection 64 KiB outside every ceiling
// (decision 11, SPEC §15.1).
const relpReadBuffer = 4 << 10

// relpWriteBuffer holds the responses being coalesced; one is 20-odd bytes.
const relpWriteBuffer = 4 << 10

// RELPConfig is the [relp] section, the protocol half of the transport (SPEC
// §5, §16.5).
type RELPConfig struct {
	// Window is relp.window: transactions in flight per connection before the
	// reader stops reading (128). Zero or negative selects 128.
	Window int
	// MaxFrameBytes is relp.max_frame_bytes: a DATALEN above it closes the
	// connection before a byte of DATA is read (16 MiB).
	MaxFrameBytes int64
	// BackpressureTimeout is relp.backpressure_timeout: how long one
	// transaction is held on a full ring or a DOWN sink before the
	// connection is closed WITHOUT an answer, so librelp retransmits it (30
	// s). Zero or negative selects 30 s; the configuration refuses zero.
	BackpressureTimeout time.Duration
	// Budget is relp.inflight_max_bytes, ONE value for the whole process.
	// nil gives the stream its own 64 MiB budget, which only tests want.
	Budget *Budget
	// Software is the relp_software offer: "go-relplog-pg-server,<version>",
	// so an operator reading an rsyslog log can tell which build is on the
	// other end.
	Software string
}

// Response bodies (SPEC §5.4). A 500 is FINAL for librelp: scrsp.c destroys
// the unacknowledged frame and calls onErr ("peer sent error response"), and
// the message is never retransmitted. So 500 goes only to what this server
// threw away on purpose, and its text says why. On this server that is the
// unaddressable guard and the client's protocol errors: the parent's
// "500 refused by the destination" answered a final refusal under ring=none,
// a mode this server does not have (SPEC §5.4).
const (
	rspOK              = "200 OK"
	rspUnaddressable   = "500 source address unusable"
	rspDisabled        = "500 command disabled"
	rspAlreadyOpen     = "500 protocol error: connection already open"
	rspNotSupported    = "500 command not supported"
	relpProtocolOffers = "\nrelp_version=0\nrelp_software="
)

// openResponse is the answer to "open": the offers librelp's own server
// sends, in the shape relpOffersToString writes them (one per line, no
// trailing LF). It is answered, not negotiated: this server speaks version 0
// and the syslog command, which is everything librelp needs (copen.c).
func openResponse(software string) string {
	// The offer value cannot carry a line break, which would start a new
	// offer; the version comes from the build and is sanitised anyway.
	software = strings.Map(func(r rune) rune {
		if r < 0x21 || r > 0x7e {
			return '_'
		}
		return r
	}, software)
	if software == "" {
		software = "go-relplog-pg-server"
	}
	return rspOK + relpProtocolOffers + software + "\ncommands=syslog"
}

// ack is one response on its way to the writer goroutine. close marks the
// serverclose frame.
type ack struct {
	txnr  int64
	body  string
	close bool
}

// relpSession is one RELP connection after its PROXY header: a reader, up to
// relp.window transactions in flight, and one writer.
//
// The window is why in-flight work is spread over goroutines instead of done
// inline. A transaction held on a full ring or a DOWN sink waits up to
// relp.backpressure_timeout, and an inline reader would make every frame
// behind it wait too, the ones the ring could take included; the parent's
// ring=sqlite made the same point with its group COMMIT. The window is the
// protocol's own answer: the client keeps up to its window unacknowledged, so
// the server may have that many in flight and answer them out of order,
// because the txnr travels in the response.
//
// The responses are written by a SEPARATE goroutine that only ever sees
// decided transactions. A shortcut that answered first and enqueued afterwards
// would turn the one reliable transport into the least reliable one.
type relpSession struct {
	s    *Stream
	c    net.Conn
	a    connAddrs
	cfg  RELPConfig
	open string

	// ctx ends the session's transactions: cancelled by the receive context
	// (shutdown), by abort (backpressure, a broken writer) and when the
	// reader ends. parent is the receive context alone, which tells the
	// shutdown apart from the rest.
	parent context.Context
	ctx    context.Context
	cancel context.CancelFunc

	acks       chan ack
	sem        chan struct{}
	inFlight   sync.WaitGroup
	writerDone chan struct{}

	opened bool

	// reason is why the session is being torn down, for the transactions it
	// leaves unanswered. Set once, by the first abort.
	reasonOnce sync.Once
	reason     atomic.Uint32
	// broken is set by the writer when the socket stops taking answers.
	broken atomic.Bool
}

// runRELP serves one connection until it ends. It returns after every
// transaction was decided or abandoned and every decided answer was handed
// to the socket; the caller then closes it.
func (s *Stream) runRELP(parent context.Context, c net.Conn, br *bufio.Reader, a connAddrs) {
	cfg := s.relp
	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	ss := &relpSession{
		s: s, c: c, a: a, cfg: cfg, open: s.openRsp,
		parent: parent, ctx: ctx, cancel: cancel,
		// window answers of transactions plus a little room for the control
		// answers the reader queues: a transaction's answer is never dropped
		// (settle blocks), and the writer drains the channel to the end even
		// after a failed write, so nothing can block forever.
		acks:       make(chan ack, window+4),
		sem:        make(chan struct{}, window),
		writerDone: make(chan struct{}),
	}
	go ss.writeLoop()

	closeHint := ss.readLoop(br)

	// Every transaction is decided or abandoned before the writer is told
	// there is nothing more: a "200 OK" for a message already in the ring
	// must reach the socket, or rsyslog retransmits a message this server
	// holds and the destination gets it twice under two uuids (SPEC §5.7).
	cancel()
	ss.inFlight.Wait()
	if closeHint && !ss.broken.Load() {
		// serverclose goes out AFTER every decided answer (SPEC §5.8).
		ss.acks <- ack{close: true}
	}
	close(ss.acks)
	<-ss.writerDone
}

// readLoop reads frames until the session ends. It reports whether the
// server, not the peer, ended it, in which case a serverclose hint is due.
func (ss *relpSession) readLoop(br *bufio.Reader) (closeHint bool) {
	s := ss.s
	rd := NewRELPReader(br, ss.cfg.MaxFrameBytes, s.in.MaxMessageBytes())
	for {
		if ss.ctx.Err() != nil {
			// An abort (backpressure, a broken writer) or the shutdown: the
			// frames still in the socket stay unread and unanswered, and
			// librelp retransmits them.
			return !ss.broken.Load()
		}
		armReadDeadline(ss.c, s.cfg.IdleTimeout, &s.closing)
		if ss.ctx.Err() != nil {
			// abort cancels FIRST and expires the deadline AFTER; a reader
			// that renewed it in between would park for a whole idle_timeout
			// (forever with 0). Checking after arming closes that window, the
			// same ordering armReadDeadline keeps with closing.
			_ = ss.c.SetReadDeadline(time.Now())
		}
		f, err := rd.ReadFrame()
		if err != nil {
			hint := s.relpFrameError(err, ss.a)
			if hint && ss.ctx.Err() == nil && !ss.parentDone() {
				// The server ended a session its peer is still on (a bad
				// frame, the idle timeout): what it leaves unanswered is not a
				// lost connection.
				ss.setReason(obs.UnansweredServerClosed)
			}
			return hint
		}
		// The one clock sample of the message: the last octet of the frame was
		// just read, and nothing has parsed it yet (SPEC §3.1).
		now := time.Now()
		s.m.RELPFramesTotal[f.Command].Inc()

		switch f.Command {
		case obs.RELPOpen:
			if ss.opened {
				// What librelp's server does (copen.c): refuse and end the
				// session. A second open is a client that lost track of
				// its own state.
				ss.send(f.Txnr, rspAlreadyOpen)
				ss.setReason(obs.UnansweredServerClosed)
				return true
			}
			ss.opened = true
			ss.send(f.Txnr, ss.open)

		case obs.RELPClose:
			// Everything in flight is decided and queued BEFORE the close
			// answer, so the client sees every response of the session. Then
			// the session ends, as librelp's server ends it (cclose.c): an
			// empty rsp and no serverclose.
			ss.inFlight.Wait()
			ss.send(f.Txnr, "")
			return false

		case obs.RELPSyslog:
			if !ss.opened {
				// librelp's server answers the same before an open
				// (scsyslog.c), and the session goes on.
				ss.send(f.Txnr, rspDisabled)
				continue
			}
			if !ss.dispatch(&f, now) {
				return !ss.broken.Load()
			}

		default:
			// An unknown command is a client extension this server does not
			// implement, not a broken stream: 500, and the connection lives
			// on.
			ss.send(f.Txnr, rspNotSupported)
		}
	}
}

// dispatch hands one syslog frame to a transaction goroutine. It waits for a
// slot in the window and for the bytes of the copy in the process-wide budget,
// in that order, and only then copies: the copy is what both ceilings bound
// (SPEC §5.6). It returns false when the session ended while waiting.
func (ss *relpSession) dispatch(f *RELPFrame, now time.Time) bool {
	s := ss.s
	select {
	case ss.sem <- struct{}{}:
	case <-ss.ctx.Done():
		ss.notDispatched()
		return false
	}
	charged, ok := s.budget.Acquire(ss.ctx, txnCharge(len(f.Data)))
	if !ok || ss.ctx.Err() != nil {
		// A select with two ready cases picks at random, and Acquire looks at
		// the budget before the context: the session may have ended while
		// this frame waited, and its transaction must not start then -- it
		// would be enqueued behind the back of librelp's retransmission.
		s.budget.Release(charged)
		<-ss.sem
		ss.notDispatched()
		return false
	}

	// The frame buffer is reused by the next ReadFrame, so a transaction
	// that outlives this iteration needs its own copy: the one copy RELP pays
	// that a TCP reader does not, and the price of the window.
	raw, class := item.CopyRaw(f.Data)
	txnr, size := f.Txnr, f.Size

	ss.inFlight.Add(1)
	s.m.RELPInflightTransactions.Inc()
	go func() {
		defer func() {
			item.PutRaw(raw, class)
			s.budget.Release(charged)
			s.m.RELPInflightTransactions.Dec()
			<-ss.sem
			ss.inFlight.Done()
		}()
		w := Wire{
			Transport: s.transport,
			Framing:   item.FramingOctetCounting,
			SrcIP:     ss.a.src.Addr(),
			SrcPort:   ss.a.src.Port(),
			DstIP:     ss.a.dst.Addr(),
			DstPort:   ss.a.dst.Port(),
			Size:      size,
			Time:      now,
		}
		if ss.a.proxied {
			w.PeerIP, w.PeerPort = ss.a.peer.Addr(), ss.a.peer.Port()
		}
		if body, answered := ss.decide(raw, &w); answered {
			ss.settle(txnr, body)
		} else {
			ss.unanswered()
		}
	}()
	return true
}

// decide runs the pipeline for one transaction and returns its answer, or
// answered=false when it must get none (SPEC §5.4, §5.7).
//
// This is where the whole promise lives: "200 OK" is returned ONLY after
// Ingest has come back Accepted, which means the ring's guarantee is met.
//
// A refusal that will clear on its own -- a full ring, a DOWN sink -- is HELD,
// re-tried every 50 ms, for relp.backpressure_timeout. It is never answered
// with a 500: librelp drops what it gets a 500 for. When the
// hold runs out, the session is aborted and the transaction left unanswered:
// the broken session makes librelp reconnect and retransmit everything it has
// not seen a 200 for, which is the only way to hand the message back.
func (ss *relpSession) decide(raw []byte, w *Wire) (string, bool) {
	s := ss.s
	held := w.Time
	out := s.in.Accept(ss.ctx, raw, w)
	if body, answered, final := ss.verdict(out, held); final {
		return body, answered
	}

	start := time.Now()
	deadline := start.Add(ss.backpressureTimeout())
	timer := time.NewTimer(backpressurePoll)
	defer timer.Stop()
	defer func() { s.m.AddBackpressure(s.obsT, time.Since(start)) }()
	for {
		select {
		case <-ss.ctx.Done():
			return "", false
		case <-timer.C:
			timer.Reset(backpressurePoll)
		}
		// Both cases may have been ready -- a Retry slower than the poll
		// returns with the timer already fired -- and a select picks at
		// random: a session that ended meanwhile must not get its held
		// transaction enqueued now, or the retransmission duplicates it
		// (independent review of go-relplog-server, 2026-10-11).
		if ss.ctx.Err() != nil {
			return "", false
		}
		// Retry does not count the arrival again: Accept already did.
		out = s.in.Retry(ss.ctx, raw, w)
		if body, answered, final := ss.verdict(out, held); final {
			return body, answered
		}
		if time.Now().After(deadline) {
			ss.abort(obs.UnansweredBackpressure)
			if ok, suppressed := s.bpLog.Allow(); ok {
				s.log.Warn("closing a RELP session after holding a transaction at "+
					"relp.backpressure_timeout; the emitter will reconnect and retransmit",
					"transport", s.transport.String(), "peer", ss.a.peer.String(),
					"held", time.Since(start).String(), "reason", out.String(),
					"suppressed", suppressed)
			}
			return "", false
		}
	}
}

// verdict maps an outcome to an answer. final is false for the outcomes that
// are held (retryable); answered is false for the ones that must get none.
func (ss *relpSession) verdict(out Outcome, held time.Time) (body string, answered, final bool) {
	s := ss.s
	switch out {
	case Accepted:
		s.m.ConfirmDuration.ObserveDuration(time.Since(held))
		return rspOK, true, true
	case Empty:
		// An empty message has nothing to keep: telling the emitter it failed
		// would be a lie, and rsyslog would log an error per message.
		s.in.RecordDrop(out)
		return rspOK, true, true
	case Unaddressable:
		s.in.RecordDrop(out)
		return rspUnaddressable, true, true
	case Shutdown:
		// The ring is closing or the session was cancelled before the item
		// got in: the emitter keeps it.
		return "", false, true
	}
	return "", false, false // RingFull, SinkDown: hold
}

func (ss *relpSession) backpressureTimeout() time.Duration {
	if ss.cfg.BackpressureTimeout > 0 {
		return ss.cfg.BackpressureTimeout
	}
	return defaultBackpressureTimeout
}

// abort ends the session from inside: no more frames are read and every
// transaction still undecided is left unanswered. The first reason wins.
func (ss *relpSession) abort(r obs.RELPUnanswered) {
	ss.setReason(r)
	ss.cancel()
	// The reader may be parked in ReadFrame; an expired read deadline is what
	// gets it out.
	_ = ss.c.SetReadDeadline(time.Now())
}

// notDispatched counts a syslog frame that was read and never became a
// transaction because the session ended while it waited for the window or
// the budget. It arrived, so it counts in messages_total like every other
// frame -- the reconciliation of SPEC §5.4 holds only if it does -- and it got
// no answer.
func (ss *relpSession) notDispatched() {
	ss.s.m.MessagesTotal[ss.s.obsT].Inc()
	ss.unanswered()
}

// setReason records why the session ends, for the transactions it leaves
// unanswered. The first reason wins.
func (ss *relpSession) setReason(r obs.RELPUnanswered) {
	ss.reasonOnce.Do(func() { ss.reason.Store(uint32(r) + 1) })
}

// unanswered counts one transaction that got no response, by the reason the
// session ended.
func (ss *relpSession) unanswered() {
	r := obs.UnansweredConnLost
	if v := ss.reason.Load(); v > 0 {
		r = obs.RELPUnanswered(v - 1)
	} else if ss.s.closing.Load() || ss.parentDone() {
		r = obs.UnansweredShutdown
	}
	ss.s.m.RELPUnansweredTotal[r].Inc()
}

// parentDone reports whether the receive context -- the shutdown -- ended.
// The session's own ctx is also cancelled by abort and by the reader's end,
// so it cannot tell the shutdown apart by itself.
func (ss *relpSession) parentDone() bool { return ss.parent.Err() != nil }

// send queues a control answer from the reader. It blocks rather than giving
// up when the session is going away: a select on ctx.Done would pick at random
// between two ready cases and could drop the answer to a close the client is
// waiting for. Blocking cannot last: the writer drains the channel to its end,
// and every write it makes has a deadline.
func (ss *relpSession) send(txnr int64, body string) { ss.acks <- ack{txnr: txnr, body: body} }

// settle queues the answer to a transaction that has been DECIDED, and never
// gives up.
//
// This is the difference that matters at shutdown. A transaction whose item is
// already in the ring must get its "200 OK" out; dropping the answer because
// the context was cancelled would make rsyslog retransmit a message the server
// already holds -- and since RELP carries no message id of its own, the
// retransmission becomes a second event with a second uuid. The writer drains
// the channel to its end even after a failed write, so this cannot block
// forever.
func (ss *relpSession) settle(txnr int64, body string) { ss.acks <- ack{txnr: txnr, body: body} }

// writeLoop serialises every answer onto the socket. It is the only writer of
// the connection, so answers never interleave and a slow write never stalls
// the reader.
func (ss *relpSession) writeLoop() {
	defer close(ss.writerDone)
	m := ss.s.m
	bw := bufio.NewWriterSize(ss.c, relpWriteBuffer)
	buf := make([]byte, 0, 256)
	dirty := false

	flush := func() {
		if !dirty || ss.broken.Load() {
			return
		}
		dirty = false
		_ = ss.c.SetWriteDeadline(time.Now().Add(relpWriteTimeout))
		if err := bw.Flush(); err != nil {
			ss.brokenWriter()
			return
		}
		// Between batches the write deadline goes back to the idle timeout,
		// as in the parent go-relplog-server, where crypto/tls writes from
		// inside Read (a KeyUpdate answer). Without TLS this writer is the
		// only one, and the reset is merely harmless: kept so the session is
		// the parent's line for line.
		ss.idleWriteDeadline()
	}

	for a := range ss.acks {
		if ss.broken.Load() {
			// The socket is gone, but transactions may still be settling.
			// Draining to the end is what keeps them from blocking forever.
			continue
		}
		if a.close {
			buf = append(buf[:0], serverClose...)
		} else {
			buf = AppendRSP(buf[:0], a.txnr, a.body)
			switch {
			case strings.HasPrefix(a.body, "200"):
				m.RELPResponsesTotal[obs.RELPCode200].Inc()
			case strings.HasPrefix(a.body, "500"):
				m.RELPResponsesTotal[obs.RELPCode500].Inc()
			}
		}
		if bw.Available() < len(buf) {
			flush()
			if ss.broken.Load() {
				continue
			}
		}
		_, _ = bw.Write(buf) // fits: the buffer was flushed above when it did not
		dirty = true
		// Flush when nothing else is decided, so a lone answer is not held
		// waiting for company. Under load the channel stays non-empty and the
		// writes coalesce on their own.
		if len(ss.acks) == 0 {
			flush()
		}
	}
	flush()
}

// brokenWriter records that the socket stopped taking answers: the session is
// over, and whatever is still undecided is left to librelp's retransmission.
func (ss *relpSession) brokenWriter() {
	ss.broken.Store(true)
	ss.abort(obs.UnansweredConnLost)
}

func (ss *relpSession) idleWriteDeadline() {
	if d := ss.s.cfg.IdleTimeout; d > 0 {
		_ = ss.c.SetWriteDeadline(time.Now().Add(d))
	} else {
		_ = ss.c.SetWriteDeadline(time.Time{})
	}
}

// relpFrameError logs and counts what ended the reader, and reports whether a
// serverclose hint is due: yes when the server ends a session the peer is
// still on (a bad frame, the idle timeout, the shutdown), no when the peer is
// gone.
func (s *Stream) relpFrameError(err error, a connAddrs) bool {
	switch {
	case errors.Is(err, io.EOF):
		return false // the peer closed cleanly between frames
	case errors.Is(err, ErrFrameTooLarge):
		s.m.RELPFrameErrorsTotal[obs.FrameTooLarge].Inc()
		if ok, suppressed := s.frameLog.Allow(); ok {
			s.log.Warn("closing a RELP connection whose DATALEN passed relp.max_frame_bytes",
				"transport", s.transport.String(), "peer", a.peer.String(), "suppressed", suppressed)
		}
		return true
	case errors.Is(err, ErrRELPFrame):
		s.m.RELPFrameErrorsTotal[obs.FrameMalformed].Inc()
		if ok, suppressed := s.frameLog.Allow(); ok {
			s.log.Warn("closing a RELP connection with a malformed frame; there is no way "+
				"to find the next frame boundary",
				"transport", s.transport.String(), "peer", a.peer.String(), "suppressed", suppressed)
		}
		return true
	case errors.Is(err, io.ErrUnexpectedEOF):
		// The peer went away in the middle of a frame. That frame was never
		// answered, so librelp retransmits it; nothing of it is kept.
		return false
	case isTimeout(err):
		// The idle timeout, the shutdown's expired deadline, or an abort.
		return true
	default:
		s.log.Debug("RELP connection ended", "transport", s.transport.String(),
			"peer", a.peer.String(), "error", err)
		return false
	}
}

// armReadDeadline sets the READ deadline for the next frame, without undoing
// a shutdown.
//
// Only the read side: the writer goroutine owns the write deadline. Close
// stops a reader by setting its read deadline to "now"; a reader between two
// frames at that instant would otherwise renew it for another idle_timeout. So
// the reader sets its deadline FIRST and checks closing AFTER, and Close sets
// closing FIRST and the deadline AFTER: in every interleaving the last value
// written is "now". A zero d clears the deadline: idle_timeout = 0 means never.
func armReadDeadline(c net.Conn, d time.Duration, closing *atomic.Bool) {
	if d > 0 {
		_ = c.SetReadDeadline(time.Now().Add(d))
	} else {
		_ = c.SetReadDeadline(time.Time{})
	}
	if closing.Load() {
		_ = c.SetReadDeadline(time.Now())
	}
}
