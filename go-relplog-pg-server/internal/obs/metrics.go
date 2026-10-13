package obs

import (
	"io"
	"strconv"
	"strings"
	"time"
)

// Label value sets. Every one is finite and known at compile time (SPEC §14.1):
// no label ever derives from a hostname, an appname or an IP, because those come
// from arbitrary emitters and would take the Prometheus down rather than the
// server. facility and severity are NOT labels either: the product with
// transport would be 768 series per counter, and the distribution by severity
// is a question for the destination, where it is an indexed column.

// Transport mirrors item.Transport; the duplication is deliberate, so obs
// imports nothing of this module.
type Transport uint8

const (
	TransportUDP Transport = iota
	TransportTCP
	TransportUnix
	NumTransports
)

var transportNames = [NumTransports]string{"udp", "tcp", "unix"}

// streamTransports is the one this server listens on: RELP in clear (tcp).
// There is no TLS here (SPEC §1.2). The enumeration keeps udp and unix because
// it mirrors item.Transport, whose converter speaks the transports of the
// skill (SPEC §3.4); a series that can never rise is not exposed, so only tcp
// is rendered.
var streamTransports = [...]Transport{TransportTCP}

func (t Transport) String() string {
	if int(t) < len(transportNames) {
		return transportNames[t]
	}
	return "unknown"
}

// DropReason is why a message that was READ never became an accepted item and
// was not handed back to the emitter either: a deliberate, final discard.
//
// What RELP hands back is NOT here. A transaction left unanswered when the
// connection closes -- held past relp.backpressure_timeout, caught by a
// shutdown, or orphaned by a vanished peer -- is retransmitted by librelp on
// the next session, so it is not a loss; it is counted in
// relplog_relp_unanswered_total (SPEC §5.7, §14.1). A ring that is full or a
// sink that is DOWN therefore never shows up as a drop on this server.
type DropReason uint8

// There is no filter and no rate limiter in this server (SPEC §1.2), so the
// only deliberate discards are an empty frame and the unaddressable guard.
const (
	DropEmpty DropReason = iota
	DropUnaddressable
	numDropReasons
)

var dropNames = [numDropReasons]string{"empty", "unaddressable"}

func (d DropReason) String() string {
	if int(d) < len(dropNames) {
		return dropNames[d]
	}
	return "unknown"
}

// RELPCommand is the command of a frame the client sent (SPEC §5.1). Every
// command the server does not implement is "unknown": the label never carries
// the client's word.
type RELPCommand uint8

const (
	RELPOpen RELPCommand = iota
	RELPSyslog
	RELPClose
	RELPUnknown
	numRELPCommands
)

var relpCommandNames = [numRELPCommands]string{"open", "syslog", "close", "unknown"}

// RELPCode is the status code of a response this server sent.
type RELPCode uint8

const (
	RELPCode200 RELPCode = iota
	RELPCode500
	numRELPCodes
)

var relpCodeNames = [numRELPCodes]string{"200", "500"}

// RELPUnanswered is why a syslog transaction that was read got no response at
// all: the connection ended first, and librelp retransmits it (SPEC §5.7).
type RELPUnanswered uint8

const (
	// UnansweredBackpressure: the ring stayed full (or the sink DOWN) past
	// relp.backpressure_timeout and the server closed the connection.
	UnansweredBackpressure RELPUnanswered = iota
	// UnansweredShutdown: the shutdown stopped the transaction before it was
	// decided.
	UnansweredShutdown
	// UnansweredConnLost: the peer went away, or the answers could no longer
	// be written, before it was decided.
	UnansweredConnLost
	// UnansweredServerClosed: the server ended a session its peer was still
	// on -- a malformed or too-large frame, the idle timeout, a second open.
	UnansweredServerClosed
	numRELPUnanswered
)

var relpUnansweredNames = [numRELPUnanswered]string{"backpressure", "shutdown", "conn_lost", "server_closed"}

// RELPFrameError is why a connection was closed on a frame that cannot be
// resynchronised (SPEC §5.1, §5.2).
type RELPFrameError uint8

const (
	FrameMalformed RELPFrameError = iota
	FrameTooLarge
	numRELPFrameErrors
)

var relpFrameErrorNames = [numRELPFrameErrors]string{"malformed", "too_large"}

// EventDropReason is why an ACCEPTED item was lost after the fact. It is a
// separate enumeration on purpose: one shared set would expose series that can
// never move, telling an operator a measurement exists where none does.
type EventDropReason uint8

const (
	EventDropSinkLogic EventDropReason = iota
	EventDropMalformed
	EventDropEmergencyFailed
	EventDropAbort
	numEventDropReasons
)

var eventDropNames = [numEventDropReasons]string{"sink_logic", "malformed", "emergency_failed", "abort"}

func (d EventDropReason) String() string {
	if int(d) < len(eventDropNames) {
		return eventDropNames[d]
	}
	return "unknown"
}

// Format mirrors convert.Format.
type Format uint8

const (
	FormatRFC5424 Format = iota
	FormatRFC3164
	FormatMalformed
	NumFormats
)

var formatNames = [NumFormats]string{"rfc5424", "rfc3164", "malformed"}

// NumWarnings mirrors convert.NumWarnings; WarningNames are the labels, in the
// same order as the converter's codes.
const NumWarnings = 14

var WarningNames = [NumWarnings]string{
	"pri-missing", "pri-invalid", "version-unsupported", "header-invalid",
	"timestamp-missing", "timestamp-invalid", "timestamp-year-inferred",
	"timestamp-zone-inferred", "hostname-missing", "sd-invalid",
	"sd-duplicate-id", "encoding-invalid", "encoding-legacy", "truncated",
}

// SinkErrKind classifies a sink failure.
type SinkErrKind uint8

const (
	SinkErrNetwork SinkErrKind = iota
	SinkErrLogic
	SinkErrFatal
	SinkErrTimeout
	numSinkErrKinds
)

var sinkErrNames = [numSinkErrKinds]string{"network", "logic", "fatal", "timeout"}

// ConnLimit is why a connection was refused before its first frame: one of the
// two ceilings, or the PROXY protocol rules (SPEC §4.4, §4.6).
type ConnLimit uint8

const (
	ConnLimitGlobal ConnLimit = iota
	ConnLimitPerIP
	ConnRefusedProxy
	numConnLimits
)

var connLimitNames = [numConnLimits]string{"global", "per_ip", "proxy"}

// ProxyResult is what the PROXY protocol reader found (SPEC §4.6).
type ProxyResult uint8

const (
	ProxyV1 ProxyResult = iota
	ProxyV2
	ProxyLocal
	ProxyAbsent
	ProxyInvalid
	numProxyResults
)

var proxyResultNames = [numProxyResults]string{"v1", "v2", "local", "absent", "invalid"}

// PoolState splits the PostgreSQL pool gauge.
type PoolState uint8

const (
	PoolAcquired PoolState = iota
	PoolIdle
	PoolMax
	numPoolStates
)

var poolStateNames = [numPoolStates]string{"acquired", "idle", "max"}

// Metrics is the whole metric set, reached on the hot path through one pointer
// dereference and never a map lookup.
type Metrics struct {
	// Receive path.
	MessagesTotal          [NumTransports]Counter
	MessagesAcceptedTotal  [NumTransports]Counter
	MessagesDroppedTotal   [numDropReasons]Counter
	MessagesTruncatedTotal [NumTransports]Counter
	MessageBytes           *Histogram
	ConfirmDuration        *Histogram

	// Conversion, in the drain worker.
	ConvertTotal         [NumFormats]Counter
	ConvertWarningsTotal [NumWarnings]Counter
	ConvertDuration      *Histogram

	// Connection-oriented transports only.
	ConnsActive             [NumTransports]Gauge
	ConnsTotal              [NumTransports]Counter
	ConnsRefusedTotal       [NumTransports][numConnLimits]Counter
	ProxyHeadersTotal       [numProxyResults]Counter
	BackpressureSecondsHund [NumTransports]Counter // hundredths of a second

	// RELP (SPEC §5, §14.1).
	RELPFramesTotal          [numRELPCommands]Counter
	RELPResponsesTotal       [numRELPCodes]Counter
	RELPUnansweredTotal      [numRELPUnanswered]Counter
	RELPFrameErrorsTotal     [numRELPFrameErrors]Counter
	RELPInflightBytes        Gauge
	RELPInflightTransactions Gauge
	RELPInflightWaitsTotal   Counter

	// Ring.
	RingItems         Gauge
	RingBytes         Gauge
	RingCapacityItems Gauge
	RingCapacityBytes Gauge

	// Sink.
	SinkBatchSize      *Histogram
	SinkBatchBytes     *Histogram
	SinkDuration       *Histogram
	SinkHealthDuration *Histogram
	SinkErrorsTotal    [numSinkErrKinds]Counter
	SinkUp             Gauge

	EventsWrittenTotal Counter
	EventsDroppedTotal [numEventDropReasons]Counter

	EmergencyFiles         Gauge
	EmergencyEventsPending Gauge

	SinkPostgresDuplicatesTotal Counter
	SinkPostgresPoolConns       [numPoolStates]Gauge
	SinkPostgresPoolWaitsTotal  Counter

	// Set once at construction and never touched again.
	info Info
}

// Info is the fixed identity of the process, exposed as the three *_info
// gauges.
type Info struct {
	RingMode   string
	SinkType   string
	Transports string

	Version string
	Commit  string
	Go      string

	From      string
	Space     string
	AgentName string
}

// NewMetrics allocates the registry.
func NewMetrics(info Info) *Metrics {
	return &Metrics{
		MessageBytes:       newHistogram(messageBytesBounds, unitBytes),
		ConfirmDuration:    newHistogram(confirmDurationBounds, unitSeconds),
		ConvertDuration:    newHistogram(convertDurationBounds, unitSeconds),
		SinkBatchSize:      newHistogram(sinkBatchBounds, unitCount),
		SinkBatchBytes:     newHistogram(sinkBatchBytesBounds, unitBytes),
		SinkDuration:       newHistogram(sinkDurationBounds, unitSeconds),
		SinkHealthDuration: newHistogram(sinkHealthBounds, unitSeconds),
		info:               info,
	}
}

// AddBackpressure records time spent not reading because the ring was full, as
// a counter of HUNDREDTHS of a second: an integer on the hot path, small enough
// never to lose precision on the way out.
func (m *Metrics) AddBackpressure(t Transport, d time.Duration) {
	if d <= 0 || int(t) >= int(NumTransports) {
		return
	}
	m.BackpressureSecondsHund[t].Add(uint64(d / (10 * time.Millisecond)))
}

// WritePrometheus renders the whole registry in Prometheus text format.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	x := &writer{w: w, buf: make([]byte, 0, 16<<10)}

	x.header("relplog_messages_total", "counter",
		"Messages received from the wire (syslog frames read), by transport.")
	for _, t := range streamTransports {
		x.counter("relplog_messages_total", "", lbl("transport", transportNames[t]), m.MessagesTotal[t].Load())
	}
	x.header("relplog_messages_accepted_total", "counter", "Messages that entered the ring, by transport.")
	for _, t := range streamTransports {
		x.counter("relplog_messages_accepted_total", "", lbl("transport", transportNames[t]), m.MessagesAcceptedTotal[t].Load())
	}
	x.header("relplog_messages_dropped_total", "counter",
		"Messages read and deliberately discarded, by reason. What RELP hands back to "+
			"the emitter is not a drop: see relplog_relp_unanswered_total.")
	for i, name := range dropNames {
		x.counter("relplog_messages_dropped_total", "", lbl("reason", name), m.MessagesDroppedTotal[i].Load())
	}
	x.header("relplog_messages_truncated_total", "counter",
		"Messages cut at syslog.max_message_bytes, by transport.")
	for _, t := range streamTransports {
		x.counter("relplog_messages_truncated_total", "", lbl("transport", transportNames[t]), m.MessagesTruncatedTotal[t].Load())
	}
	x.header("relplog_message_bytes", "histogram", "Original size of received messages.")
	x.histogram("relplog_message_bytes", "", m.MessageBytes)
	x.header("relplog_confirm_duration_seconds", "histogram",
		"From a whole syslog frame to its 200 OK being decided: the ring's guarantee met.")
	x.histogram("relplog_confirm_duration_seconds", "", m.ConfirmDuration)

	x.header("relplog_convert_total", "counter", "Conversions to JLH, by payload.format.")
	for i, name := range formatNames {
		x.counter("relplog_convert_total", "", lbl("format", name), m.ConvertTotal[i].Load())
	}
	x.header("relplog_convert_warnings_total", "counter", "parse.warnings emitted, by code.")
	for i, name := range WarningNames {
		x.counter("relplog_convert_warnings_total", "", lbl("code", name), m.ConvertWarningsTotal[i].Load())
	}
	x.header("relplog_convert_duration_seconds", "histogram",
		"Conversion of one message into its JLH envelope, in the drain worker.")
	x.histogram("relplog_convert_duration_seconds", "", m.ConvertDuration)

	x.header("relplog_conns_active", "gauge", "Open connections, by transport.")
	for _, t := range streamTransports {
		x.gauge("relplog_conns_active", lbl("transport", transportNames[t]), m.ConnsActive[t].Load())
	}
	x.header("relplog_conns_total", "counter", "Connections accepted, by transport.")
	for _, t := range streamTransports {
		x.counter("relplog_conns_total", "", lbl("transport", transportNames[t]), m.ConnsTotal[t].Load())
	}
	x.header("relplog_conns_refused_total", "counter",
		"Connections refused by max_conns (global, parked in the backlog), max_conns_per_ip (per_ip) "+
			"or the PROXY protocol rules (proxy).")
	for _, t := range streamTransports {
		for l, name := range connLimitNames {
			x.counter("relplog_conns_refused_total", "",
				lbl("transport", transportNames[t])+","+lbl("reason", name), m.ConnsRefusedTotal[t][l].Load())
		}
	}
	x.header("relplog_proxy_headers_total", "counter", "What the PROXY protocol reader found, by result.")
	for i, name := range proxyResultNames {
		x.counter("relplog_proxy_headers_total", "", lbl("result", name), m.ProxyHeadersTotal[i].Load())
	}
	x.header("relplog_backpressure_seconds_total", "counter",
		"Time spent not reading because the ring was full or the sink DOWN, by transport. "+
			"Backpressure working, not an error: nothing was lost.")
	for _, t := range streamTransports {
		x.counter("relplog_backpressure_seconds_total", "", lbl("transport", transportNames[t]),
			m.BackpressureSecondsHund[t].Load()/100)
	}
	x.header("relplog_relp_frames_total", "counter", "RELP frames received, by command.")
	for i, name := range relpCommandNames {
		x.counter("relplog_relp_frames_total", "", lbl("command", name), m.RELPFramesTotal[i].Load())
	}
	x.header("relplog_relp_responses_total", "counter",
		"RELP responses sent, by code. 500 is final: librelp drops the message it answers.")
	for i, name := range relpCodeNames {
		x.counter("relplog_relp_responses_total", "", lbl("code", name), m.RELPResponsesTotal[i].Load())
	}
	x.header("relplog_relp_unanswered_total", "counter",
		"syslog transactions read and left without a response when the connection ended, by reason. "+
			"librelp retransmits them on the next session: handed back, not lost.")
	for i, name := range relpUnansweredNames {
		x.counter("relplog_relp_unanswered_total", "", lbl("reason", name), m.RELPUnansweredTotal[i].Load())
	}
	x.header("relplog_relp_frame_errors_total", "counter",
		"Connections closed on a frame that cannot be resynchronised, by reason.")
	for i, name := range relpFrameErrorNames {
		x.counter("relplog_relp_frame_errors_total", "", lbl("reason", name), m.RELPFrameErrorsTotal[i].Load())
	}
	x.header("relplog_relp_inflight_bytes", "gauge",
		"Bytes held by the copies of in-flight transactions, all connections (ceiling relp.inflight_max_bytes).")
	x.gauge("relplog_relp_inflight_bytes", "", m.RELPInflightBytes.Load())
	x.header("relplog_relp_inflight_transactions", "gauge", "syslog transactions read and not yet decided.")
	x.gauge("relplog_relp_inflight_transactions", "", m.RELPInflightTransactions.Load())
	x.header("relplog_relp_inflight_waits_total", "counter",
		"Times a reader stopped reading because relp.inflight_max_bytes was exhausted.")
	x.counter("relplog_relp_inflight_waits_total", "", "", m.RELPInflightWaitsTotal.Load())

	x.header("relplog_ring_items", "gauge", "Items held by the ring.")
	x.gauge("relplog_ring_items", "", m.RingItems.Load())
	x.header("relplog_ring_bytes", "gauge", "Bytes held by the ring: each raw buffer (its capacity) plus the item overhead. The JLH is not counted.")
	x.gauge("relplog_ring_bytes", "", m.RingBytes.Load())
	x.header("relplog_ring_capacity_items", "gauge", "Configured ring item ceiling.")
	x.gauge("relplog_ring_capacity_items", "", m.RingCapacityItems.Load())
	x.header("relplog_ring_capacity_bytes", "gauge", "Configured ring byte ceiling.")
	x.gauge("relplog_ring_capacity_bytes", "", m.RingCapacityBytes.Load())

	x.header("relplog_sink_batch_size", "histogram", "Envelopes per sink write.")
	x.histogram("relplog_sink_batch_size", "", m.SinkBatchSize)
	x.header("relplog_sink_batch_bytes", "histogram", "JLH bytes per sink write.")
	x.histogram("relplog_sink_batch_bytes", "", m.SinkBatchBytes)
	x.header("relplog_sink_duration_seconds", "histogram", "Duration of one sink write.")
	x.histogram("relplog_sink_duration_seconds", "", m.SinkDuration)
	x.header("relplog_sink_health_duration_seconds", "histogram", "Duration of one sink health check.")
	x.histogram("relplog_sink_health_duration_seconds", "", m.SinkHealthDuration)
	x.header("relplog_sink_errors_total", "counter", "Sink failures, by class.")
	for i, name := range sinkErrNames {
		x.counter("relplog_sink_errors_total", "", lbl("kind", name), m.SinkErrorsTotal[i].Load())
	}
	x.header("relplog_sink_up", "gauge", "1 when the sink is healthy, 0 when it is DOWN.")
	x.gauge("relplog_sink_up", "", m.SinkUp.Load())

	x.header("relplog_events_written_total", "counter", "Envelopes confirmed by the sink.")
	x.counter("relplog_events_written_total", "", "", m.EventsWrittenTotal.Load())
	x.header("relplog_events_dropped_total", "counter",
		"Accepted messages lost afterwards, by reason. Anything here is a message the "+
			"server had already taken responsibility for.")
	for i, name := range eventDropNames {
		x.counter("relplog_events_dropped_total", "", lbl("reason", name), m.EventsDroppedTotal[i].Load())
	}

	x.header("relplog_emergency_files", "gauge", "Emergency dump files awaiting reingestion.")
	x.gauge("relplog_emergency_files", "", m.EmergencyFiles.Load())
	x.header("relplog_emergency_events_pending", "gauge", "Messages held in emergency files.")
	x.gauge("relplog_emergency_events_pending", "", m.EmergencyEventsPending.Load())

	x.header("relplog_sink_postgres_duplicates_total", "counter",
		"Rows ignored by ON CONFLICT (uuid): envelopes an earlier attempt had delivered.")
	x.counter("relplog_sink_postgres_duplicates_total", "", "", m.SinkPostgresDuplicatesTotal.Load())
	x.header("relplog_sink_postgres_pool_conns", "gauge", "Connection pool occupancy, by state.")
	for i, name := range poolStateNames {
		x.gauge("relplog_sink_postgres_pool_conns", lbl("state", name), m.SinkPostgresPoolConns[i].Load())
	}
	x.header("relplog_sink_postgres_pool_waits_total", "counter", "Acquires that had to wait for a connection.")
	x.counter("relplog_sink_postgres_pool_waits_total", "", "", m.SinkPostgresPoolWaitsTotal.Load())

	in := m.info
	x.header("relplog_config_info", "gauge", "Active ring mode, sink type and enabled transports.")
	x.sample("relplog_config_info", lbl("ring_mode", in.RingMode)+","+lbl("sink_type", in.SinkType)+","+
		lbl("transports", in.Transports), "1")
	x.header("relplog_build_info", "gauge", "Build identity.")
	x.sample("relplog_build_info", lbl("version", in.Version)+","+lbl("commit", in.Commit)+","+
		lbl("go", in.Go), "1")
	x.header("relplog_identity_info", "gauge",
		"LogHub identity of this instance: the envelope's from and space, and the AgentName.")
	x.sample("relplog_identity_info", lbl("from", in.From)+","+lbl("space", in.Space)+","+
		lbl("agent_name", in.AgentName), "1")

	x.flush()
	return x.err
}

// --------------------------------------------------------------------- writer

// writer buffers the exposition so a scrape is a handful of syscalls.
type writer struct {
	w   io.Writer
	buf []byte
	err error
}

func (x *writer) flush() {
	if x.err != nil || len(x.buf) == 0 {
		return
	}
	_, x.err = x.w.Write(x.buf)
	x.buf = x.buf[:0]
}

func (x *writer) maybeFlush() {
	if len(x.buf) >= 32<<10 {
		x.flush()
	}
}

func (x *writer) header(name, typ, help string) {
	x.buf = append(x.buf, "# HELP "...)
	x.buf = append(x.buf, name...)
	x.buf = append(x.buf, ' ')
	x.buf = append(x.buf, help...)
	x.buf = append(x.buf, '\n')
	x.buf = append(x.buf, "# TYPE "...)
	x.buf = append(x.buf, name...)
	x.buf = append(x.buf, ' ')
	x.buf = append(x.buf, typ...)
	x.buf = append(x.buf, '\n')
}

func (x *writer) sample(name, labels, value string) {
	x.buf = append(x.buf, name...)
	if labels != "" {
		x.buf = append(x.buf, '{')
		x.buf = append(x.buf, labels...)
		x.buf = append(x.buf, '}')
	}
	x.buf = append(x.buf, ' ')
	x.buf = append(x.buf, value...)
	x.buf = append(x.buf, '\n')
	x.maybeFlush()
}

func (x *writer) counter(name, _ string, labels string, v uint64) {
	x.sample(name, labels, strconv.FormatUint(v, 10))
}

func (x *writer) gauge(name, labels string, v int64) {
	x.sample(name, labels, strconv.FormatInt(v, 10))
}

// histogram writes the cumulative buckets, the sum and the count. The stored
// counts are non-cumulative and are accumulated here; +Inf and _count come from
// the same running total, which makes them equal by construction.
func (x *writer) histogram(name, labels string, h *Histogram) {
	var cum uint64
	for i := range h.bounds {
		cum += h.counts[i].Load()
		x.sample(name+"_bucket", withLE(labels, h.bounds[i].le), strconv.FormatUint(cum, 10))
	}
	cum += h.counts[len(h.bounds)].Load()
	x.sample(name+"_bucket", withLE(labels, "+Inf"), strconv.FormatUint(cum, 10))
	x.sample(name+"_sum", labels, h.sumValue())
	x.sample(name+"_count", labels, strconv.FormatUint(cum, 10))
}

func withLE(labels, le string) string {
	if labels == "" {
		return `le="` + le + `"`
	}
	return labels + `,le="` + le + `"`
}

func lbl(k, v string) string { return k + `="` + escapeLabel(v) + `"` }

func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}
