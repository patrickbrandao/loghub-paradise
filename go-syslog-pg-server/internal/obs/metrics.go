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

// streamTransports hold connections and apply backpressure. udp and unix have
// neither, and a series that can never rise is not exposed.
var streamTransports = [...]Transport{TransportTCP}

func (t Transport) String() string {
	if int(t) < len(transportNames) {
		return transportNames[t]
	}
	return "unknown"
}

// DropReason is why a message never became an accepted item. On UDP and unix
// it is the ONLY record that anything was lost (SPEC §2.3), which is why every
// drop path increments exactly one of these.
type DropReason uint8

const (
	DropRingFull DropReason = iota
	DropSinkDown
	DropMalformedFrame
	DropProxy
	DropEmpty
	DropShutdown
	numDropReasons
)

var dropNames = [numDropReasons]string{
	"ring_full", "sink_down", "malformed_frame", "proxy", "empty", "shutdown",
}

func (d DropReason) String() string {
	if int(d) < len(dropNames) {
		return dropNames[d]
	}
	return "unknown"
}

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

// ConnLimit is which ceiling refused a connection.
type ConnLimit uint8

const (
	ConnLimitGlobal ConnLimit = iota
	ConnLimitPerIP
	numConnLimits
)

var connLimitNames = [numConnLimits]string{"global", "per_ip"}

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
	UDPReceiveErrorsTotal   Counter
	UDPReadBufferBytes      Gauge

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

	x.header("syslog_messages_total", "counter",
		"Messages received from the wire, by transport.")
	for i, name := range transportNames {
		x.counter("syslog_messages_total", "", lbl("transport", name), m.MessagesTotal[i].Load())
	}
	x.header("syslog_messages_accepted_total", "counter", "Messages that entered the ring, by transport.")
	for i, name := range transportNames {
		x.counter("syslog_messages_accepted_total", "", lbl("transport", name), m.MessagesAcceptedTotal[i].Load())
	}
	x.header("syslog_messages_dropped_total", "counter",
		"Messages that never became events, by reason. On UDP and unix this is the "+
			"ONLY record that anything was lost: those transports cannot say no.")
	for i, name := range dropNames {
		x.counter("syslog_messages_dropped_total", "", lbl("reason", name), m.MessagesDroppedTotal[i].Load())
	}
	x.header("syslog_messages_truncated_total", "counter",
		"Messages cut at syslog.max_message_bytes, by transport.")
	for i, name := range transportNames {
		x.counter("syslog_messages_truncated_total", "", lbl("transport", name), m.MessagesTruncatedTotal[i].Load())
	}
	x.header("syslog_message_bytes", "histogram", "Original size of received messages.")
	x.histogram("syslog_message_bytes", "", m.MessageBytes)
	x.header("syslog_confirm_duration_seconds", "histogram",
		"From a whole frame to the ring's guarantee. tcp only: udp and unix have nothing to confirm.")
	x.histogram("syslog_confirm_duration_seconds", "", m.ConfirmDuration)

	x.header("syslog_convert_total", "counter", "Conversions to JLH, by payload.format.")
	for i, name := range formatNames {
		x.counter("syslog_convert_total", "", lbl("format", name), m.ConvertTotal[i].Load())
	}
	x.header("syslog_convert_warnings_total", "counter", "parse.warnings emitted, by code.")
	for i, name := range WarningNames {
		x.counter("syslog_convert_warnings_total", "", lbl("code", name), m.ConvertWarningsTotal[i].Load())
	}
	x.header("syslog_convert_duration_seconds", "histogram",
		"Conversion of one message into its JLH envelope, in the drain worker.")
	x.histogram("syslog_convert_duration_seconds", "", m.ConvertDuration)

	x.header("syslog_conns_active", "gauge", "Open connections, by transport.")
	for _, t := range streamTransports {
		x.gauge("syslog_conns_active", lbl("transport", transportNames[t]), m.ConnsActive[t].Load())
	}
	x.header("syslog_conns_total", "counter", "Connections accepted, by transport.")
	for _, t := range streamTransports {
		x.counter("syslog_conns_total", "", lbl("transport", transportNames[t]), m.ConnsTotal[t].Load())
	}
	x.header("syslog_conns_refused_total", "counter",
		"Connections refused by max_conns (global) or max_conns_per_ip (per_ip).")
	for _, t := range streamTransports {
		for l, name := range connLimitNames {
			x.counter("syslog_conns_refused_total", "",
				lbl("transport", transportNames[t])+","+lbl("limit", name), m.ConnsRefusedTotal[t][l].Load())
		}
	}
	x.header("syslog_proxy_headers_total", "counter", "What the PROXY protocol reader found, by result.")
	for i, name := range proxyResultNames {
		x.counter("syslog_proxy_headers_total", "", lbl("result", name), m.ProxyHeadersTotal[i].Load())
	}
	x.header("syslog_backpressure_seconds_total", "counter",
		"Time spent not reading because the ring was full or the sink DOWN, by transport. "+
			"Backpressure working, not an error: nothing was lost.")
	for _, t := range streamTransports {
		x.counter("syslog_backpressure_seconds_total", "", lbl("transport", transportNames[t]),
			m.BackpressureSecondsHund[t].Load()/100)
	}
	x.header("syslog_udp_receive_errors_total", "counter",
		"Errors from the UDP socket itself. Kernel drops from a small SO_RCVBUF do NOT "+
			"appear here: only /proc/net/udp knows about them.")
	x.counter("syslog_udp_receive_errors_total", "", "", m.UDPReceiveErrorsTotal.Load())
	x.header("syslog_udp_read_buffer_bytes", "gauge",
		"The effective SO_RCVBUF, read back. Linux silently caps the request at net.core.rmem_max.")
	x.gauge("syslog_udp_read_buffer_bytes", "", m.UDPReadBufferBytes.Load())

	x.header("syslog_ring_items", "gauge", "Items held by the ring.")
	x.gauge("syslog_ring_items", "", m.RingItems.Load())
	x.header("syslog_ring_bytes", "gauge", "Raw-line bytes held by the ring. The JLH is not counted.")
	x.gauge("syslog_ring_bytes", "", m.RingBytes.Load())
	x.header("syslog_ring_capacity_items", "gauge", "Configured ring item ceiling.")
	x.gauge("syslog_ring_capacity_items", "", m.RingCapacityItems.Load())
	x.header("syslog_ring_capacity_bytes", "gauge", "Configured ring byte ceiling.")
	x.gauge("syslog_ring_capacity_bytes", "", m.RingCapacityBytes.Load())
	x.header("syslog_sink_batch_size", "histogram", "Envelopes per sink write.")
	x.histogram("syslog_sink_batch_size", "", m.SinkBatchSize)
	x.header("syslog_sink_batch_bytes", "histogram", "JLH bytes per sink write.")
	x.histogram("syslog_sink_batch_bytes", "", m.SinkBatchBytes)
	x.header("syslog_sink_duration_seconds", "histogram", "Duration of one sink write.")
	x.histogram("syslog_sink_duration_seconds", "", m.SinkDuration)
	x.header("syslog_sink_health_duration_seconds", "histogram", "Duration of one sink health check.")
	x.histogram("syslog_sink_health_duration_seconds", "", m.SinkHealthDuration)
	x.header("syslog_sink_errors_total", "counter", "Sink failures, by class.")
	for i, name := range sinkErrNames {
		x.counter("syslog_sink_errors_total", "", lbl("kind", name), m.SinkErrorsTotal[i].Load())
	}
	x.header("syslog_sink_up", "gauge", "1 when the sink is healthy, 0 when it is DOWN.")
	x.gauge("syslog_sink_up", "", m.SinkUp.Load())

	x.header("syslog_events_written_total", "counter", "Envelopes confirmed by the sink.")
	x.counter("syslog_events_written_total", "", "", m.EventsWrittenTotal.Load())
	x.header("syslog_events_dropped_total", "counter",
		"Accepted messages lost afterwards, by reason. Anything here is a message the "+
			"server had already taken responsibility for.")
	for i, name := range eventDropNames {
		x.counter("syslog_events_dropped_total", "", lbl("reason", name), m.EventsDroppedTotal[i].Load())
	}

	x.header("syslog_emergency_files", "gauge", "Emergency dump files awaiting reingestion.")
	x.gauge("syslog_emergency_files", "", m.EmergencyFiles.Load())
	x.header("syslog_emergency_events_pending", "gauge", "Messages held in emergency files.")
	x.gauge("syslog_emergency_events_pending", "", m.EmergencyEventsPending.Load())

	x.header("syslog_sink_postgres_duplicates_total", "counter",
		"Rows ignored by ON CONFLICT (uuid): envelopes an earlier attempt had delivered.")
	x.counter("syslog_sink_postgres_duplicates_total", "", "", m.SinkPostgresDuplicatesTotal.Load())
	x.header("syslog_sink_postgres_pool_conns", "gauge", "Connection pool occupancy, by state.")
	for i, name := range poolStateNames {
		x.gauge("syslog_sink_postgres_pool_conns", lbl("state", name), m.SinkPostgresPoolConns[i].Load())
	}
	x.header("syslog_sink_postgres_pool_waits_total", "counter", "Acquires that had to wait for a connection.")
	x.counter("syslog_sink_postgres_pool_waits_total", "", "", m.SinkPostgresPoolWaitsTotal.Load())

	in := m.info
	x.header("syslog_config_info", "gauge", "Active ring mode, sink type and enabled transports.")
	x.sample("syslog_config_info", lbl("ring_mode", in.RingMode)+","+lbl("sink_type", in.SinkType)+","+
		lbl("transports", in.Transports), "1")
	x.header("syslog_build_info", "gauge", "Build identity.")
	x.sample("syslog_build_info", lbl("version", in.Version)+","+lbl("commit", in.Commit)+","+
		lbl("go", in.Go), "1")
	x.header("syslog_identity_info", "gauge",
		"LogHub identity of this instance: the envelope's from and space, and the AgentName.")
	x.sample("syslog_identity_info", lbl("from", in.From)+","+lbl("space", in.Space)+","+
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
