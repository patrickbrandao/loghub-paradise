package obs

import (
	"io"
	"strconv"
	"strings"
	"time"
)

// Label value sets. Every one is finite and known at compile time (SPEC §11.1):
// no label ever derives from a path, a method, an IP or a user agent, because
// those come from arbitrary internet traffic and would take the Prometheus down
// rather than the server.

// Outcome splits request latency into the two distributions that matter.
// Keeping rejections separate is not cosmetic: under an attack producing 90%
// 429s -- which cost microseconds -- a single distribution would have its p99
// pulled DOWN, hiding a real regression on the accept path.
type Outcome uint8

const (
	Accepted Outcome = iota
	Rejected
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"accepted", "rejected"}

// Reason enumerates why a request was rejected.
type Reason uint8

const (
	ReasonRingFull Reason = iota
	ReasonTooLarge
	ReasonSinkDown
	ReasonShutdown
	ReasonBadRequest
	numReasons
)

var reasonNames = [numReasons]string{
	"ring_full", "too_large", "sink_down", "shutdown", "bad_request",
}

// Status codes are enumerated rather than labelled freely, so the series count
// is fixed no matter what the network sends.
var statusCodes = [...]int{200, 400, 405, 413, 414, 429, 431, 500, 503}

func statusIndex(code int) int {
	for i, c := range statusCodes {
		if c == code {
			return i
		}
	}
	return len(statusCodes) // "other"
}

var statusNames = func() []string {
	out := make([]string, 0, len(statusCodes)+1)
	for _, c := range statusCodes {
		out = append(out, strconv.Itoa(c))
	}
	return append(out, "other")
}()

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

// DropReason enumerates why an accepted event was lost.
type DropReason uint8

const (
	DropSinkLogic DropReason = iota
	DropEmergencyFailed
	DropAbort
	numDropReasons
)

var dropNames = [numDropReasons]string{"sink_logic", "emergency_failed", "abort"}

// Proto splits connection gauges by wire protocol.
type Proto uint8

const (
	ProtoHTTP1 Proto = iota
	ProtoHTTP2
	numProtos
)

var protoNames = [numProtos]string{"http1", "http2"}

// Metrics is the whole metric set, held by value on the hot path so a handler
// reaches a counter through one pointer dereference and never a map lookup.
type Metrics struct {
	RequestsTotal   [len(statusCodes) + 1]Counter
	RejectedTotal   [numReasons]Counter
	RequestDuration [numOutcomes]*Histogram
	BodyBytes       *Histogram

	RingItems         Gauge
	RingBytes         Gauge
	RingCapacityItems Gauge
	RingCapacityBytes Gauge

	SinkBatchSize      *Histogram
	SinkDuration       *Histogram
	SinkHealthDuration *Histogram
	SinkErrorsTotal    [numSinkErrKinds]Counter
	SinkUp             Gauge

	EventsWrittenTotal Counter
	EventsDroppedTotal [numDropReasons]Counter

	EmergencyFiles         Gauge
	EmergencyEventsPending Gauge

	ConnsActive [numProtos]Gauge

	ClientIPUntrustedForwardedTotal Counter

	SinkNATSDuplicatesTotal Counter
	SinkNATSReconnectsTotal Counter

	// BeaconsOK and BeaconsError are whs_beacons_total{result} (SPEC
	// §8.7.10): two fields rather than an array, because the beacon is the
	// only writer and the two outcomes are all there is.
	BeaconsOK    Counter
	BeaconsError Counter

	// info labels, fixed at startup
	buildVersion, buildCommit, buildGo string
	ringMode, sinkType                 string
}

// NewMetrics builds the registry. The info labels are captured once; they never
// change for the life of the process.
func NewMetrics(version, commit, goVersion, ringMode, sinkType string) *Metrics {
	m := &Metrics{
		BodyBytes:          newHistogram(bodyBytesBounds, unitBytes),
		SinkBatchSize:      newHistogram(sinkBatchBounds, unitCount),
		SinkDuration:       newHistogram(sinkDurationBounds, unitSeconds),
		SinkHealthDuration: newHistogram(sinkHealthBounds, unitSeconds),
		buildVersion:       version,
		buildCommit:        commit,
		buildGo:            goVersion,
		ringMode:           ringMode,
		sinkType:           sinkType,
	}
	for i := range m.RequestDuration {
		m.RequestDuration[i] = newHistogram(requestDurationBounds, unitSeconds)
	}
	return m
}

// ObserveRequest records one finished request.
func (m *Metrics) ObserveRequest(code int, o Outcome, d time.Duration) {
	m.RequestsTotal[statusIndex(code)].Inc()
	m.RequestDuration[o].ObserveDuration(d)
}

// Reject records a rejection together with its reason.
func (m *Metrics) Reject(r Reason) { m.RejectedTotal[r].Inc() }

// ---------------------------------------------------------------------------
// Text exposition.

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
	x.buf = append(x.buf, '\n', '#', ' ', 'T', 'Y', 'P', 'E', ' ')
	x.buf = append(x.buf, name...)
	x.buf = append(x.buf, ' ')
	x.buf = append(x.buf, typ...)
	x.buf = append(x.buf, '\n')
}

// sample writes one line. labels is already-rendered label text without braces.
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

func (x *writer) counter(name, help, labels string, v uint64) {
	x.sample(name, labels, strconv.FormatUint(v, 10))
}

func (x *writer) gauge(name, labels string, v int64) {
	x.sample(name, labels, strconv.FormatInt(v, 10))
}

// histogram writes the cumulative buckets, the sum and the count.
//
// The stored counts are non-cumulative, so they are accumulated here. The +Inf
// bucket is mandatory and must equal _count; both come from the same running
// total, which is what makes that hold by construction.
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

// escapeLabel escapes the three characters the exposition format reserves in a
// label value. All current label values are compile-time constants except the
// build info, which comes from linker flags.
func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// WritePrometheus renders the whole registry in Prometheus text format.
// It is deliberately not named WriteTo: that name belongs to io.WriterTo,
// whose signature returns a byte count this has no use for.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	x := &writer{w: w, buf: make([]byte, 0, 16<<10)}

	x.header("whs_requests_total", "counter", "Requests completed, by response code.")
	for i, name := range statusNames {
		x.counter("whs_requests_total", "", lbl("code", name), m.RequestsTotal[i].Load())
	}

	x.header("whs_rejected_total", "counter", "Requests rejected, by reason.")
	for i, name := range reasonNames {
		x.counter("whs_rejected_total", "", lbl("reason", name), m.RejectedTotal[i].Load())
	}

	x.header("whs_request_duration_seconds", "histogram",
		"Time from the first header byte to the response, split by outcome.")
	for i, name := range outcomeNames {
		x.histogram("whs_request_duration_seconds", lbl("outcome", name), m.RequestDuration[i])
	}

	x.header("whs_body_bytes", "histogram",
		"Request body sizes. Bucket edges match the buffer pool size classes.")
	x.histogram("whs_body_bytes", "", m.BodyBytes)

	x.header("whs_ring_items", "gauge", "Items currently held by the ring.")
	x.gauge("whs_ring_items", "", m.RingItems.Load())
	x.header("whs_ring_bytes", "gauge", "Bytes currently held by the ring.")
	x.gauge("whs_ring_bytes", "", m.RingBytes.Load())
	x.header("whs_ring_capacity_items", "gauge", "Configured ring item ceiling.")
	x.gauge("whs_ring_capacity_items", "", m.RingCapacityItems.Load())
	x.header("whs_ring_capacity_bytes", "gauge", "Configured ring byte ceiling.")
	x.gauge("whs_ring_capacity_bytes", "", m.RingCapacityBytes.Load())

	x.header("whs_sink_batch_size", "histogram",
		"Items per sink write. Consistently full batches mean the sink is the bottleneck.")
	x.histogram("whs_sink_batch_size", "", m.SinkBatchSize)

	x.header("whs_sink_duration_seconds", "histogram", "Duration of one sink write.")
	x.histogram("whs_sink_duration_seconds", "", m.SinkDuration)

	x.header("whs_sink_health_duration_seconds", "histogram", "Duration of one sink health check.")
	x.histogram("whs_sink_health_duration_seconds", "", m.SinkHealthDuration)

	x.header("whs_sink_errors_total", "counter", "Sink failures, by class.")
	for i, name := range sinkErrNames {
		x.counter("whs_sink_errors_total", "", lbl("kind", name), m.SinkErrorsTotal[i].Load())
	}

	x.header("whs_sink_up", "gauge", "1 when the sink is healthy, 0 when it is DOWN.")
	x.gauge("whs_sink_up", "", m.SinkUp.Load())

	x.header("whs_events_written_total", "counter", "Events confirmed by the sink.")
	x.counter("whs_events_written_total", "", "", m.EventsWrittenTotal.Load())

	x.header("whs_events_dropped_total", "counter", "Accepted events that were lost, by reason.")
	for i, name := range dropNames {
		x.counter("whs_events_dropped_total", "", lbl("reason", name), m.EventsDroppedTotal[i].Load())
	}

	x.header("whs_emergency_files", "gauge", "Emergency dump files awaiting reingestion.")
	x.gauge("whs_emergency_files", "", m.EmergencyFiles.Load())
	x.header("whs_emergency_events_pending", "gauge", "Events held in emergency files.")
	x.gauge("whs_emergency_events_pending", "", m.EmergencyEventsPending.Load())

	x.header("whs_conns_active", "gauge", "Open connections, by wire protocol.")
	for i, name := range protoNames {
		x.gauge("whs_conns_active", lbl("proto", name), m.ConnsActive[i].Load())
	}

	x.header("whs_clientip_untrusted_forwarded_total", "counter",
		"Requests carrying a forwarding header from a peer outside proxy.trusted. "+
			"A rising value almost always means proxy.trusted is misconfigured.")
	x.counter("whs_clientip_untrusted_forwarded_total", "", "",
		m.ClientIPUntrustedForwardedTotal.Load())

	x.header("whs_sink_nats_duplicates_total", "counter",
		"Publishes the stream acknowledged as duplicates by Nats-Msg-Id: events an earlier attempt had already delivered.")
	x.counter("whs_sink_nats_duplicates_total", "", "", m.SinkNATSDuplicatesTotal.Load())
	x.header("whs_sink_nats_reconnects_total", "counter", "Reconnections of the NATS client.")
	x.counter("whs_sink_nats_reconnects_total", "", "", m.SinkNATSReconnectsTotal.Load())

	x.header("whs_beacons_total", "counter",
		"Heartbeat beacons published on the control plane, by result. A failure never touches the sink's health.")
	x.counter("whs_beacons_total", "", lbl("result", "ok"), m.BeaconsOK.Load())
	x.counter("whs_beacons_total", "", lbl("result", "error"), m.BeaconsError.Load())

	x.header("whs_config_info", "gauge", "Active ring mode and sink type.")
	x.sample("whs_config_info", lbl("ring_mode", m.ringMode)+","+lbl("sink_type", m.sinkType), "1")

	x.header("whs_build_info", "gauge", "Build identity.")
	x.sample("whs_build_info",
		lbl("version", m.buildVersion)+","+lbl("commit", m.buildCommit)+","+lbl("go", m.buildGo), "1")

	x.flush()
	return x.err
}

// String returns the metric label for a rejection reason. It is also used by
// the sampled rejection log, so the two always agree.
func (r Reason) String() string {
	if int(r) < len(reasonNames) {
		return reasonNames[r]
	}
	return "unknown"
}
