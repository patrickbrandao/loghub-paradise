package obs

import (
	"io"
	"strconv"
	"strings"
	"time"
)

// Label value sets. Every one is finite and known at compile time (SPEC §13.1):
// no label ever derives from a hostname, a tag, an app-name or an IP, because
// those come from arbitrary emitters and would take the Prometheus down rather
// than the server.
//
// facility and severity are NOT labels either, even though they are finite --
// twenty-four and eight values. The product with transport would be 768 series
// per counter, and the distribution by severity is a question for the
// destination, where it is an indexed column, not for a /metrics endpoint
// scraped every fifteen seconds with no retention.

// Transport is which listener received a message. It mirrors
// envelope.Transport; the duplication is deliberate, so obs does not import
// envelope and envelope does not import obs.
type Transport uint8

const (
	TransportUDP Transport = iota
	TransportTCP
	TransportUnix
	TransportRELP
	NumTransports
)

var transportNames = [NumTransports]string{"udp", "tcp", "unix", "relp"}

// streamTransports are the transports that hold connections and apply
// backpressure. udp and unix (a datagram socket) have neither, and a series
// that can never rise is not exposed (SPEC §13.1).
var streamTransports = [...]Transport{TransportTCP, TransportRELP}

func (t Transport) String() string {
	if int(t) < len(transportNames) {
		return transportNames[t]
	}
	return "unknown"
}

// DropReason enumerates why a message never became an accepted item.
//
// Unlike the sibling service, which can always answer 429 or 503, most of these
// are the ONLY record that anything happened: on UDP there is nobody to tell
// (SPEC §2.3). That makes this counter the honest answer to "are we losing
// messages", and it is why every drop path increments exactly one of these.
//
// It is a separate enumeration from EventDropReason on purpose. One shared
// enumeration would expose every reason on both counters, and a series that
// can never move -- syslog_events_dropped_total{reason="filter"} -- is not
// harmless: it tells an operator a measurement exists where none does
// (SPEC §13.1).
type DropReason uint8

const (
	DropRingFull DropReason = iota
	DropSinkDown
	DropFilter
	DropRateLimit
	DropFrameTooLarge
	DropMalformedFrame
	DropShutdown
	numDropReasons
)

var dropNames = [numDropReasons]string{
	"ring_full", "sink_down", "filter", "ratelimit", "frame_too_large",
	"malformed_frame", "shutdown",
}

func (d DropReason) String() string {
	if int(d) < len(dropNames) {
		return dropNames[d]
	}
	return "unknown"
}

// EventDropReason enumerates why an ACCEPTED item was lost after the fact: the
// destination refused it for good, the emergency dump could not be written, or
// a second signal cut the shutdown short. Anything above zero here is data
// that was confirmed and then lost.
type EventDropReason uint8

const (
	EventDropSinkLogic EventDropReason = iota
	EventDropEmergencyFailed
	EventDropAbort
	numEventDropReasons
)

var eventDropNames = [numEventDropReasons]string{"sink_logic", "emergency_failed", "abort"}

func (d EventDropReason) String() string {
	if int(d) < len(eventDropNames) {
		return eventDropNames[d]
	}
	return "unknown"
}

// RFC classifies what the parser made of a message. It mirrors syslog.RFC.
type RFC uint8

const (
	RFCUnknown RFC = iota
	RFC3164
	RFC5424
	numRFCs
)

var rfcNames = [numRFCs]string{"unknown", "3164", "5424"}

// Anomaly enumerates parse oddities worth a counter and never worth a rejection.
// It mirrors syslog.Anomaly.
type Anomaly uint8

const (
	AnomDupSDID Anomaly = iota
	AnomSDTruncated
	AnomSDUnterminated
	AnomBadTimestamp
	NumAnomalies
)

var anomalyNames = [NumAnomalies]string{
	"dup_sd_id", "sd_truncated", "sd_unterminated", "bad_timestamp",
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

// RELPCommand enumerates the frame commands. An unrecognised command folds into
// "unknown" rather than becoming a label of its own: the command name comes off
// the wire, so a free label would let an emitter mint series at will.
type RELPCommand uint8

const (
	RELPOpen RELPCommand = iota
	RELPSyslog
	RELPClose
	RELPUnknown
	numRELPCommands
)

var relpCommandNames = [numRELPCommands]string{"open", "syslog", "close", "unknown"}

// PoolState splits the PostgreSQL and MySQL connection pool gauges
// (SPEC §10.7.8).
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
	MessagesTotal         [NumTransports]Counter
	MessagesAcceptedTotal [NumTransports]Counter
	MessagesDroppedTotal  [numDropReasons]Counter
	MessageBytes          *Histogram
	ConfirmDuration       *Histogram

	// Parse, which runs in the drain worker.
	ParseTotal          [numRFCs]Counter
	ParseAnomaliesTotal [NumAnomalies]Counter
	ParseDuration       *Histogram

	// Connection-oriented transports only.
	ConnsActive             [NumTransports]Gauge
	ConnsTotal              [NumTransports]Counter
	BackpressureSecondsHund [NumTransports]Counter // hundredths of a second
	UDPReceiveErrorsTotal   Counter
	RELPFramesTotal         [numRELPCommands]Counter
	RELPAcksTotal           Counter

	// Ring.
	RingItems         Gauge
	RingBytes         Gauge
	RingCapacityItems Gauge
	RingCapacityBytes Gauge
	// RingQuarantinedTotal counts ring=sqlite rows whose head could not be
	// decoded and were moved to the quarantine table (SPEC §9.4).
	RingQuarantinedTotal Counter

	// Sink.
	SinkBatchSize      *Histogram
	SinkDuration       *Histogram
	SinkHealthDuration *Histogram
	SinkErrorsTotal    [numSinkErrKinds]Counter
	SinkUp             Gauge

	EventsWrittenTotal Counter
	EventsDroppedTotal [numEventDropReasons]Counter

	EmergencyFiles         Gauge
	EmergencyEventsPending Gauge

	RateLimitBuckets Gauge

	SinkSQLiteFileBytes         Gauge
	SinkSQLiteRotationsTotal    Counter
	SinkSQLiteFilesRemovedTotal Counter

	SinkPostgresDuplicatesTotal Counter
	SinkPostgresPoolConns       [numPoolStates]Gauge
	SinkPostgresPoolWaitsTotal  Counter

	SinkMySQLDuplicatesTotal Counter
	SinkMySQLPoolConns       [numPoolStates]Gauge
	SinkMySQLPoolWaitsTotal  Counter

	SinkNATSDuplicatesTotal Counter
	SinkNATSReconnectsTotal Counter

	// Identity, set once at construction and never touched again.
	ringMode     string
	sinkType     string
	transports   string
	buildVersion string
	buildCommit  string
	buildGo      string
}

// BuildInfo is the linker-injected identity.
type BuildInfo struct {
	Version string
	Commit  string
	Go      string
}

// NewMetrics allocates the registry. ringMode, sinkType and transports become
// the labels of syslog_config_info, which is how a dashboard tells one
// deployment's guarantees from another's.
func NewMetrics(ringMode, sinkType, transports string, b BuildInfo) *Metrics {
	return &Metrics{
		MessageBytes:       newHistogram(messageBytesBounds, unitBytes),
		ConfirmDuration:    newHistogram(confirmDurationBounds, unitSeconds),
		ParseDuration:      newHistogram(parseDurationBounds, unitSeconds),
		SinkBatchSize:      newHistogram(sinkBatchBounds, unitCount),
		SinkDuration:       newHistogram(sinkDurationBounds, unitSeconds),
		SinkHealthDuration: newHistogram(sinkHealthBounds, unitSeconds),

		ringMode:     ringMode,
		sinkType:     sinkType,
		transports:   transports,
		buildVersion: b.Version,
		buildCommit:  b.Commit,
		buildGo:      b.Go,
	}
}

// AddBackpressure records time spent not reading because the ring was full.
//
// It is a counter of HUNDREDTHS of a second, not a float and not nanoseconds.
// Prometheus counters are float64 on the wire, but the value is accumulated as
// an integer here so the hot path needs no float compare-and-swap; hundredths
// keep the number small enough that it never loses precision on the way out,
// and a backpressure episode shorter than 10ms is not a thing anyone needs to
// measure.
func (m *Metrics) AddBackpressure(t Transport, d time.Duration) {
	if d <= 0 || int(t) >= int(NumTransports) {
		return
	}
	m.BackpressureSecondsHund[t].Add(uint64(d / (10 * time.Millisecond)))
}

// WritePrometheus renders the whole registry in Prometheus text format.
//
// It is deliberately not named WriteTo: that name belongs to io.WriterTo, whose
// signature returns a byte count this has no use for.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	x := &writer{w: w, buf: make([]byte, 0, 16<<10)}

	x.header("syslog_messages_total", "counter",
		"Frames received from the wire, by transport, before any filtering.")
	for i, name := range transportNames {
		x.counter("syslog_messages_total", "", lbl("transport", name), m.MessagesTotal[i].Load())
	}

	x.header("syslog_messages_accepted_total", "counter",
		"Messages that entered the ring, by transport.")
	for i, name := range transportNames {
		x.counter("syslog_messages_accepted_total", "", lbl("transport", name),
			m.MessagesAcceptedTotal[i].Load())
	}

	x.header("syslog_messages_dropped_total", "counter",
		"Messages that never became events, by reason. On UDP and unix this is the "+
			"ONLY record that anything was lost: those transports cannot say no.")
	for i, name := range dropNames {
		x.counter("syslog_messages_dropped_total", "", lbl("reason", name),
			m.MessagesDroppedTotal[i].Load())
	}

	x.header("syslog_message_bytes", "histogram",
		"Size of received messages. Bucket edges mirror the buffer pool classes.")
	x.histogram("syslog_message_bytes", "", m.MessageBytes)

	x.header("syslog_confirm_duration_seconds", "histogram",
		"Time from holding a frame to being able to confirm it. Observed only on "+
			"tcp and relp: udp and unix have nothing to confirm.")
	x.histogram("syslog_confirm_duration_seconds", "", m.ConfirmDuration)

	x.header("syslog_parse_total", "counter",
		"Messages by the grammar they matched. A rising unknown means an emitter "+
			"is not speaking syslog.")
	for i, name := range rfcNames {
		x.counter("syslog_parse_total", "", lbl("rfc", name), m.ParseTotal[i].Load())
	}

	x.header("syslog_parse_anomalies_total", "counter",
		"Parse oddities tolerated rather than rejected, by kind.")
	for i, name := range anomalyNames {
		x.counter("syslog_parse_anomalies_total", "", lbl("kind", name),
			m.ParseAnomaliesTotal[i].Load())
	}

	x.header("syslog_parse_duration_seconds", "histogram",
		"Full parse plus envelope serialisation, which run in the drain worker.")
	x.histogram("syslog_parse_duration_seconds", "", m.ParseDuration)

	x.header("syslog_conns_active", "gauge", "Open connections, by transport.")
	for _, t := range streamTransports {
		x.gauge("syslog_conns_active", lbl("transport", transportNames[t]), m.ConnsActive[t].Load())
	}
	x.header("syslog_conns_total", "counter", "Connections accepted, by transport.")
	for _, t := range streamTransports {
		x.counter("syslog_conns_total", "", lbl("transport", transportNames[t]), m.ConnsTotal[t].Load())
	}

	x.header("syslog_backpressure_seconds_total", "counter",
		"Time spent not reading because the ring was full, by transport. This is "+
			"backpressure working, not an error: nothing was lost.")
	for _, t := range streamTransports {
		x.counter("syslog_backpressure_seconds_total", "", lbl("transport", transportNames[t]),
			m.BackpressureSecondsHund[t].Load()/100)
	}

	x.header("syslog_udp_receive_errors_total", "counter",
		"Errors from the UDP socket itself. Kernel drops from a small SO_RCVBUF do "+
			"NOT appear here: they happen before this process sees them, and only "+
			"/proc/net/udp knows about them.")
	x.counter("syslog_udp_receive_errors_total", "", "", m.UDPReceiveErrorsTotal.Load())

	x.header("syslog_relp_frames_total", "counter", "RELP frames received, by command.")
	for i, name := range relpCommandNames {
		x.counter("syslog_relp_frames_total", "", lbl("command", name), m.RELPFramesTotal[i].Load())
	}
	x.header("syslog_relp_acks_total", "counter",
		"RELP transactions answered 200 OK. The only true delivery confirmation "+
			"this server can give.")
	x.counter("syslog_relp_acks_total", "", "", m.RELPAcksTotal.Load())

	x.header("syslog_ring_items", "gauge", "Items currently held by the ring.")
	x.gauge("syslog_ring_items", "", m.RingItems.Load())
	x.header("syslog_ring_bytes", "gauge",
		"Raw-line bytes currently held by the ring. The envelope is not counted: "+
			"it does not exist while the item waits.")
	x.gauge("syslog_ring_bytes", "", m.RingBytes.Load())
	x.header("syslog_ring_capacity_items", "gauge", "Configured ring item ceiling.")
	x.gauge("syslog_ring_capacity_items", "", m.RingCapacityItems.Load())
	x.header("syslog_ring_capacity_bytes", "gauge", "Configured ring byte ceiling.")
	x.gauge("syslog_ring_capacity_bytes", "", m.RingCapacityBytes.Load())
	x.header("syslog_ring_quarantined_total", "counter",
		"ring=sqlite rows whose head could not be decoded, moved to the quarantine "+
			"table of ring.db for inspection. Anything above zero deserves a look.")
	x.counter("syslog_ring_quarantined_total", "", "", m.RingQuarantinedTotal.Load())

	x.header("syslog_sink_batch_size", "histogram", "Items per sink write.")
	x.histogram("syslog_sink_batch_size", "", m.SinkBatchSize)
	x.header("syslog_sink_duration_seconds", "histogram", "Duration of one sink write.")
	x.histogram("syslog_sink_duration_seconds", "", m.SinkDuration)
	x.header("syslog_sink_health_duration_seconds", "histogram",
		"Duration of one sink health check.")
	x.histogram("syslog_sink_health_duration_seconds", "", m.SinkHealthDuration)

	x.header("syslog_sink_errors_total", "counter", "Sink failures, by class.")
	for i, name := range sinkErrNames {
		x.counter("syslog_sink_errors_total", "", lbl("kind", name), m.SinkErrorsTotal[i].Load())
	}
	x.header("syslog_sink_up", "gauge", "1 when the sink is healthy, 0 when it is DOWN.")
	x.gauge("syslog_sink_up", "", m.SinkUp.Load())

	x.header("syslog_events_written_total", "counter", "Events confirmed by the sink.")
	x.counter("syslog_events_written_total", "", "", m.EventsWrittenTotal.Load())
	x.header("syslog_events_dropped_total", "counter",
		"Accepted events that were lost afterwards, by reason. Anything here is a "+
			"message the server had already taken responsibility for.")
	for i, name := range eventDropNames {
		x.counter("syslog_events_dropped_total", "", lbl("reason", name),
			m.EventsDroppedTotal[i].Load())
	}

	x.header("syslog_emergency_files", "gauge", "Emergency dump files awaiting reingestion.")
	x.gauge("syslog_emergency_files", "", m.EmergencyFiles.Load())
	x.header("syslog_emergency_events_pending", "gauge", "Events held in emergency files.")
	x.gauge("syslog_emergency_events_pending", "", m.EmergencyEventsPending.Load())

	x.header("syslog_ratelimit_buckets", "gauge", "Network prefixes tracked by the limiter.")
	x.gauge("syslog_ratelimit_buckets", "", m.RateLimitBuckets.Load())

	x.header("syslog_sink_sqlite_file_bytes", "gauge",
		"Size of the current events database file.")
	x.gauge("syslog_sink_sqlite_file_bytes", "", m.SinkSQLiteFileBytes.Load())
	x.header("syslog_sink_sqlite_rotations_total", "counter", "Events database rotations.")
	x.counter("syslog_sink_sqlite_rotations_total", "", "", m.SinkSQLiteRotationsTotal.Load())
	x.header("syslog_sink_sqlite_files_removed_total", "counter",
		"Rotated files removed by keep_files.")
	x.counter("syslog_sink_sqlite_files_removed_total", "", "", m.SinkSQLiteFilesRemovedTotal.Load())

	x.header("syslog_sink_postgres_duplicates_total", "counter",
		"Rows ignored by ON CONFLICT: events an earlier attempt had already delivered.")
	x.counter("syslog_sink_postgres_duplicates_total", "", "", m.SinkPostgresDuplicatesTotal.Load())
	x.header("syslog_sink_postgres_pool_conns", "gauge", "Connection pool occupancy, by state.")
	for i, name := range poolStateNames {
		x.gauge("syslog_sink_postgres_pool_conns", lbl("state", name),
			m.SinkPostgresPoolConns[i].Load())
	}
	x.header("syslog_sink_postgres_pool_waits_total", "counter",
		"Acquires that found no idle connection and had to wait or open one.")
	x.counter("syslog_sink_postgres_pool_waits_total", "", "", m.SinkPostgresPoolWaitsTotal.Load())

	x.header("syslog_sink_mysql_duplicates_total", "counter",
		"Rows left untouched by ON DUPLICATE KEY: events an earlier attempt had already delivered.")
	x.counter("syslog_sink_mysql_duplicates_total", "", "", m.SinkMySQLDuplicatesTotal.Load())
	x.header("syslog_sink_mysql_pool_conns", "gauge", "Connection pool occupancy, by state.")
	for i, name := range poolStateNames {
		x.gauge("syslog_sink_mysql_pool_conns", lbl("state", name), m.SinkMySQLPoolConns[i].Load())
	}
	x.header("syslog_sink_mysql_pool_waits_total", "counter",
		"Acquires that had to wait for a connection.")
	x.counter("syslog_sink_mysql_pool_waits_total", "", "", m.SinkMySQLPoolWaitsTotal.Load())

	x.header("syslog_sink_nats_duplicates_total", "counter",
		"Publishes the stream acknowledged as duplicates by Nats-Msg-Id: events an earlier attempt had already delivered.")
	x.counter("syslog_sink_nats_duplicates_total", "", "", m.SinkNATSDuplicatesTotal.Load())
	x.header("syslog_sink_nats_reconnects_total", "counter", "Reconnections of the NATS client.")
	x.counter("syslog_sink_nats_reconnects_total", "", "", m.SinkNATSReconnectsTotal.Load())

	x.header("syslog_config_info", "gauge",
		"Active ring mode, sink type and enabled transports.")
	x.sample("syslog_config_info",
		lbl("ring_mode", m.ringMode)+","+lbl("sink_type", m.sinkType)+","+
			lbl("transports", m.transports), "1")

	x.header("syslog_build_info", "gauge", "Build identity.")
	x.sample("syslog_build_info",
		lbl("version", m.buildVersion)+","+lbl("commit", m.buildCommit)+","+
			lbl("go", m.buildGo), "1")

	x.flush()
	return x.err
}

// --------------------------------------------------------------------- writer

// writer buffers the exposition so a scrape is a handful of syscalls rather
// than one per sample.
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
