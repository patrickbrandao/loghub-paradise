package obs

import (
	"bytes"
	"io"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	var b bytes.Buffer
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// Prometheus treats `le` as a label STRING. Emitting "0.01" on one scrape and
// "0.010" on the next creates two series and breaks every quantile silently, so
// the exposition must be byte-identical across scrapes.
func TestScrapesAreByteStable(t *testing.T) {
	m := NewMetrics(Info{RingMode: "memory", SinkType: "nats", Transports: "tcp", Version: "v", Commit: "c", Go: "go"})
	m.MessageBytes.Observe(200)
	m.ConfirmDuration.ObserveDuration(3 * time.Millisecond)

	first := scrape(t, m)
	second := scrape(t, m)
	if first != second {
		t.Fatal("two scrapes of an unchanged registry differ")
	}
}

// Every bound table must be strictly increasing, and every `le` string must parse
// to the value it stands for. A typo here is invisible until a quantile is wrong.
func TestBoundTablesAreSane(t *testing.T) {
	tables := map[string][]bound{
		"confirm":  confirmDurationBounds,
		"convert":  convertDurationBounds,
		"bytes":    sinkBatchBytesBounds,
		"sink":     sinkDurationBounds,
		"health":   sinkHealthBounds,
		"messages": messageBytesBounds,
		"batch":    sinkBatchBounds,
	}
	for name, tb := range tables {
		for i, b := range tb {
			v, err := strconv.ParseFloat(b.le, 64)
			if err != nil {
				t.Errorf("%s[%d]: le %q does not parse", name, i, b.le)
				continue
			}
			if v != b.v {
				t.Errorf("%s[%d]: le %q means %g, but the bound is %g", name, i, b.le, v, b.v)
			}
			if i > 0 && tb[i-1].v >= b.v {
				t.Errorf("%s[%d]: %g is not above %g", name, i, b.v, tb[i-1].v)
			}
		}
	}
}

// The +Inf bucket must equal _count, by construction.
func TestHistogramInfEqualsCount(t *testing.T) {
	m := NewMetrics(Info{})
	for _, n := range []int64{0, 100, 5000, 1 << 30} {
		m.MessageBytes.Observe(n)
	}
	out := scrape(t, m)
	inf := regexp.MustCompile(`relplog_message_bytes_bucket\{le="\+Inf"\} (\d+)`).FindStringSubmatch(out)
	cnt := regexp.MustCompile(`relplog_message_bytes_count (\d+)`).FindStringSubmatch(out)
	if inf == nil || cnt == nil || inf[1] != cnt[1] || cnt[1] != "4" {
		t.Fatalf("+Inf=%v count=%v", inf, cnt)
	}
}

// No label value may derive from emitter content. Every label value that
// appears in the exposition must come from a closed, compile-time set.
func TestLabelsAreAClosedSet(t *testing.T) {
	m := NewMetrics(Info{RingMode: "memory", SinkType: "nats", Transports: "tcp", Version: "v1", Commit: "abc", Go: "go1.25",
		From: "01962a3f-07bc-70b4-ae05-c57e7541d8a8", Space: "default", AgentName: "go-relplog-nats-server"})
	out := scrape(t, m)

	allowed := map[string]bool{}
	for _, s := range transportNames {
		allowed[s] = true
	}
	for _, s := range dropNames {
		allowed[s] = true
	}
	for _, s := range eventDropNames {
		allowed[s] = true
	}
	for _, s := range formatNames {
		allowed[s] = true
	}
	for _, s := range WarningNames {
		allowed[s] = true
	}
	for _, s := range connLimitNames {
		allowed[s] = true
	}
	for _, s := range proxyResultNames {
		allowed[s] = true
	}
	allowed["ok"], allowed["error"] = true, true
	for _, s := range sinkErrNames {
		allowed[s] = true
	}
	for _, set := range [][]string{relpCommandNames[:], relpCodeNames[:], relpUnansweredNames[:], relpFrameErrorNames[:]} {
		for _, s := range set {
			allowed[s] = true
		}
	}
	// Identity labels are set once, from configuration and the linker.
	for _, s := range []string{"memory", "nats", "tcp", "v1", "abc", "go1.25",
		"01962a3f-07bc-70b4-ae05-c57e7541d8a8", "default", "go-relplog-nats-server"} {
		allowed[s] = true
	}

	re := regexp.MustCompile(`(\w+)="([^"]*)"`)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, m := range re.FindAllStringSubmatch(line, -1) {
			key, val := m[1], m[2]
			if key == "le" {
				continue
			}
			if !allowed[val] {
				t.Errorf("label %s=%q is not from a closed set: %s", key, val, line)
			}
		}
	}
}

// SPEC §14.1: the two drop counters have DIFFERENT reason sets. A shared
// enumeration would expose ten series that can never move, each one claiming a
// measurement that does not exist.
func TestDropReasonsAreNotCrossed(t *testing.T) {
	m := NewMetrics(Info{RingMode: "memory", SinkType: "nats", Transports: "tcp"})
	out := scrape(t, m)
	for _, want := range []string{
		`relplog_messages_dropped_total{reason="empty"}`,
		`relplog_messages_dropped_total{reason="unaddressable"}`,
		`relplog_events_dropped_total{reason="sink_logic"}`,
		`relplog_events_dropped_total{reason="abort"}`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, never := range []string{
		`relplog_messages_dropped_total{reason="abort"}`,
		`relplog_messages_dropped_total{reason="sink_logic"}`,
		`relplog_events_dropped_total{reason="ring_full"}`,
		`relplog_events_dropped_total{reason="filter"}`,
		`relplog_messages_dropped_total{reason="malformed"}`,
		// What RELP hands back to the emitter is not a drop (SPEC §5.7).
		`relplog_messages_dropped_total{reason="ring_full"}`,
		`relplog_messages_dropped_total{reason="sink_down"}`,
		`relplog_messages_dropped_total{reason="shutdown"}`,
		// There is no filter and no rate limiter here (SPEC §1.2).
		`relplog_messages_dropped_total{reason="filter"}`,
		`relplog_messages_dropped_total{reason="ratelimit"}`,
		`relplog_ratelimit_`,
		// Nor do the transports, destinations and features this server does
		// not have expose series (SPEC §1.2, §14.1).
		`transport="udp"`,
		`transport="unix"`,
		`transport="tls"`,
		`relplog_udp_`,
		`relplog_tls_`,
		`relplog_sink_postgres_`,
		`relplog_sink_mysql_`,
		`relplog_sink_sqlite_`,
		`relplog_sink_clickhouse_`,
		`relplog_ring_quarantined_total`,
		`relplog_ring_redis_`,
	} {
		if strings.Contains(out, never) {
			t.Errorf("%s is exposed but can never move", never)
		}
	}
}

// The sink-specific series are this server's: the NATS ones and the beacon are
// exposed (the beacon is kept here, SPEC §11), and nothing of the PostgreSQL
// sink the sibling module had survives. A series that can never move is a
// claim about a measurement that does not exist.
func TestSinkSeriesAreTheNATSOnes(t *testing.T) {
	m := NewMetrics(Info{RingMode: "memory", SinkType: "nats", Transports: "tcp"})
	m.BeaconsOK.Add(3)
	m.BeaconsError.Add(1)
	m.SinkNATSDuplicatesTotal.Add(2)
	m.SinkNATSReconnectsTotal.Add(5)
	out := scrape(t, m)
	for _, want := range []string{
		`relplog_beacons_total{result="ok"} 3`,
		`relplog_beacons_total{result="error"} 1`,
		"relplog_sink_nats_duplicates_total 2",
		"relplog_sink_nats_reconnects_total 5",
		`relplog_config_info{ring_mode="memory",sink_type="nats",transports="tcp"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s:\n%s", want, grep(out, "beacon")+"\n"+grep(out, "nats"))
		}
	}
	for _, never := range []string{"relplog_sink_postgres", "pool_conns", "pool_waits", "relplog_sink_redis",
		"relplog_ring_sqlite", "relplog_ring_redis", "relplog_ring_quarantined", "clickhouse", "mysql",
		"sqlite", "tls", "ratelimit", "filter", "relplog_sink_nats_subject"} {
		if strings.Contains(out, never) {
			t.Errorf("the exposition carries %q, a removed feature:\n%s", never, grep(out, never))
		}
	}
}

func TestFacilityAndSeverityAreNotLabels(t *testing.T) {
	out := scrape(t, NewMetrics(Info{}))
	for _, forbidden := range []string{`facility="`, `severity="`, `hostname="`, `appname="`} {
		if strings.Contains(out, forbidden) {
			t.Errorf("the exposition carries %s, which SPEC §14.1 forbids", forbidden)
		}
	}
}

func TestBackpressureCounterIsSeconds(t *testing.T) {
	m := NewMetrics(Info{})
	m.AddBackpressure(TransportTCP, 2500*time.Millisecond)
	m.AddBackpressure(TransportTCP, 1500*time.Millisecond)
	out := scrape(t, m)
	if !strings.Contains(out, `relplog_backpressure_seconds_total{transport="tcp"} 4`) {
		t.Fatalf("backpressure not exported in seconds:\n%s", grep(out, "backpressure"))
	}
}

func TestSamplerAllowsOncePerInterval(t *testing.T) {
	s := NewSampler(time.Hour)
	if ok, _ := s.Allow(); !ok {
		t.Fatal("the first line was suppressed")
	}
	for i := 0; i < 5; i++ {
		if ok, _ := s.Allow(); ok {
			t.Fatal("a line got through inside the interval")
		}
	}
}

func TestLoggerLevelIsReloadable(t *testing.T) {
	var b bytes.Buffer
	l := NewLogger(&b, "text", "error")
	l.Info("hidden")
	l.SetLevel("info")
	l.Info("visible")
	if strings.Contains(b.String(), "hidden") || !strings.Contains(b.String(), "visible") {
		t.Fatalf("log output = %q", b.String())
	}
	_ = NewLogger(io.Discard, "json", "debug")
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
