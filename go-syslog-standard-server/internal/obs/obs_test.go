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
	m := NewMetrics("memory", "redis", "udp,tcp", BuildInfo{Version: "v", Commit: "c", Go: "go"})
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
		"parse":    parseDurationBounds,
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
	m := NewMetrics("memory", "none", "udp", BuildInfo{})
	for _, n := range []int64{0, 100, 5000, 1 << 30} {
		m.MessageBytes.Observe(n)
	}
	out := scrape(t, m)
	inf := regexp.MustCompile(`syslog_message_bytes_bucket\{le="\+Inf"\} (\d+)`).FindStringSubmatch(out)
	cnt := regexp.MustCompile(`syslog_message_bytes_count (\d+)`).FindStringSubmatch(out)
	if inf == nil || cnt == nil || inf[1] != cnt[1] || cnt[1] != "4" {
		t.Fatalf("+Inf=%v count=%v", inf, cnt)
	}
}

// No label value may derive from emitter content. Every label value that
// appears in the exposition must come from a closed, compile-time set.
func TestLabelsAreAClosedSet(t *testing.T) {
	m := NewMetrics("sqlite", "postgres", "udp,tcp,unix,relp", BuildInfo{Version: "v1", Commit: "abc", Go: "go1.25"})
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
	for _, s := range rfcNames {
		allowed[s] = true
	}
	for _, s := range anomalyNames {
		allowed[s] = true
	}
	for _, s := range sinkErrNames {
		allowed[s] = true
	}
	for _, s := range relpCommandNames {
		allowed[s] = true
	}
	for _, s := range poolStateNames {
		allowed[s] = true
	}
	// Identity labels are set once, from configuration and the linker.
	for _, s := range []string{"sqlite", "postgres", "udp,tcp,unix,relp", "v1", "abc", "go1.25"} {
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

// SPEC §13.1: the two drop counters have DIFFERENT reason sets. A shared
// enumeration would expose ten series that can never move, each one claiming a
// measurement that does not exist.
func TestDropReasonsAreNotCrossed(t *testing.T) {
	m := NewMetrics("memory", "redis", "udp", BuildInfo{Version: "v", Commit: "c", Go: "go"})
	out := scrape(t, m)
	for _, want := range []string{
		`syslog_messages_dropped_total{reason="ring_full"}`,
		`syslog_messages_dropped_total{reason="shutdown"}`,
		`syslog_events_dropped_total{reason="sink_logic"}`,
		`syslog_events_dropped_total{reason="abort"}`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, never := range []string{
		`syslog_messages_dropped_total{reason="abort"}`,
		`syslog_messages_dropped_total{reason="sink_logic"}`,
		`syslog_events_dropped_total{reason="ring_full"}`,
		`syslog_events_dropped_total{reason="filter"}`,
	} {
		if strings.Contains(out, never) {
			t.Errorf("%s is exposed but can never move", never)
		}
	}
}

func TestFacilityAndSeverityAreNotLabels(t *testing.T) {
	out := scrape(t, NewMetrics("memory", "redis", "udp", BuildInfo{}))
	for _, forbidden := range []string{`facility="`, `severity="`, `hostname="`, `app_name="`} {
		if strings.Contains(out, forbidden) {
			t.Errorf("the exposition carries %s, which SPEC §13.1 forbids", forbidden)
		}
	}
}

func TestBackpressureCounterIsSeconds(t *testing.T) {
	m := NewMetrics("memory", "redis", "tcp", BuildInfo{})
	m.AddBackpressure(TransportTCP, 2500*time.Millisecond)
	m.AddBackpressure(TransportTCP, 1500*time.Millisecond)
	out := scrape(t, m)
	if !strings.Contains(out, `syslog_backpressure_seconds_total{transport="tcp"} 4`) {
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
