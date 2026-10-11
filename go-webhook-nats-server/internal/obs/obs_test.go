package obs

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ------------------------------------------------------------------ buckets

// SPEC §11.2: the boundaries are a literal table and the `le` strings are
// pre-computed. Prometheus treats `le` as a label STRING, so emitting "0.01" on
// one scrape and "0.010" on the next would create two series and break the
// histogram silently.
func TestBucketLabelsAreStableLiterals(t *testing.T) {
	sets := map[string][]bound{
		"request_duration": requestDurationBounds,
		"sink_duration":    sinkDurationBounds,
		"sink_health":      sinkHealthBounds,
		"body_bytes":       bodyBytesBounds,
		"sink_batch":       sinkBatchBounds,
	}
	for name, bs := range sets {
		t.Run(name, func(t *testing.T) {
			seen := map[string]bool{}
			for i, b := range bs {
				if b.le == "" {
					t.Errorf("boundary %d has no label", i)
				}
				if seen[b.le] {
					t.Errorf("duplicate le label %q", b.le)
				}
				seen[b.le] = true

				// The label must parse back to the boundary it names, or the
				// exported series describes a different bucket than the code
				// counts into.
				got, err := strconv.ParseFloat(b.le, 64)
				if err != nil {
					t.Errorf("le %q does not parse: %v", b.le, err)
					continue
				}
				if got != b.v {
					t.Errorf("le %q parses to %g, but the boundary is %g", b.le, got, b.v)
				}
			}
			// Ascending order is required by the exposition format and by the
			// binary search.
			for i := 1; i < len(bs); i++ {
				if bs[i].v <= bs[i-1].v {
					t.Errorf("boundary %d (%g) does not exceed %d (%g)", i, bs[i].v, i-1, bs[i-1].v)
				}
			}
		})
	}
}

// SPEC §15.5: the two acceptance criteria are 10ms and 25ms, and putting exact
// boundaries there is what turns the SLO check into a division of two counters
// with no interpolation.
func TestSLOBoundariesAreExact(t *testing.T) {
	want := map[float64]string{0.010: "0.01", 0.025: "0.025"}
	found := map[float64]bool{}
	for _, b := range requestDurationBounds {
		if le, ok := want[b.v]; ok {
			found[b.v] = true
			if b.le != le {
				t.Errorf("boundary %g is labelled %q, want %q", b.v, b.le, le)
			}
		}
	}
	for v := range want {
		if !found[v] {
			t.Errorf("no boundary at %gs; the SLO would have to be interpolated", v)
		}
	}
}

// SPEC §11.2: four boundaries mirror the sync.Pool size classes of §12.2, which
// is why this set is not plain powers of two -- the histogram then reports the
// pool hit distribution directly.
func TestBodyBucketsMatchPoolClasses(t *testing.T) {
	for _, class := range []float64{4096, 16384, 65536, 262144} {
		var found bool
		for _, b := range bodyBytesBounds {
			if b.v == class {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no boundary at %g; the histogram cannot report pool class usage", class)
		}
	}
}

// ---------------------------------------------------------------- histogram

func TestHistogramSearch(t *testing.T) {
	h := newHistogram([]bound{{1, "1"}, {5, "5"}, {10, "10"}}, unitCount)
	cases := map[float64]int{
		0: 0, 0.5: 0, 1: 0, // <= 1 lands in the first bucket
		1.1: 1, 5: 1, // <= 5
		5.1: 2, 10: 2, // <= 10
		10.1: 3, 1e9: 3, // +Inf overflow
	}
	for v, want := range cases {
		if got := h.search(v); got != want {
			t.Errorf("search(%g) = %d, want %d", v, got, want)
		}
	}
}

func TestHistogramObserve(t *testing.T) {
	h := newHistogram([]bound{{1, "1"}, {10, "10"}}, unitCount)
	for _, v := range []int64{0, 1, 5, 10, 100} {
		h.Observe(v)
	}
	if got := h.count.Load(); got != 5 {
		t.Errorf("count = %d, want 5", got)
	}
	if got := h.sum.Load(); got != 116 {
		t.Errorf("sum = %d, want 116", got)
	}
	// Counts are stored NON-cumulatively; the exposition accumulates them.
	if got := h.counts[0].Load(); got != 2 { // 0 and 1
		t.Errorf("bucket 0 = %d, want 2", got)
	}
	if got := h.counts[2].Load(); got != 1 { // 100 overflows
		t.Errorf("+Inf bucket = %d, want 1", got)
	}
}

// Durations accumulate as integer nanoseconds and are divided only at scrape
// time, so there is no float compare-and-swap on the hot path.
func TestHistogramDurationSum(t *testing.T) {
	h := newHistogram(requestDurationBounds, unitSeconds)
	h.ObserveDuration(1500 * time.Millisecond)
	h.ObserveDuration(500 * time.Millisecond)

	if got := h.sumValue(); got != "2" {
		t.Errorf("sum = %q, want \"2\" seconds", got)
	}
	// A negative duration would come from a clock going backwards; it must not
	// underflow the unsigned sum.
	h.ObserveDuration(-time.Second)
	if v, err := strconv.ParseFloat(h.sumValue(), 64); err != nil || v < 0 {
		t.Errorf("sum = %q after a negative observation", h.sumValue())
	}
}

func TestHistogramConcurrent(t *testing.T) {
	h := newHistogram(requestDurationBounds, unitSeconds)
	const goroutines, each = 8, 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				h.ObserveDuration(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	if got := h.count.Load(); got != goroutines*each {
		t.Errorf("count = %d, want %d", got, goroutines*each)
	}
	var total uint64
	for i := range h.counts {
		total += h.counts[i].Load()
	}
	if total != goroutines*each {
		t.Errorf("buckets total %d, want %d", total, goroutines*each)
	}
}

// --------------------------------------------------------------- exposition

func render(t *testing.T, m *Metrics) string {
	t.Helper()
	var buf bytes.Buffer
	if err := m.WritePrometheus(&buf); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	return buf.String()
}

// The invariant that matters most: if +Inf ever diverges from _count, every
// quantile computed from that histogram is silently wrong.
func TestExpositionInfEqualsCount(t *testing.T) {
	m := NewMetrics("v", "c", "go", "memory", "nats")
	for i := 0; i < 37; i++ {
		m.ObserveRequest(200, Accepted, time.Duration(i)*time.Millisecond)
	}
	for i := 0; i < 11; i++ {
		m.ObserveRequest(429, Rejected, time.Microsecond)
	}
	m.BodyBytes.Observe(1024)
	m.SinkBatchSize.Observe(256)
	m.SinkDuration.ObserveDuration(3 * time.Millisecond)

	infs, counts := map[string]string{}, map[string]string{}
	for _, line := range strings.Split(render(t, m), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, _ := strings.Cut(line, " ")
		switch {
		case strings.Contains(name, `le="+Inf"`):
			base, labels := splitName(name)
			infs[strings.TrimSuffix(base, "_bucket")+"|"+dropLE(labels)] = value
		case strings.HasSuffix(name, "_count") || strings.Contains(name, "_count{"):
			base, labels := splitName(name)
			counts[strings.TrimSuffix(base, "_count")+"|"+labels] = value
		}
	}

	// The duration histogram carries {outcome}, so it contributes two series.
	want := []string{
		`whs_request_duration_seconds|outcome="accepted"`,
		`whs_request_duration_seconds|outcome="rejected"`,
		"whs_body_bytes|",
		"whs_sink_batch_size|",
		"whs_sink_duration_seconds|",
		"whs_sink_health_duration_seconds|",
	}
	for _, k := range want {
		if _, ok := infs[k]; !ok {
			t.Errorf("histogram %s was not exported", k)
		}
	}
	if len(infs) != len(want) {
		t.Errorf("%d histogram series exported, want %d", len(infs), len(want))
	}
	for key, inf := range infs {
		if got := counts[key]; got != inf {
			t.Errorf("%s: +Inf = %s but _count = %s", key, inf, got)
		}
	}
}

func splitName(s string) (name, labels string) {
	i := strings.IndexByte(s, '{')
	if i < 0 {
		return s, ""
	}
	return s[:i], strings.TrimSuffix(s[i+1:], "}")
}

func dropLE(labels string) string {
	var keep []string
	for _, p := range strings.Split(labels, ",") {
		if !strings.HasPrefix(p, "le=") {
			keep = append(keep, p)
		}
	}
	return strings.Join(keep, ",")
}

// Buckets must be cumulative and non-decreasing, or histogram_quantile produces
// nonsense from a mathematically impossible histogram.
func TestExpositionBucketsAreCumulative(t *testing.T) {
	m := NewMetrics("v", "c", "go", "memory", "none")
	for _, d := range []time.Duration{
		10 * time.Microsecond, 200 * time.Microsecond, 3 * time.Millisecond,
		30 * time.Millisecond, 2 * time.Second, time.Minute,
	} {
		m.ObserveRequest(200, Accepted, d)
	}

	var last uint64
	var seen bool
	for _, line := range strings.Split(render(t, m), "\n") {
		if !strings.HasPrefix(line, "whs_request_duration_seconds_bucket{outcome=\"accepted\"") {
			continue
		}
		_, value, _ := strings.Cut(line, " ")
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			t.Fatalf("bucket value %q: %v", value, err)
		}
		if seen && v < last {
			t.Errorf("bucket counts decreased: %d after %d in %q", v, last, line)
		}
		last, seen = v, true
	}
	if !seen {
		t.Fatal("no accepted-duration buckets were exported")
	}
	if last != 6 {
		t.Errorf("final cumulative bucket = %d, want 6", last)
	}
}

// SPEC §11.1: every label value set is finite and known at compile time. Nothing
// derives from a path, a method, an IP or a user agent.
func TestExpositionCardinalityIsFixed(t *testing.T) {
	m := NewMetrics("v", "c", "go", "memory", "nats")

	// Codes outside the enumerated set collapse into "other" rather than
	// creating a new series.
	for _, code := range []int{200, 418, 599, 302} {
		m.ObserveRequest(code, Accepted, time.Millisecond)
	}
	out := render(t, m)

	if strings.Contains(out, `code="418"`) || strings.Contains(out, `code="599"`) {
		t.Error("an unenumerated status code created its own series")
	}
	if !strings.Contains(out, `whs_requests_total{code="other"} 3`) {
		t.Error("unenumerated codes should aggregate into code=\"other\"")
	}

	// The series count must not depend on traffic.
	before := len(strings.Split(out, "\n"))
	for i := 0; i < 5000; i++ {
		m.ObserveRequest(200, Accepted, time.Duration(i)*time.Microsecond)
		m.Reject(Reason(i % int(numReasons)))
	}
	after := len(strings.Split(render(t, m), "\n"))
	if before != after {
		t.Errorf("the exported series count changed with traffic: %d then %d", before, after)
	}
}

func TestExpositionInfoLabels(t *testing.T) {
	m := NewMetrics("1.2.3", "abc123", "go1.27", "memory", "nats")
	out := render(t, m)

	if !strings.Contains(out, `whs_config_info{ring_mode="memory",sink_type="nats"} 1`) {
		t.Error("whs_config_info does not report the active modes")
	}
	if !strings.Contains(out, `whs_build_info{version="1.2.3",commit="abc123",go="go1.27"} 1`) {
		t.Error("whs_build_info does not report the build identity")
	}
}

// SPEC §8.7.8, §11.1: the NATS series keep the fast's names, the beacon's has
// its two results from the first scrape, and nothing of the PostgreSQL sink is
// left.
func TestExpositionNATSAndBeaconSeries(t *testing.T) {
	m := NewMetrics("v", "c", "go", "memory", "nats")
	m.SinkNATSDuplicatesTotal.Add(3)
	m.SinkNATSReconnectsTotal.Inc()
	m.BeaconsOK.Add(5)
	m.BeaconsError.Add(2)
	out := render(t, m)
	for _, want := range []string{
		"whs_sink_nats_duplicates_total 3",
		"whs_sink_nats_reconnects_total 1",
		`whs_beacons_total{result="ok"} 5`,
		`whs_beacons_total{result="error"} 2`,
		"# TYPE whs_beacons_total counter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition lacks %q", want)
		}
	}
	if strings.Contains(out, "postgres") {
		t.Error("a PostgreSQL series survived the swap")
	}
}

// Label values must be escaped, or a value containing a quote produces an
// unparseable exposition.
func TestLabelEscaping(t *testing.T) {
	m := NewMetrics(`v"1`, "c\\x", "go\ntest", "memory", "none")
	out := render(t, m)

	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "whs_build_info") {
			if strings.Contains(line, "\n") {
				t.Error("a raw newline reached the exposition")
			}
			if !strings.Contains(line, `\"`) || !strings.Contains(line, `\\`) {
				t.Errorf("label was not escaped: %q", line)
			}
			return
		}
	}
	t.Error("whs_build_info was not exported")
}

func TestEveryMetricHasHelpAndType(t *testing.T) {
	out := render(t, NewMetrics("v", "c", "go", "memory", "none"))

	var help, typ int
	names := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			help++
		case strings.HasPrefix(line, "# TYPE "):
			typ++
		case line != "" && !strings.HasPrefix(line, "#"):
			name, _, _ := strings.Cut(line, " ")
			base, _ := splitName(name)
			base = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(base, "_bucket"), "_sum"), "_count")
			names[base] = true
		}
	}
	if help != typ {
		t.Errorf("%d HELP lines but %d TYPE lines", help, typ)
	}
	if help != len(names) {
		t.Errorf("%d documented metrics but %d distinct names exported", help, len(names))
	}
}

// ------------------------------------------------------------------ counters

func TestCounterAndGauge(t *testing.T) {
	var c Counter
	c.Inc()
	c.Add(5)
	if c.Load() != 6 {
		t.Errorf("counter = %d, want 6", c.Load())
	}

	var g Gauge
	g.Set(10)
	g.Inc()
	g.Dec()
	g.Add(-4)
	if g.Load() != 6 {
		t.Errorf("gauge = %d, want 6", g.Load())
	}
}

// --------------------------------------------------------------------- log

// SPEC §11.3: nothing is written per request on the default path.
func TestAccessSampling(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, "json", "info")

	if l.AccessEnabled() {
		t.Error("access logging must be off by default")
	}

	l.SetAccess(true, 1)
	if !l.AccessEnabled() {
		t.Error("a sample rate of 1 must log everything")
	}

	l.SetAccess(true, 0)
	if l.AccessEnabled() {
		t.Error("a sample rate of 0 must log nothing")
	}

	l.SetAccess(true, 0.5)
	var on int
	for i := 0; i < 2000; i++ {
		if l.AccessEnabled() {
			on++
		}
	}
	if on < 800 || on > 1200 {
		t.Errorf("%d of 2000 sampled at 0.5; far outside the expected range", on)
	}

	// Out-of-range rates are clamped rather than producing nonsense.
	l.SetAccess(true, 5)
	if !l.AccessEnabled() {
		t.Error("a rate above 1 should clamp to always-on")
	}
	l.SetAccess(true, -1)
	if l.AccessEnabled() {
		t.Error("a negative rate should clamp to never")
	}
}

func TestLoggerLevelIsLive(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, "json", "error")

	l.Info("suppressed")
	if buf.Len() != 0 {
		t.Error("an info line was written at error level")
	}

	l.SetLevel("debug")
	l.Debug("now visible")
	if !strings.Contains(buf.String(), "now visible") {
		t.Error("the level change did not take effect")
	}
}

// The sampler exists so a per-request condition that describes a persistent
// misconfiguration does not print on every single request.
func TestSampler(t *testing.T) {
	s := NewSampler(time.Hour)

	ok, suppressed := s.Allow()
	if !ok {
		t.Fatal("the first call must be allowed")
	}
	if suppressed != 0 {
		t.Errorf("suppressed = %d on the first call", suppressed)
	}

	for i := 0; i < 100; i++ {
		if ok, _ := s.Allow(); ok {
			t.Fatalf("call %d was allowed inside the interval", i)
		}
	}

	// A short interval lets the next one through, carrying the count of what
	// it stands for.
	s2 := NewSampler(10 * time.Millisecond)
	s2.Allow()
	for i := 0; i < 5; i++ {
		s2.Allow()
	}
	time.Sleep(20 * time.Millisecond)
	ok, suppressed = s2.Allow()
	if !ok {
		t.Error("the sampler never reopened")
	}
	if suppressed != 5 {
		t.Errorf("suppressed = %d, want 5", suppressed)
	}
}

func TestSamplerConcurrent(t *testing.T) {
	s := NewSampler(time.Hour)
	var allowed int64
	var mu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if ok, _ := s.Allow(); ok {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	// A lost race means two lines instead of one, which is harmless; what must
	// not happen is the flood the sampler exists to prevent.
	if allowed > 20 {
		t.Errorf("%d lines allowed in one interval; the sampler is not limiting", allowed)
	}
	if allowed == 0 {
		t.Error("no line was ever allowed")
	}
}

func TestStatusIndex(t *testing.T) {
	if statusIndex(200) != 0 {
		t.Error("200 should be the first enumerated code")
	}
	if got := statusIndex(999); got != len(statusCodes) {
		t.Errorf("an unenumerated code should map to the 'other' slot, got %d", got)
	}
	if len(statusNames) != len(statusCodes)+1 {
		t.Error("statusNames must cover every code plus 'other'")
	}
}

func TestReasonString(t *testing.T) {
	for i := Reason(0); i < numReasons; i++ {
		if i.String() == "" || i.String() == "unknown" {
			t.Errorf("reason %d has no name", i)
		}
	}
	if Reason(200).String() != "unknown" {
		t.Error("an out-of-range reason should be 'unknown', not a panic")
	}
}

func TestSumValueUnits(t *testing.T) {
	bytesH := newHistogram(bodyBytesBounds, unitBytes)
	bytesH.Observe(1000)
	bytesH.Observe(24)
	if got := bytesH.sumValue(); got != "1024" {
		t.Errorf("byte sum = %q, want a plain integer", got)
	}

	secsH := newHistogram(sinkDurationBounds, unitSeconds)
	secsH.ObserveDuration(1500 * time.Millisecond)
	v, err := strconv.ParseFloat(secsH.sumValue(), 64)
	if err != nil || math.Abs(v-1.5) > 1e-9 {
		t.Errorf("duration sum = %q, want 1.5", secsH.sumValue())
	}
}
