// Package obs holds the metric registry and the structured logger.
//
// The Prometheus text exposition is written by hand (SPEC §17.3). That puts the
// correctness rules of SPEC §14.1 on us rather than on a library, so they are
// enforced structurally here:
//
//   - Buckets are stored NON-cumulatively and summed at scrape time, so an
//     observation is one binary search plus one atomic add, never O(buckets).
//   - The `le` label strings come from a literal table and are never formatted
//     at scrape time. Prometheus treats `le` as a label STRING: emitting "0.01"
//     on one scrape and "0.010" on the next would create two series and break
//     the histogram silently.
//   - Sums accumulate in integer base units (nanoseconds, bytes) inside an
//     atomic.Uint64. There is no float compare-and-swap on the hot path.
package obs

import (
	"strconv"
	"sync/atomic"
	"time"
)

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n uint64) { c.v.Add(n) }
func (c *Counter) Load() uint64 { return c.v.Load() }

// Gauge is a value that goes up and down.
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64) { g.v.Store(n) }
func (g *Gauge) Add(n int64) { g.v.Add(n) }
func (g *Gauge) Inc()        { g.v.Add(1) }
func (g *Gauge) Dec()        { g.v.Add(-1) }
func (g *Gauge) Load() int64 { return g.v.Load() }

// bound is one histogram boundary together with the exact string used as its
// `le` label. Keeping them paired in a literal table is what guarantees the
// label text is byte-stable for the life of the process.
type bound struct {
	v  float64
	le string
}

// unit selects how a histogram's _sum is converted for exposition.
type unit uint8

const (
	unitSeconds unit = iota // sum accumulates nanoseconds
	unitBytes               // sum accumulates bytes
	unitCount               // sum accumulates a plain count
)

// Histogram has fixed boundaries chosen at construction. It is safe for
// concurrent use.
type Histogram struct {
	bounds []bound
	// counts has len(bounds)+1 entries; the last is the +Inf overflow. Entries
	// are NON-cumulative: WriteTo accumulates them.
	counts []atomic.Uint64
	sum    atomic.Uint64
	count  atomic.Uint64
	unit   unit
}

func newHistogram(bounds []bound, u unit) *Histogram {
	return &Histogram{
		bounds: bounds,
		counts: make([]atomic.Uint64, len(bounds)+1),
		unit:   u,
	}
}

// Count reports how many observations the histogram holds -- the _count series
// of the exposition. It is read-only and exists so a caller can check the
// denominator without parsing a scrape.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// search returns the index of the first bucket whose boundary is >= v, or
// len(bounds) for the +Inf overflow.
func (h *Histogram) search(v float64) int {
	lo, hi := 0, len(h.bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if h.bounds[mid].v < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ObserveDuration records a latency. The sum accumulates nanoseconds as an
// integer and is divided only at scrape time.
func (h *Histogram) ObserveDuration(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.counts[h.search(d.Seconds())].Add(1)
	h.sum.Add(uint64(d))
	h.count.Add(1)
}

// Observe records a size or a count.
func (h *Histogram) Observe(n int64) {
	if n < 0 {
		n = 0
	}
	h.counts[h.search(float64(n))].Add(1)
	h.sum.Add(uint64(n))
	h.count.Add(1)
}

// sumValue renders _sum in the metric's exposed unit.
func (h *Histogram) sumValue() string {
	s := h.sum.Load()
	switch h.unit {
	case unitSeconds:
		// Integer nanoseconds to seconds, without float rounding surprises.
		return strconv.FormatFloat(float64(s)/1e9, 'f', -1, 64)
	default:
		return strconv.FormatUint(s, 10)
	}
}

// ---------------------------------------------------------------------------
// Bucket boundaries -- SPEC §14.1.
//
// These are STATIC: identical on every instance, independent of sink.batch,
// sink.op_timeout and syslog.max_message_bytes. Deriving them from local
// configuration would give two differently-configured nodes different `le`
// sets, and
//
//	histogram_quantile(0.99, sum by (le) (rate(..._bucket[5m])))
//
// across them returns garbage with no visible error. Empty buckets cost one
// integer each; portable dashboards, alerts and history are worth far more.

// confirmDurationBounds measures the time from having the frame to being able
// to confirm it, on the RELP transport (SPEC §2.3, §5.4). The bounds are the
// parent go-relplog-server's, rings it no longer has included, so a dashboard
// built for one reads the other.
//
// The ceiling is 30s because that is the default backpressure_timeout: a
// confirmation slower than that never happens, the connection is closed
// instead.
var confirmDurationBounds = []bound{
	{0.00001, "0.00001"}, // floor: ring=memory, straight into the channel
	{0.000025, "0.000025"},
	{0.00005, "0.00005"},
	{0.0001, "0.0001"},
	{0.00025, "0.00025"},
	{0.0005, "0.0005"},
	{0.001, "0.001"},
	{0.0025, "0.0025"},
	{0.005, "0.005"},
	{0.010, "0.01"},
	{0.020, "0.02"},
	{0.040, "0.04"},
	{0.100, "0.1"},
	{0.500, "0.5"},
	{5.0, "5"},
	{30.0, "30"}, // ceiling: relp.backpressure_timeout default
}

// convertDurationBounds measures the conversion of one message into its JLH
// envelope, which runs in the drain worker (SPEC §2.2).
//
// It answers the question decision 7 left open: was keeping the conversion off
// the receive path the right call? If the p99 here is small relative to sink
// duration, the answer stays yes.
var convertDurationBounds = []bound{
	{0.0000005, "5e-07"}, // 500ns: a short RFC 3164 line with no structured data
	{0.000001, "1e-06"},
	{0.0000025, "2.5e-06"},
	{0.000005, "5e-06"},
	{0.00001, "1e-05"},
	{0.000025, "2.5e-05"},
	{0.00005, "5e-05"},
	{0.0001, "0.0001"},
	{0.001, "0.001"},
	{0.010, "0.01"}, // a pathological line: 64 KiB of control bytes
}

var sinkDurationBounds = []bound{
	{0.0001, "0.0001"},
	{0.00025, "0.00025"},
	{0.0005, "0.0005"},
	{0.001, "0.001"},
	{0.0025, "0.0025"},
	{0.005, "0.005"},
	{0.010, "0.01"},
	{0.025, "0.025"},
	{0.050, "0.05"},
	{0.100, "0.1"},
	{0.250, "0.25"},
	{1.0, "1"},
	{5.0, "5"}, // ceiling: sink.op_timeout default
}

var sinkHealthBounds = []bound{
	{0.0001, "0.0001"},
	{0.00025, "0.00025"},
	{0.0005, "0.0005"},
	{0.001, "0.001"},
	{0.005, "0.005"},
	{0.025, "0.025"},
	{0.100, "0.1"},
	{1.0, "1"}, // ceiling: sink.health_timeout default
}

// messageBytesBounds mirrors the buffer pool classes of SPEC §15.1 at 2, 8 and
// 64 KiB, so the histogram reports the pool hit distribution directly. The low
// end is dense where syslog lives: the median line is 150-250 bytes. It records
// the ORIGINAL size, so a message truncated at syslog.max_message_bytes still
// lands in the bucket of what the emitter sent.
var messageBytesBounds = []bound{
	{0, "0"},
	{64, "64"},
	{128, "128"},
	{192, "192"},
	{256, "256"},
	{512, "512"},
	{1024, "1024"},
	{2048, "2048"}, // pool class
	{4096, "4096"},
	{8192, "8192"}, // pool class; rsyslog maxMessageSize default; RFC 5425 SHOULD
	{16384, "16384"},
	{65536, "65536"}, // pool class; syslog.max_message_bytes default
	{262144, "262144"},
	{1048576, "1.048576e+06"},
	{16777216, "1.6777216e+07"}, // syslog.max_message_bytes ceiling
}

var sinkBatchBounds = []bound{
	{1, "1"},
	{2, "2"},
	{4, "4"},
	{8, "8"},
	{16, "16"},
	{32, "32"},
	{64, "64"},
	{128, "128"},
	{256, "256"}, // sink.batch default
	{1024, "1024"},
	{4096, "4096"},
}

// sinkBatchBytesBounds measures the JLH bytes of one sink write. The ceiling is
// sink.batch_max_bytes (SPEC §10.1); one envelope of the worst case is about
// 776 KiB (SPEC §3.9).
var sinkBatchBytesBounds = []bound{
	{1024, "1024"},
	{4096, "4096"},
	{16384, "16384"},
	{65536, "65536"},
	{262144, "262144"},
	{1048576, "1.048576e+06"},
	{4194304, "4.194304e+06"},
	{16777216, "1.6777216e+07"}, // sink.batch_max_bytes default
	{67108864, "6.7108864e+07"},
}
