// Package obs holds the metric registry and the structured logger.
//
// The Prometheus text exposition is written by hand (SPEC §14.3). That puts the
// correctness rules of SPEC §11.2 on us rather than on a library, so they are
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
// Bucket boundaries -- SPEC §11.2.
//
// These are STATIC: identical on every instance, independent of
// server.max_body_bytes, sink.batch and sink.op_timeout. Deriving them from
// local configuration would give two differently-configured nodes different
// `le` sets, and
//
//	histogram_quantile(0.99, sum by (le) (rate(..._bucket[5m])))
//
// across them returns garbage with no visible error. Empty buckets cost one
// integer each; portable dashboards, alerts and history are worth far more.

// requestDurationBounds places exact boundaries at 0.010 and 0.025 -- the two
// acceptance criteria of SPEC §15.5. Checking the SLO is then a division of two
// counters with no interpolation at all.
var requestDurationBounds = []bound{
	{0.00005, "0.00005"}, // floor of ring=none + sink=none
	{0.0001, "0.0001"},
	{0.00025, "0.00025"},
	{0.0005, "0.0005"},
	{0.001, "0.001"},
	{0.0025, "0.0025"},
	{0.005, "0.005"},
	{0.010, "0.01"},  // SLO: ring=memory + sink=redis
	{0.025, "0.025"}, // SLO: ring=sqlite, synchronous=FULL
	{0.050, "0.05"},
	{0.100, "0.1"},
	{0.250, "0.25"},
	{0.500, "0.5"},
	{1.0, "1"},
	{5.0, "5"},
	{30.0, "30"}, // ceiling: server.read_timeout
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

// bodyBytesBounds deliberately mirrors the sync.Pool size classes of SPEC §12.2
// at 4/16/64/256 KiB. That is why it is not plain powers of two: the histogram
// then reports the pool hit distribution directly, and says whether the 256 KiB
// pooling cutoff is calibrated for this deployment's real traffic.
var bodyBytesBounds = []bound{
	{0, "0"}, // empty body: GET, HEAD, DELETE
	{64, "64"},
	{256, "256"},
	{1024, "1024"},     // benchmark reference size
	{4096, "4096"},     // pool class
	{16384, "16384"},   // pool class
	{65536, "65536"},   // pool class
	{262144, "262144"}, // pool class / pooling cutoff
	{1048576, "1.048576e+06"},
	{2097152, "2.097152e+06"},
	{5242880, "5.24288e+06"}, // server.max_body_bytes default
	{16777216, "1.6777216e+07"},
	{67108864, "6.7108864e+07"}, // headroom for larger configurations
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
