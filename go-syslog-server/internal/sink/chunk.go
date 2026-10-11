package sink

import (
	"sync"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// chunk is one Batch under construction or in delivery (SPEC §10.1 step 2).
//
// The JLH of every envelope is serialised straight into buf, contiguously from
// offset zero; ends records where each one stops. The Envelope.JLH slices are
// only cut at seal time, so a buffer that grows (and moves) while the chunk is
// being filled never leaves an envelope pointing at the old array.
type chunk struct {
	Batch

	buf  []byte
	ends []int

	// items is every item the chunk confirms, in dequeue order: the ones with
	// an envelope AND the malformed ones refused by syslog.accept_malformed =
	// false, which are committed with the chunk and never written (SPEC §10.1).
	items   []*item.Item
	refused []*item.Item

	// tainted records that an attempt ended by the clock (or the destination
	// said a lower layer may still read the buffer). From then on buf goes to
	// the GC, never to the pool, even if a retry succeeds (SPEC §15.2).
	tainted bool
}

func (c *chunk) add(it *item.Item, res convert.Result, size int) {
	c.Envelopes = append(c.Envelopes, Envelope{Item: it, Res: res})
	c.Bytes += size
	c.ends = append(c.ends, c.Bytes)
	c.items = append(c.items, it)
}

func (c *chunk) refuse(it *item.Item) {
	c.items = append(c.items, it)
	c.refused = append(c.refused, it)
}

// seal cuts the JLH slices. Each is capacity-limited to its own length: a
// destination that appended to one would otherwise overwrite the next envelope.
func (c *chunk) seal() {
	start := 0
	for i := range c.Envelopes {
		end := c.ends[i]
		c.Envelopes[i].JLH = c.buf[start:end:end]
		start = end
	}
}

// taint marks the chunk and its items: neither the chunk buffer nor the raw
// lines return to a pool any more (SPEC §10.2, §15.2).
func (c *chunk) taint() {
	c.tainted = true
	for _, it := range c.items {
		it.Taint()
	}
}

// reset empties the chunk, handing its buffer back to the pool unless it is
// tainted. Every reference is cleared, not just truncated: a slot left holding
// an item or a JLH would pin buffers that already went back to their pools.
func (c *chunk) reset(pool *bufPool) {
	if !c.tainted {
		pool.put(c.buf)
	}
	c.buf = nil
	clear(c.Envelopes)
	c.Envelopes = c.Envelopes[:0]
	c.Bytes = 0
	c.ends = c.ends[:0]
	clear(c.items)
	c.items = c.items[:0]
	clear(c.refused)
	c.refused = c.refused[:0]
	c.tainted = false
}

// bufPool holds chunk buffers, in ONE class: the ceiling, sink.batch_max_bytes
// (SPEC §15.3). A buffer that grew past it -- one JLH larger than the ceiling,
// which forms a chunk alone -- is never pooled: pooling the worst case makes
// the pool hold the worst case forever.
type bufPool struct {
	max int
	p   sync.Pool
}

// bufInitial is a new buffer's starting capacity. Chunks grow to the ceiling by
// doubling; starting at the ceiling would make every idle worker hold 16 MiB.
const bufInitial = 64 << 10

func newBufPool(max int) *bufPool { return &bufPool{max: max} }

func (bp *bufPool) get() []byte {
	if b, ok := bp.p.Get().(*[]byte); ok {
		return (*b)[:0]
	}
	return make([]byte, 0, min(bufInitial, bp.max))
}

func (bp *bufPool) put(b []byte) {
	if b == nil || cap(b) > bp.max {
		return
	}
	b = b[:0]
	bp.p.Put(&b)
}

// builder converts items into chunks for one drain worker, or for one ring=none
// write at a time. It owns a Converter, which is not safe for concurrent use.
//
// Two chunks alternate: cur is the one being filled or delivered, next holds
// the item whose JLH crossed the ceiling and therefore opens the following
// chunk (SPEC §10.1: "o item que passou abre o pedaço seguinte").
type builder struct {
	conv   *convert.Converter
	pool   *bufPool
	max    int
	accept bool
	m      *obs.Metrics

	cur, next *chunk
}

func newBuilder(cfg convert.Config, max int, accept bool, pool *bufPool, m *obs.Metrics) *builder {
	return &builder{
		conv: convert.New(cfg), pool: pool, max: max, accept: accept, m: m,
		cur: &chunk{}, next: &chunk{},
	}
}

// reserve makes room for n more bytes in buf without letting it pass the
// ceiling when the content does not need to. append's own growth would: a
// 12 MiB chunk doubled to 24 MiB of capacity is a buffer the pool must refuse,
// and every full chunk would then cost a fresh allocation.
func (b *builder) reserve(buf []byte, n int) []byte {
	if buf == nil {
		buf = b.pool.get()
	}
	if cap(buf)-len(buf) >= n {
		return buf
	}
	need := len(buf) + n
	c := 2 * cap(buf)
	if c < need {
		c = need
	}
	if c > b.max && need <= b.max {
		c = b.max
	}
	nb := make([]byte, len(buf), c)
	copy(nb, buf)
	return nb
}

// fill converts items into b.cur until the chunk closes or the items run out,
// and returns how many it consumed (SPEC §10.1 step 2).
//
// The chunk closes when the sum of its JLH would pass the ceiling. The item
// that would pass it opens the next chunk, in b.next, and a JLH larger than the
// ceiling forms a chunk alone. A refused malformed message (accept=false) gets
// no envelope: it joins the chunk's items only to be committed with it.
//
// Each item is first sized by its worst case (SPEC §3.9): while the worst case
// still fits, the JLH is serialised straight into the chunk; when it might not,
// it is serialised into the next chunk's buffer and copied back if it turns
// out to fit. Either way no buffer grows past the ceiling unless one JLH alone
// is larger than it.
func (b *builder) fill(items []*item.Item) int {
	c, nx := b.cur, b.next
	start := time.Now()
	n := 0
	defer func() {
		if n > 0 && b.m != nil {
			// One observation per chunk, divided, rather than one per message:
			// the histogram answers "is the conversion cheap relative to the
			// write", and a binary search per message to ask it would be
			// self-defeating.
			b.m.ConvertDuration.ObserveDuration(time.Since(start) / time.Duration(n))
		}
	}()

	for n < len(items) {
		it := items[n]
		n++
		worst := int(MaxEventBytes(int64(len(it.Raw))))

		if len(c.Envelopes) == 0 || len(c.buf)+worst <= b.max {
			c.buf = b.reserve(c.buf, worst)
			off := len(c.buf)
			var res convert.Result
			c.buf, res = b.conv.Append(c.buf, &it.Head, it.Raw)
			b.observe(res)
			if b.refused(res) {
				c.buf = c.buf[:off]
				c.refuse(it)
				continue
			}
			c.add(it, res, len(c.buf)-off)
			continue
		}

		// It might cross: serialise into the next chunk, which is empty here
		// because a carry always ends the fill.
		nx.buf = b.reserve(nx.buf, worst)
		var res convert.Result
		nx.buf, res = b.conv.Append(nx.buf, &it.Head, it.Raw)
		b.observe(res)
		if b.refused(res) {
			nx.buf = nx.buf[:0]
			c.refuse(it)
			continue
		}
		size := len(nx.buf)
		if c.Bytes+size <= b.max {
			c.buf = b.reserve(c.buf, size)
			c.buf = append(c.buf, nx.buf...)
			nx.buf = nx.buf[:0]
			c.add(it, res, size)
			continue
		}
		nx.add(it, res, size)
		break
	}
	return n
}

// refused applies syslog.accept_malformed = false (SPEC §6.6).
func (b *builder) refused(res convert.Result) bool {
	return !b.accept && res.Format == convert.FormatMalformed
}

// observe feeds the conversion metrics: one count per format, and one per
// warning bit (SPEC §14.1).
func (b *builder) observe(res convert.Result) {
	if b.m == nil {
		return
	}
	if res.Format < convert.NumFormats {
		b.m.ConvertTotal[res.Format].Inc()
	}
	if res.Warnings.Empty() {
		return
	}
	for w := convert.Warning(0); w < convert.NumWarnings; w++ {
		if res.Warnings.Has(w) {
			b.m.ConvertWarningsTotal[w].Inc()
		}
	}
}

// advance finishes b.cur, after a delivery or a deliberate drop, and moves on
// to the chunk the carried item opened.
func (b *builder) advance() {
	b.cur.reset(b.pool)
	b.cur, b.next = b.next, b.cur
}

// release finishes both chunks. A tainted one leaves its buffer to the GC.
func (b *builder) release() {
	b.cur.reset(b.pool)
	b.next.reset(b.pool)
}
