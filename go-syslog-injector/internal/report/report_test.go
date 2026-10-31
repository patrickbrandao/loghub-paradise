package report

import (
	"math"
	"testing"
	"time"
)

func TestQuantiles(t *testing.T) {
	var h Hist
	for i := 1; i <= 10000; i++ {
		h.Add(time.Duration(i) * time.Microsecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 5000 * time.Microsecond}, {0.9, 9000 * time.Microsecond}, {0.99, 9900 * time.Microsecond}, {1, 10000 * time.Microsecond}} {
		got := h.Quantile(c.q)
		if math.Abs(float64(got-c.want))/float64(c.want) > 0.04 {
			t.Errorf("q%.2f = %v, want ~%v", c.q, got, c.want)
		}
	}
	var o Hist
	o.Add(time.Nanosecond)
	h.Merge(&o)
	if h.min != time.Nanosecond || h.n != 10001 {
		t.Fatalf("merge: min %v n %d", h.min, h.n)
	}
	for ns := uint64(0); ns < 1<<20; ns = ns*3/2 + 1 {
		if b := bucket(ns); bucketLow(b) > ns || bucketLow(b+1) <= ns {
			t.Fatalf("bucket(%d)=%d covers [%d,%d)", ns, b, bucketLow(b), bucketLow(b+1))
		}
	}
}
