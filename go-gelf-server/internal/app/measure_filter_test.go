package app

import (
	"bufio"
	"os"
	"syscall"
	"testing"
	"time"
)

// TestMeasureFilterDoubleValidation is the end-to-end measure of SPEC §7 (the
// §22.1 item 3 closed on 2026-11-07): with the filter active the document is
// validated twice, at the reception (LevelOf) and in the drain (the
// conversion). It pushes the same documents through TCP, the memory ring, the
// drain and sink=none with the filter off (min_level=debug, nothing can be
// refused, LevelOf never runs) and on (min_level=info: every document is
// level 6 and passes, so both runs convert and deliver the same events).
// Reported: wall throughput and process CPU per document. It is a measure,
// not a test, and runs only with GELF_MEASURE=1:
//
//	GELF_MEASURE=1 go test -run MeasureFilter -v -count=1 ./internal/app/
func TestMeasureFilterDoubleValidation(t *testing.T) {
	if os.Getenv("GELF_MEASURE") == "" {
		t.Skip("a measure: GELF_MEASURE=1 runs it")
	}
	doc, err := os.ReadFile("../convert/testdata/corpus/docker-gelf-driver.gelf")
	if err != nil {
		t.Fatal(err)
	}
	const n = 200000
	run := func(minLevel string) (time.Duration, time.Duration) {
		in := start(t, t.TempDir(), "--ring.mode=memory", "--sink.type=none",
			"--filter.min_level="+minLevel)
		c := dialTCP(t, in.app.TransportAddr("tcp"))
		defer c.Close()
		cpu0 := cpuTime(t)
		t0 := time.Now()
		w := bufio.NewWriterSize(c, 64<<10)
		for i := 0; i < n; i++ {
			w.Write(doc)
			w.WriteByte(0)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 120*time.Second, "every event written", func() bool {
			return in.app.metrics.EventsWrittenTotal.Load() == n
		})
		wall, cpu := time.Since(t0), cpuTime(t)-cpu0
		in.stop()
		return wall, cpu
	}
	for round := 1; round <= 3; round++ {
		for _, mode := range []struct{ name, level string }{{"off", "debug"}, {"on", "info"}} {
			wall, cpu := run(mode.level)
			t.Logf("round %d filter %-3s  %8.0f docs/s  %6.2f µs CPU/doc  (%d docs of %d B)",
				round, mode.name, n/wall.Seconds(), float64(cpu.Microseconds())/n, n, len(doc))
		}
	}
}

// cpuTime is user + system time of this process so far.
func cpuTime(t *testing.T) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
