package config

import (
	"regexp"
	"strings"
	"testing"
)

// SPEC §8.1 step 2, §10.1, §13.5 (user's decision, 2026-10-14): sink.batch_max_bytes exists, defaults to 16 MiB, reads from the INI,
// the flags and WHS_SINK_BATCH_MAX_BYTES, goes out in --dump-config, and is
// refused outside 1 byte..256 MiB.
func TestSinkBatchMaxBytes(t *testing.T) {
	if got := load(t, Options{}).Config.Sink.BatchMaxBytes; got != 16<<20 {
		t.Errorf("default = %d, want 16 MiB", got)
	}
	if got := load(t, Options{Args: []string{"--sink.batch_max_bytes=256MiB"}}).Config.Sink.BatchMaxBytes; got != 256<<20 {
		t.Errorf("flag at the ceiling = %d, want 256 MiB", got)
	}
	if got := load(t, Options{Args: []string{"--sink.batch_max_bytes=1"}}).Config.Sink.BatchMaxBytes; got != 1 {
		t.Errorf("flag at the floor = %d, want 1 (every event alone)", got)
	}
	l := load(t, Options{LookupEnv: envMap(map[string]string{"WHS_SINK_BATCH_MAX_BYTES": "4MiB"})})
	if got := l.Config.Sink.BatchMaxBytes; got != 4<<20 {
		t.Errorf("env = %d, want 4 MiB", got)
	}
	ini := writeINI(t, "[sink]\nbatch_max_bytes = 32MiB\n")
	if got := load(t, Options{Args: []string{"--config=" + ini}}).Config.Sink.BatchMaxBytes; got != 32<<20 {
		t.Errorf("ini = %d, want 32 MiB", got)
	}

	var dump strings.Builder
	if err := load(t, Options{}).DumpINI(&dump); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^batch_max_bytes\s*= 16MiB$`).MatchString(dump.String()) {
		t.Errorf("--dump-config does not carry the key:\n%s", dump.String())
	}

	for _, bad := range []string{"0", "257MiB", "1GiB"} {
		_, err := Load(Options{Args: []string{"--sink.batch_max_bytes=" + bad}, LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), "sink.batch_max_bytes") {
			t.Errorf("--sink.batch_max_bytes=%s: err = %v, want a refusal naming the key", bad, err)
		}
	}
}
