package config

import (
	"strings"
	"testing"
)

// The parent's valkey aliases are refused like the redis keys they stand for
// (removed.go, 2026-11-07): the env WARN names them, a flag or an INI key is
// exit 1 naming the parent, and LOGHUB_EB_URL=valkey:// is refused like
// redis://.
func TestValkeyAliasesOfTheParentAreRemoved(t *testing.T) {
	for _, k := range []string{"RELPLOG_SINK_VALKEY_ADDRS", "RELPLOG_RING_VALKEY_PASSWORD"} {
		found := false
		for _, e := range removedEnv {
			found = found || e == k
		}
		if !found {
			t.Errorf("removedEnv lacks %s", k)
		}
	}
	w := removedEnvWarnings(func(k string) (string, bool) { return "x", k == "RELPLOG_SINK_VALKEY_DB" })
	if len(w) != 1 || !strings.Contains(w[0], "RELPLOG_SINK_VALKEY_DB") {
		t.Errorf("env WARN: %v", w)
	}
	for _, name := range []string{"sink.valkey.addrs", "ring.valkey.stream"} {
		if !removedParams[name] {
			t.Errorf("removedParams lacks %s", name)
		}
	}
	for _, sec := range []string{"sink.valkey", "ring.valkey"} {
		if !removedSections[sec] {
			t.Errorf("removedSections lacks [%s]", sec)
		}
	}
	ix := newIndex(Registry(&Config{}))
	if _, err := ix.lookup("sink.valkey.password"); err == nil || !strings.Contains(err.Error(), "go-relplog-server") {
		t.Errorf("--sink.valkey.password: %v; want the refusal naming go-relplog-server", err)
	}
}
