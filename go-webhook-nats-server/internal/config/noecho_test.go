package config

import (
	"strings"
	"testing"
)

// SPEC §13.2 (independent review of 2026-11-07, ported from
// go-webhook-server; project rule): a configuration error names the key and
// the problem, never the value -- it may be a password pasted into the wrong
// key -- nor the text of the strconv/net/netip error that repeats it.
func TestConfigErrorsNeverEchoTheValue(t *testing.T) {
	const m = "Hunter2Marker"
	cases := []struct {
		name string
		args []string
		env  map[string]string
		key  string
	}{
		{"int flag", []string{"--sink.workers=" + m}, nil, "--sink.workers"},
		{"int env", nil, map[string]string{"WHS_SINK_WORKERS": m}, "WHS_SINK_WORKERS"},
		{"bool", []string{"--sink.nats.tls=" + m}, nil, "--sink.nats.tls"},
		{"duration", []string{"--sink.op_timeout=" + m}, nil, "--sink.op_timeout"},
		{"bytes", []string{"--sink.batch_max_bytes=" + m}, nil, "--sink.batch_max_bytes"},
		{"bytes with unit", []string{"--sink.batch_max_bytes=" + m + "MiB"}, nil, "--sink.batch_max_bytes"},
		{"ini int", []string{"--config", "INI:[sink]\nworkers = " + m + "\n"}, nil, "sink.workers"},
		{"ring.mode", []string{"--ring.mode=" + m}, nil, "ring.mode"},
		{"sink.type", []string{"--sink.type=" + m}, nil, "sink.type"},
		{"log.level", []string{"--log.level=" + m}, nil, "log.level"},
		{"log.format", []string{"--log.format=" + m}, nil, "log.format"},
		{"admin.bind", []string{"--admin.bind=" + m}, nil, "admin.bind"},
		{"proxy.trusted address", []string{"--proxy.trusted=" + m}, nil, "proxy.trusted"},
		{"proxy.trusted cidr", []string{"--proxy.trusted=10.0.0.0/8," + m + "/8"}, nil, "proxy.trusted"},
		{"seconds", []string{"--loghub.beacon_interval=" + m}, nil, "--loghub.beacon_interval"},
		{"seconds negative", []string{"--loghub.beacon_interval=-" + m}, nil, "--loghub.beacon_interval"},
		{"nats stream", []string{"--sink.nats.stream=" + m + ".x"}, nil, "sink.nats.stream"},
		{"nats mode", []string{"--sink.nats.mode=" + m}, nil, "sink.nats.mode"},
		{"nats subject", []string{"--sink.nats.subject=" + m}, nil, "sink.nats.subject"},
		{"ctl namespace", []string{"--loghub.ctl_namespace=a." + m + "*"}, nil, "loghub.ctl_namespace"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string(nil), tc.args...)
			for i, a := range args {
				if body, ok := strings.CutPrefix(a, "INI:"); ok {
					args[i] = writeINI(t, body)
				}
			}
			_, err := Load(Options{Args: args, LookupEnv: envMap(tc.env)})
			if err == nil {
				t.Fatal("accepted")
			}
			msg := err.Error()
			if strings.Contains(strings.ToLower(msg), strings.ToLower(m)) {
				t.Errorf("the error echoes the value: %q", msg)
			}
			if !strings.Contains(msg, tc.key) {
				t.Errorf("the error does not name %s: %q", tc.key, msg)
			}
		})
	}
}
