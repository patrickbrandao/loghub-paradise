package config

import (
	"strings"
	"testing"
)

// SPEC §13.2 (independent review of 2026-11-07; project rule): a
// configuration error names the key and the problem, never the value -- it
// may be a password pasted into the wrong key. Each case puts the marker in
// the value of one key, as a flag, an environment variable or an INI line,
// and the error must name the key without the marker, nor the text of the
// strconv/net/netip error that repeats it.
func TestConfigErrorsNeverEchoTheValue(t *testing.T) {
	const m = "Hunter2Marker"
	cases := []struct {
		name string
		args []string
		env  map[string]string
		key  string // what the error must name
	}{
		{"int flag", []string{"--sink.redis.db=" + m}, nil, "--sink.redis.db"},
		{"int env", nil, map[string]string{"WHS_SINK_WORKERS": m}, "WHS_SINK_WORKERS"},
		{"int64", []string{"--sink.redis.maxlen=" + m}, nil, "--sink.redis.maxlen"},
		{"bool", []string{"--sink.redis.tls=" + m}, nil, "--sink.redis.tls"},
		{"float", []string{"--ratelimit.rate=" + m}, nil, "--ratelimit.rate"},
		{"duration", []string{"--sink.op_timeout=" + m}, nil, "--sink.op_timeout"},
		{"bytes", []string{"--sink.batch_max_bytes=" + m}, nil, "--sink.batch_max_bytes"},
		{"bytes with unit", []string{"--sink.batch_max_bytes=" + m + "MiB"}, nil, "--sink.batch_max_bytes"},
		{"ini int", []string{"--config", "INI:[sink.redis]\ndb = " + m + "\n"}, nil, "sink.redis.db"},
		{"sink.type", []string{"--sink.type=" + m}, nil, "sink.type"},
		{"ring.mode", []string{"--ring.mode=" + m}, nil, "ring.mode"},
		{"log.level", []string{"--log.level=" + m}, nil, "log.level"},
		{"log.format", []string{"--log.format=" + m}, nil, "log.format"},
		{"rotate", []string{"--sink.sqlite.rotate=" + m}, nil, "sink.sqlite.rotate"},
		{"synchronous", []string{"--ring.sqlite.synchronous=" + m}, nil, "ring.sqlite.synchronous"},
		{"redis addrs", []string{"--sink.redis.addrs=10.0.0.1:6379," + m}, nil, "sink.redis.addrs"},
		{"admin.bind", []string{"--admin.bind=" + m}, nil, "admin.bind"},
		{"proxy.trusted address", []string{"--proxy.trusted=" + m}, nil, "proxy.trusted"},
		{"proxy.trusted cidr", []string{"--proxy.trusted=10.0.0.0/8," + m + "/8"}, nil, "proxy.trusted"},
		{"ratelimit.exempt", []string{"--ratelimit.exempt=" + m}, nil, "ratelimit.exempt"},
		{"postgres table", []string{"--sink.type=postgres", "--sink.postgres.table=" + m + "-x"}, nil, "sink.postgres.table"},
		{"postgres addr", []string{"--sink.type=postgres", "--sink.postgres.addr=" + m}, nil, "sink.postgres.addr"},
		{"postgres socket", []string{"--sink.type=postgres", "--sink.postgres.addr=/run/" + m + ",/x"}, nil, "sink.postgres.addr"},
		{"postgres socket port", []string{"--sink.type=postgres", "--sink.postgres.addr=/run/" + m + ":0"}, nil, "sink.postgres.addr"},
		{"postgres tls socket", []string{"--sink.type=postgres", "--sink.postgres.tls", "--sink.postgres.addr=/run/" + m}, nil, "sink.postgres.tls"},
		{"mysql database", []string{"--sink.type=mysql", "--sink.mysql.database=" + m + "`x"}, nil, "sink.mysql.database"},
		{"mysql database control", []string{"--config", "INI:[sink]\ntype = mysql\n[sink.mysql]\ndatabase = \"" + m + "\tx\"\n"}, nil, "sink.mysql.database"},
		{"mysql table", []string{"--sink.type=mysql", "--sink.mysql.table=" + m + "-x"}, nil, "sink.mysql.table"},
		{"clickhouse addrs", []string{"--sink.type=clickhouse", "--sink.clickhouse.addrs=" + m}, nil, "sink.clickhouse.addrs"},
		{"clickhouse database", []string{"--sink.type=clickhouse", "--sink.clickhouse.database=" + m + "\\x"}, nil, "sink.clickhouse.database"},
		{"clickhouse table", []string{"--sink.type=clickhouse", "--sink.clickhouse.table=" + m + "-x"}, nil, "sink.clickhouse.table"},
		{"nats stream", []string{"--sink.type=nats", "--sink.nats.stream=" + m + ".x"}, nil, "sink.nats.stream"},
		{"nats subject", []string{"--sink.type=nats", "--sink.nats.subject=a." + m + "*"}, nil, "sink.nats.subject"},
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
