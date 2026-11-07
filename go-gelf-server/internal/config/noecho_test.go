package config

import (
	"strings"
	"testing"
)

// SPEC §16.2: configuration errors name the key and what is wrong, never the
// value -- a password pasted under the wrong key, or split by the shell onto
// the next one, would otherwise reach the stderr of the boot and, on a SIGHUP,
// the log (independent review of 2026-11-07: validateDBName, the enum check,
// "invalid integer %q" and their kin quoted it). Each case puts a marker where
// a typed or checked value goes; the refusal must name the key (so it is the
// refusal of THAT value, not some other one) and must not carry the marker.
func TestConfigErrorsNeverEchoTheValue(t *testing.T) {
	const m = "Zq9SecretMark"
	for _, tc := range []struct {
		args []string
		key  string
	}{
		{[]string{"--sink.workers=" + m}, "sink.workers"},
		{[]string{"--sink.op_timeout=" + m}, "sink.op_timeout"},
		{[]string{"--ring.max_bytes=" + m}, "ring.max_bytes"},
		{[]string{"--ring.max_bytes=" + m + "MiB"}, "ring.max_bytes"},
		{[]string{"--ring.high_water=" + m}, "ring.high_water"},
		{[]string{"--ring.redis.tls=" + m}, "ring.redis.tls"},
		{[]string{"--sink.type=" + m}, "sink.type"},
		{[]string{"--ring.mode=" + m}, "ring.mode"},
		{[]string{"--proxy.trusted=" + m}, "proxy.trusted"},
		{[]string{"--proxy.trusted=" + m + "/8"}, "proxy.trusted"},
		{[]string{"--admin.bind=" + m}, "admin.bind"},
		{[]string{"--sink.type=mysql", "--sink.mysql.addr=db:3306", "--sink.mysql.username=u",
			"--sink.mysql.database=" + m + "`x"}, "sink.mysql.database"},
		{[]string{"--sink.type=mysql", "--sink.mysql.addr=db:3306", "--sink.mysql.username=u",
			"--sink.mysql.database=" + m + "\x01"}, "sink.mysql.database"},
		{[]string{"--sink.type=mysql", "--sink.mysql.addr=db:3306", "--sink.mysql.username=u",
			"--sink.mysql.database=d", "--sink.mysql.table=" + m + "-x"}, "sink.mysql.table"},
		{[]string{"--sink.type=postgres", "--sink.postgres.addr=" + m}, "sink.postgres.addr"},
		{[]string{"--sink.type=postgres", "--sink.postgres.addr=db:5432", "--sink.postgres.table=" + m},
			"sink.postgres.table"},
	} {
		err := mustFail(t, tc.args, nil, tc.key)
		if strings.Contains(err.Error(), m) {
			t.Errorf("%v: the error echoes the value: %v", tc.args, err)
		}
	}
	// The same through the environment, whose wrapper differs but whose
	// message comes from the same parser.
	err := mustFail(t, nil, map[string]string{envName(t, "sink.workers"): m}, "sink.workers")
	if strings.Contains(err.Error(), m) {
		t.Errorf("env: the error echoes the value: %v", err)
	}
}

// envName is the environment name of a parameter, whatever this server's
// prefix.
func envName(t *testing.T, name string) string {
	t.Helper()
	for _, p := range Registry(new(Config)) {
		if p.Name() == name {
			return p.Env()
		}
	}
	t.Fatalf("no parameter %s", name)
	return ""
}
