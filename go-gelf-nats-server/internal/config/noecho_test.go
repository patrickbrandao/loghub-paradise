package config

import (
	"strings"
	"testing"
)

// SPEC §16.2: configuration errors name the key and what is wrong, never the
// value -- a password pasted under the wrong key, or split by the shell onto
// the next one, would otherwise reach the stderr of the boot and, on a SIGHUP,
// the log (independent review of 2026-11-07, ported from the parent server:
// "invalid integer %q", the enum check and their kin quoted it). Each case
// puts a marker where a typed or checked value goes; the refusal must name
// the key (so it is the refusal of THAT value) and must not carry the marker.
func TestConfigErrorsNeverEchoTheValue(t *testing.T) {
	const m = "Zq9SecretMark"
	cases := []struct {
		args []string
		key  string
	}{
		{[]string{"--sink.workers=" + m}, "sink.workers"},
		{[]string{"--sink.op_timeout=" + m}, "sink.op_timeout"},
		{[]string{"--ring.max_bytes=" + m}, "ring.max_bytes"},
		{[]string{"--ring.max_bytes=" + m + "MiB"}, "ring.max_bytes"},
		{[]string{"--ring.high_water=" + m}, "ring.high_water"},
		{[]string{"--proxy.trusted=" + m}, "proxy.trusted"},
		{[]string{"--admin.bind=" + m}, "admin.bind"},
		{[]string{"--listen.tcp=" + m}, "listen.tcp"},
		{[]string{"--log.level=" + m}, "log.level"},
	}
	for _, p := range Registry(new(Config)) {
		switch p.Name() {
		case "sink.postgres.addr":
			cases = append(cases, struct {
				args []string
				key  string
			}{[]string{"--sink.postgres.addr=" + m}, "sink.postgres.addr"})
		case "sink.nats.stream":
			cases = append(cases, struct {
				args []string
				key  string
			}{[]string{"--sink.nats.stream=" + m + ".x"}, "sink.nats.stream"})
		}
	}
	for _, tc := range cases {
		err := mustFail(t, tc.args, nil, tc.key)
		if strings.Contains(err.Error(), m) {
			t.Errorf("%v: the error echoes the value: %v", tc.args, err)
		}
	}
}
