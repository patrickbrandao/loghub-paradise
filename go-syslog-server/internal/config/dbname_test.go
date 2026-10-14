package config

import (
	"strings"
	"testing"
)

// Pendência 12 de 2026-10-11 (SPEC §10.6, §10.10.5, §13.2): both sinks put
// the database name raw between backticks, so a backtick, a backslash or a
// control byte would change the statement, and a name above 64 bytes is
// refused by MySQL only after the boot wait. Ported from go-relplog-server.
func TestDatabaseNamesFitBetweenBackticks(t *testing.T) {
	long := strings.Repeat("d", 64)
	for _, sec := range []string{"mysql", "clickhouse"} {
		base := []string{"--sink.type=" + sec}
		key := "--sink." + sec + ".database="
		// Controls: the default, a name at the ceiling, and names that are
		// odd but quote fine.
		// 64 CHARACTERS, not bytes: 64 'ç' are 128 bytes, a name MySQL
		// accepts (independent review of the port, 2026-10-13).
		for _, ok := range []string{"syslog", long, "logs-2026", "Dados.Brutos", "a b",
			strings.Repeat("ç", 64), strings.Repeat("日", 25)} {
			mustLoad(t, append(base, key+ok), nil)
		}
		for _, tc := range []struct{ name, want string }{
			{"sys`log", "backtick"},
			{"sys`; DROP DATABASE x; --", "backtick"},
			{`sys\log`, "backslash"},
			{"sys\nlog", "control byte"},
			{"sys\tlog", "control byte"},
			{"sys\x00log", "control byte"},
			{"sys\x7flog", "control byte"},
			{long + "d", "above 64"},
			{strings.Repeat("ç", 65), "65 characters is above 64"},
		} {
			mustFail(t, append(base, key+tc.name), nil, "sink."+sec+".database")
			mustFail(t, append(base, key+tc.name), nil, tc.want)
		}
		// The rule bites only when that database is the sink.
		mustLoad(t, []string{"--sink.type=sqlite", key + "a`b"}, nil)
	}
}
