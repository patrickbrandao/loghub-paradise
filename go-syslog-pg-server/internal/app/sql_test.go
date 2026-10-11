package app

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// The queue end to end (SPEC §3.7.1, §10.5): what a consumer of loghub.inbox
// rebuilds from each row is a valid type=syslog envelope from this instance,
// semantic rules included.

// sqlLines is every shape of payload the queue must hold: all fields present,
// all absent (NILVALUE), absent procid and msgid (RFC 3164 without PID), a NUL
// in the message, a NUL in header fields, invalid UTF-8, and a malformed line.
// jsonb refuses U+0000 (22P05), so the NUL lines come back with raw in base64
// and U+FFFD in every other string (SPEC §3.7.1).
var sqlLines = []string{
	"<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] sql-full \x00 com nul",
	"<13>1 2026-09-12T14:33:02Z - - - - - sql-nil",
	"<34>Oct 11 22:14:15 mymachine su: sql-nopid",
	"<13>Oct 11 22:14:15 ho\x00st ap\x00p[1\x002]: sql-nul-columns",
	"<190>Oct  1 00:00:00 host app: sql-latin \xff octet",
	"GET / HTTP/1.1 sql-malformed",
}

func TestPostgresEndToEnd(t *testing.T) {
	in := start(t, t.TempDir())
	sendOctet(t, in.app.TransportAddr("tcp"), sqlLines...)
	waitFor(t, 20*time.Second, "every row in the queue", func() bool { return in.count() == len(sqlLines) })
	if got := in.metric(`syslog_config_info{ring_mode="memory",sink_type="postgres",transports="udp,tcp"}`); got != "1" {
		t.Errorf("syslog_config_info = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	rows := in.rows()
	if len(rows) != len(sqlLines) {
		t.Fatalf("%d rows, want %d", len(rows), len(sqlLines))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		e := checkRow(t, r)
		if r.From != testID.From || r.Space != testID.Space {
			t.Errorf("row %s: from %s space %s, want this instance's identity", r.UUID, r.From, r.Space)
		}
		// raw comes back exact: in base64 when the line has a NUL (the queue's
		// rebuild) or invalid UTF-8 (the converter's own rule), in text otherwise.
		raw := string(e.Payload.rawBytes())
		var line string
		for _, l := range sqlLines {
			if l == raw {
				line = l
			}
		}
		if line == "" {
			t.Errorf("raw %q is no line that was sent", raw)
			continue
		}
		if seen[line] {
			t.Errorf("%q stored twice", line)
		}
		seen[line] = true
		wantB64 := strings.ContainsRune(line, 0) || !utf8.ValidString(line)
		if (e.Payload.RawEnc == "base64") != wantB64 {
			t.Errorf("%q: raw-encoding %q", line, e.Payload.RawEnc)
		}
		if e.Payload.Size != len(line) {
			t.Errorf("%q: size %d, want the octets of the line", line, e.Payload.Size)
		}
		if strings.Contains(line, "\x00") && strings.ContainsRune(string(r.Payload), 0) {
			t.Errorf("%q: a NUL reached the jsonb column", line)
		}
		switch {
		case strings.Contains(line, "sql-nul-columns"):
			if show(e.Payload.hostname()) != "ho�st" || show(e.Payload.AppName) != "ap�p" ||
				show(e.Payload.ProcID) != "1�2" {
				t.Errorf("NUL header fields = %s %s %s, want U+FFFD for each NUL",
					show(e.Payload.hostname()), show(e.Payload.AppName), show(e.Payload.ProcID))
			}
		case strings.Contains(line, "sql-full"):
			if e.Payload.message() != "sql-full � com nul" {
				t.Errorf("message %q, want the NUL as U+FFFD", e.Payload.message())
			}
		case strings.Contains(line, "sql-malformed"):
			if e.Payload.Format != "malformed" {
				t.Errorf("format %q", e.Payload.Format)
			}
		}
	}
	if len(seen) != len(sqlLines) {
		t.Errorf("%d distinct lines, want %d", len(seen), len(sqlLines))
	}
}
