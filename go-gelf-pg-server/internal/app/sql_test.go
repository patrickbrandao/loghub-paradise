package app

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// The queue end to end (SPEC §3.7.1, §10.5): what a consumer of loghub.inbox
// rebuilds from each row is a valid type=gelf envelope from this instance,
// semantic rules included.

// sqlLines is every shape of payload the queue must hold: hostname present and
// absent, a NUL in the message and in the host (jsonb refuses U+0000, 22P05:
// they come back as U+FFFD, SPEC §3.7.1), a number past the uint64 that must
// keep its value, a document whose loss forces raw (a duplicate key), and two
// malformed documents, one with a raw NUL byte (raw in base64 in the queue's
// copy) and one with invalid UTF-8 (raw in base64 everywhere).
var sqlLines = []string{
	`{"version":"1.1","host":"web-01","short_message":"sql-full \u0000 com nul","level":5,` +
		`"_k":"v\"q","_n":12345678901234567890,"custom":{"a":[1,2]}}`,
	`{"short_message":"sql-nil"}`,
	`{"version":"1.1","host":"ho\u0000st","short_message":"sql-nul-columns","level":2}`,
	`{"version":"1.1","host":"h","short_message":"sql-dup","level":3,"level":4}`,
	"GET / HTTP/1.1 sql-malformed \x00 nul",
	"sql-malformed-bin \xff",
}

// sendSQLLines sends the lines over TCP, except a line with a NUL byte, which
// the \0 framing would cut in two (SPEC §5.3): that one goes as a datagram.
func sendSQLLines(t *testing.T, in *instance, lines []string) {
	t.Helper()
	var tcp []string
	for _, l := range lines {
		if strings.IndexByte(l, 0) >= 0 {
			sendUDP(t, in.app.TransportAddr("udp"), l)
			continue
		}
		tcp = append(tcp, l)
	}
	sendNUL(t, in.app.TransportAddr("tcp"), tcp...)
}

func TestPostgresEndToEnd(t *testing.T) {
	in := start(t, t.TempDir())
	sendSQLLines(t, in, sqlLines)
	waitFor(t, 20*time.Second, "every row in the queue", func() bool { return in.count() == len(sqlLines) })
	if got := in.metric(`gelf_config_info{ring_mode="memory",sink_type="postgres",transports="udp,tcp,http"}`); got != "1" {
		t.Errorf("gelf_config_info = %s", got)
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
		if strings.ContainsRune(string(r.Payload), 0) || strings.Contains(string(r.Payload), `\u0000`) {
			t.Errorf("row %s: a NUL reached the jsonb column", r.UUID)
		}
		if e.Payload.Raw == nil {
			if e.Payload.Format != "gelf" {
				t.Errorf("a %s row without raw: %s", e.Payload.Format, e.bytes)
			}
			// The NUL of the message became U+FFFD in the queue's copy.
			m := e.Payload.message()
			seen[m] = true
			switch m {
			case "sql-nul-columns":
				if show(e.Payload.hostname()) != "ho�st" {
					t.Errorf("NUL host = %s, want U+FFFD for the NUL", show(e.Payload.hostname()))
				}
			case "sql-full � com nul":
				// 12345678901234567890 is past the uint64 a float64 holds
				// exactly: jsonb keeps it whole (SPEC §3.3).
				if !strings.Contains(string(r.Payload), "12345678901234567890") {
					t.Errorf("the number lost digits: %s", r.Payload)
				}
			}
			continue
		}
		// raw comes back exact: in base64 when the line has a NUL (the queue's
		// rebuild) or invalid UTF-8 (the converter's own rule), in text
		// otherwise.
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
		seen[line] = true
		wantB64 := strings.ContainsRune(line, 0) || !utf8.ValidString(line)
		if (e.Payload.RawEnc == "base64") != wantB64 {
			t.Errorf("%q: raw-encoding %q", line, e.Payload.RawEnc)
		}
		if e.Payload.Size != len(line) {
			t.Errorf("%q: size %d, want the octets of the document", line, e.Payload.Size)
		}
	}
	for _, want := range []string{"sql-full � com nul", "sql-nil", "sql-nul-columns", sqlLines[3], sqlLines[4], sqlLines[5]} {
		if !seen[want] {
			t.Errorf("%q is missing from the queue (seen %v)", want, seen)
		}
	}
}
