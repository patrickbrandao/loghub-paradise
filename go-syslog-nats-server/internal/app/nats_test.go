package app

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/obs"
)

// The stream end to end (SPEC §3.8, §10.7, §11): what a LogHub consumer reads
// from loghub.<space>.syslog is a valid type=syslog envelope from this
// instance, byte for byte the JLH the worker wrote, with the headers of §3.8;
// the beacon goes to the control plane by core NATS and never into the data
// stream; core mode delivers to a plain subscriber.

// natsLines is every shape of payload the stream must carry: all fields
// present, all absent (NILVALUE), absent procid and msgid (RFC 3164 without
// PID), a NUL in the message, NULs in header fields, invalid UTF-8, and a
// malformed line. Unlike a jsonb column, NATS carries the JLH untouched: the
// NULs stay, escaped as \u0000.
var natsLines = []string{
	"<165>1 2026-09-12T14:33:01.5Z web-01 nginx 42 ID7 [a@1 k=\"v\\\"q\"] nats-full \x00 com nul",
	"<13>1 2026-09-12T14:33:02Z - - - - - nats-nil",
	"<34>Oct 11 22:14:15 mymachine su: nats-nopid",
	"<13>Oct 11 22:14:15 ho\x00st ap\x00p[1\x002]: nats-nul-header",
	"<190>Oct  1 00:00:00 host app: nats-latin \xff octet",
	"GET / HTTP/1.1 nats-malformed",
}

func TestNATSEndToEnd(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	sendOctet(t, in.app.TransportAddr("tcp"), natsLines...)
	waitFor(t, 20*time.Second, "every message in the stream", func() bool { return f.count() == len(natsLines) })
	if got := in.metric(`syslog_config_info{ring_mode="memory",sink_type="nats",transports="udp,tcp"}`); got != "1" {
		t.Errorf("syslog_config_info = %s", got)
	}
	if got := in.metric("syslog_events_written_total"); got != fmt.Sprint(len(natsLines)) {
		t.Errorf("syslog_events_written_total = %s, want %d", got, len(natsLines))
	}
	if got := in.metric("syslog_sink_nats_duplicates_total"); got != "0" {
		t.Errorf("syslog_sink_nats_duplicates_total = %s", got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}

	msgs := f.msgs()
	if len(msgs) != len(natsLines) {
		t.Fatalf("%d messages, want %d", len(msgs), len(natsLines))
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		e := checkMsg(t, m)
		if e.From != f.id.From || e.Space != f.space {
			t.Errorf("%s: from %s space %s, want this instance's identity", e.UUID, e.From, e.Space)
		}
		if m.subject != "loghub."+f.space+".syslog" {
			t.Errorf("%s: subject %q", e.UUID, m.subject)
		}
		// The JLH travels in Data alone, escaped: never a raw NUL byte, and
		// never a header carrying it (SPEC §3.8).
		if bytes.IndexByte(m.data, 0) >= 0 {
			t.Errorf("%s: a raw NUL in Data", e.UUID)
		}
		for k, v := range m.header {
			if len(strings.Join(v, "")) > 64 {
				t.Errorf("%s: header %s carries %d bytes; the JLH belongs in Data", e.UUID, k, len(strings.Join(v, "")))
			}
		}
		raw := string(e.Payload.rawBytes())
		var line string
		for _, l := range natsLines {
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
		// base64 only where the converter itself needs it: invalid UTF-8. A NUL
		// is valid UTF-8 and stays text.
		if wantB64 := !utf8.ValidString(line); (e.Payload.RawEnc == "base64") != wantB64 {
			t.Errorf("%q: raw-encoding %q", line, e.Payload.RawEnc)
		}
		if e.Payload.Size != len(line) {
			t.Errorf("%q: size %d, want the octets of the line", line, e.Payload.Size)
		}
		switch {
		case strings.Contains(line, "nats-nul-header"):
			if show(e.Payload.hostname()) != "ho\x00st" || show(e.Payload.AppName) != "ap\x00p" ||
				show(e.Payload.ProcID) != "1\x002" {
				t.Errorf("NUL header fields = %q %q %q, want them untouched",
					show(e.Payload.hostname()), show(e.Payload.AppName), show(e.Payload.ProcID))
			}
		case strings.Contains(line, "nats-full"):
			if e.Payload.message() != "nats-full \x00 com nul" {
				t.Errorf("message %q, want the NUL untouched", e.Payload.message())
			}
		case strings.Contains(line, "nats-malformed"):
			if e.Payload.Format != "malformed" {
				t.Errorf("format %q", e.Payload.Format)
			}
		}
	}
	if len(seen) != len(natsLines) {
		t.Errorf("%d distinct lines, want %d", len(seen), len(natsLines))
	}
}

// SPEC §11: the heartbeat beacon goes to <ctl_namespace>.beacon by core NATS
// on the first connection -- that is, at boot -- as a JLH of type beacon, level
// 9, sid the PID, from this instance, and then every loghub.beacon_interval.
// The data stream, which never covers the control plane, stores none of them.
func TestNATSBeacon(t *testing.T) {
	f := newFixture(t)
	sub := f.beacons("loghub._ctl")

	in := launch(t, f.id, f.args(t.TempDir(), "--loghub.beacon_interval=1"), nil)
	in.waitReady()

	b, ok := nextBeacon(t, sub, f.id.From, 15*time.Second)
	if !ok {
		t.Fatal("no beacon from this instance on loghub._ctl.beacon")
	}
	if b.Type != "beacon" || b.Level != 9 || b.Space != f.space || b.Step != 0 || b.SID != os.Getpid() {
		t.Errorf("beacon envelope = type %s level %d space %s step %d sid %d", b.Type, b.Level, b.Space, b.Step, b.SID)
	}
	p := b.Payload
	if p.Name != f.id.AgentName || p.Hostname != f.id.Hostname || p.Machine != f.id.MachineID ||
		net.ParseIP(p.Address) == nil || p.Uptime < 0 {
		t.Errorf("beacon payload = %+v", p)
	}
	meta := map[string]any{}
	for _, kv := range p.Meta {
		meta[kv.Key] = kv.Value
	}
	if meta["sink"] != "nats" || meta["ring"] != "memory" || fmt.Sprint(meta["transports"]) != "[udp tcp]" ||
		meta["version"] != "dev" {
		t.Errorf("beacon meta = %v", meta)
	}

	// The periodic beacon (interval 1 s) follows, with a later uptime.
	b2, ok := nextBeacon(t, sub, f.id.From, 10*time.Second)
	if !ok {
		t.Fatal("no periodic beacon")
	}
	if b2.UUID == b.UUID || b2.Payload.Uptime < b.Payload.Uptime {
		t.Errorf("the periodic beacon repeats the first: %s/%d after %s/%d", b2.UUID, b2.Payload.Uptime, b.UUID, b.Payload.Uptime)
	}
	if got := in.metric(`syslog_beacons_total{result="ok"}`); got == "0" {
		t.Errorf(`syslog_beacons_total{result="ok"} = %s`, got)
	}
	if got := in.metric(`syslog_beacons_total{result="error"}`); got != "0" {
		t.Errorf(`syslog_beacons_total{result="error"} = %s`, got)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
	if n := f.count(); n != 0 {
		t.Errorf("the data stream captured %d control-plane messages", n)
	}
}

// SPEC §10.7: sink.nats.mode=core publishes by core NATS, without a stream --
// none is validated, none is needed -- and a subscriber on
// loghub.<space>.syslog receives every message with the headers of §3.8 (and
// no Nats-Expected-Stream). The beacon goes out in core mode as well.
func TestNATSCoreMode(t *testing.T) {
	f := newCoreFixture(t)
	sub, err := f.nats.nc.SubscribeSync(subject(f.space))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	bsub := f.beacons("loghub._ctl")

	in := launch(t, f.id, f.args(t.TempDir(), "--sink.nats.stream=SLS_NO_SUCH_STREAM_"+unique()), nil)
	in.waitReady()
	if _, ok := nextBeacon(t, bsub, f.id.From, 10*time.Second); !ok {
		t.Error("no beacon in core mode")
	}
	const n = 20
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - core-%02d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	sendUDP(t, in.app.TransportAddr("udp"), "<13>Oct 11 22:14:15 host app: core-udp")

	got := map[string]bool{}
	for len(got) < n+1 {
		msg, err := sub.NextMsg(10 * time.Second)
		if err != nil {
			t.Fatalf("received %d of %d messages: %v", len(got), n+1, err)
		}
		m := natsMsg{subject: msg.Subject, header: msg.Header, data: msg.Data, env: mustJLH(t, msg.Data)}
		e := checkMsg(t, m)
		if h := msg.Header.Get("Nats-Expected-Stream"); h != "" {
			t.Errorf("core publish carries Nats-Expected-Stream %q", h)
		}
		if e.From != f.id.From {
			t.Errorf("from %s", e.From)
		}
		got[e.Payload.message()] = true
	}
	for i := range lines {
		if !got[fmt.Sprintf("core-%02d", i)] {
			t.Errorf("core-%02d is missing", i)
		}
	}
	if !got["core-udp"] {
		t.Error("the UDP message is missing")
	}
	if a := in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != n {
		t.Errorf("tcp accepted %d", a)
	}
	if code := in.stop(); code != ExitOK {
		t.Errorf("exit %d", code)
	}
}
