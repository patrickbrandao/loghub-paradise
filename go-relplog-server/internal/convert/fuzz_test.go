package convert

import (
	"net/netip"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlhcheck"
)

var fuzzSeeds = []string{
	"<86>1 2024-03-15T14:22:05.123Z srv101.intranet.br sshd 12345 ID47 - Accepted publickey",
	"<14>1 2026-10-03T08:59:59.999999-03:00 fw01 app 42 - [origin ip=\"192.0.2.1\" ip=\"192.0.2.129\" software=\"fw\\\"x\\]\"][meta note=\"a\\b\"] sd",
	"<14>1 - - - - - -",
	"<14>1 2026-10-03T12:00:00Z host01 app - - [semFechamento x=\"1\" mensagem",
	"<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
	"<30>Sep 13 10:00:00 systemd[1]: Started Session 42.",
	"<30>2026-09-13T10:00:00.123456+00:00 web-01 systemd[1]: Started",
	"<189>123: *Mar  1 00:00:01.123: %SYS-5-CONFIG_I: Configured",
	"<14>myapp[123]: algo aconteceu",
	"<00>Oct 11 22:14:15 host01 app: prioridade invalida",
	"GET / HTTP/1.1",
	"Oct  3 08:15:00 srv101 sshd[999]: Server listening",
	"<13>Mar  1 2024 00:00:01: %ASA-6-302013: Built",
	"<165>1 2003-10-11T22:14:15.003Z mymachine evntslog - ID47 [exampleSDID@32473 iut=\"3\"] \xef\xbb\xbfevent",
	"<14>Oct  3 08:59:59 pabx01 app: Liga\xe7\xe3o encerrada",
	"<14>1 2026-10-03T12:00:00Z host\t01 a\xc2\xa0b - - - x",
	"<14>Feb 29 10:00:00 h a: leap",
	"<14>1 2026-10-03t12:00:00.1234567z h a p m [a@1 b=\"c\"][a@1 b=\"d\"] dup",
	"",
	"<",
	"<191>",
}

// FuzzConvert is the conversion's fuzz property (SPEC §18.3): for any line and
// any valid head, the converter neither panics nor produces an envelope that
// fails the JLH schema, the type=syslog payload schema or the semantic rules.
// The other knobs exercise the configurations that change the bytes.
func FuzzConvert(f *testing.F) {
	for i, s := range fuzzSeeds {
		f.Add([]byte(s), uint8(i), uint16(0), true, uint8(0))
	}
	f.Fuzz(func(t *testing.T, raw []byte, shape uint8, extra uint16, includeRaw bool, cs uint8) {
		cfg := DefaultConfig()
		cfg.IncludeRaw = includeRaw
		cfg.Charset = Charset(cs % 3)
		cfg.SDMaxElements = 4
		cfg.SDMaxParams = 4
		cfg.MaxMessageBytes = 512
		if shape&8 != 0 {
			loc, err := time.LoadLocation("America/Sao_Paulo")
			if err == nil {
				cfg.Location = loc
			}
		}
		cfg.Columns = true
		c := New(cfg)

		recv := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
		h := item.Head{
			UUID: jlh.NewUUID(recv), From: "01962a3f-07bc-70b4-ae05-c57e7541d8a8", Space: "default",
		}
		h.SetTime(recv)
		switch shape % 4 {
		case 0:
			h.Transport, h.Framing = item.TransportUDP, item.FramingDatagram
		case 1:
			h.Transport, h.Framing = item.TransportTCP, item.FramingNonTransparent
		case 2:
			h.Transport, h.Framing = item.TransportTLS, item.FramingOctetCounting
			h.TLS = &item.TLSInfo{Version: "TLS1.3", Cipher: "TLS_AES_128_GCM_SHA256"}
		case 3:
			h.Transport, h.Framing = item.TransportUnix, item.FramingDatagram
		}
		if h.Transport.Network() {
			h.SrcIP, h.SrcPort = netip.MustParseAddr("2001:db8::1"), 51514
			h.DstPort = 1514
			if shape&4 != 0 {
				h.DstIP = netip.MustParseAddr("10.0.0.10")
			}
			if shape&16 != 0 && h.Transport != item.TransportUDP {
				h.PeerIP, h.PeerPort = netip.MustParseAddr("127.0.0.1"), 40000
			}
		}
		if len(raw) > cfg.MaxMessageBytes {
			raw = raw[:cfg.MaxMessageBytes]
		}
		h.Size = len(raw) + int(extra%3)

		out, res := c.Append(nil, &h, raw)
		if err := jlhcheck.ValidateWith(out, jlhcheck.Options{SkipFFFDRule: true}); err != nil {
			t.Fatalf("%v\nraw=%q\n%s", err, raw, out)
		}
		for _, col := range []NullString{res.Hostname, res.AppName, res.ProcID, res.MsgID} {
			if col.Valid && !utf8.ValidString(col.S) {
				t.Fatalf("column is not UTF-8: %q", col.S)
			}
		}
		// Determinism (SPEC §3.5): the same head and line, the same bytes.
		again, _ := New(cfg).Append(nil, &h, raw)
		if string(again) != string(out) {
			t.Fatalf("not deterministic:\n%s\n%s", out, again)
		}
	})
}

// FuzzSD drives the STRUCTURED-DATA parser directly: no panic, the caps hold,
// and a success leaves every SD-ID and PARAM-NAME inside the SD-NAME grammar.
func FuzzSD(f *testing.F) {
	f.Add([]byte(`[a b="c"][d@1 e="f\"]" e="g"]`), uint8(2), uint8(2))
	f.Add([]byte(`[a b="c\`), uint8(1), uint8(1))
	f.Add([]byte(`[x][x y="1"][x y="2"] tail`), uint8(8), uint8(8))
	f.Fuzz(func(t *testing.T, b []byte, me, mp uint8) {
		cfg := DefaultConfig()
		cfg.SDMaxElements = int(me % 16)
		cfg.SDMaxParams = int(mp % 16)
		c := New(cfg)
		if len(b) == 0 || b[0] != '[' {
			return
		}
		var warn, local Warnings
		end, ok := c.parseSD(b, 0, &warn, &local)
		if end < 0 || end > len(b) {
			t.Fatalf("end %d out of range", end)
		}
		if !ok {
			return
		}
		if cfg.SDMaxElements > 0 && len(c.sd.ids) > cfg.SDMaxElements {
			t.Fatalf("%d elements over the cap", len(c.sd.ids))
		}
		for e, n := range c.sd.perElem {
			if cfg.SDMaxParams > 0 && n > cfg.SDMaxParams {
				t.Fatalf("element %d has %d params over the cap", e, n)
			}
		}
		for _, id := range c.sd.ids {
			if _, _, ok := sdName(id, 0); !ok {
				t.Fatalf("bad SD-ID %q", id)
			}
		}
		out := c.appendSD(nil)
		if !utf8.Valid(out) {
			t.Fatal("invalid UTF-8 output")
		}
	})
}
