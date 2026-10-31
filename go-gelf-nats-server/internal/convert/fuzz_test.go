package convert

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/jlhcheck"
)

// FuzzConvert (SPEC §18.3): whatever the bytes, the envelope passes both
// schemas and every semantic rule of the oracle, the reception's LevelOf is
// the envelope's level, size is the document, and the bound of SPEC §3.9
// holds.
func FuzzConvert(f *testing.F) {
	docs, _ := filepath.Glob("testdata/corpus/*.gelf")
	for _, p := range docs {
		b, _ := os.ReadFile(p)
		f.Add(b, uint8(0))
	}
	for _, s := range []string{`{}`, `{"_a":{"b":[1,{"c":null}]}}`, `{"level":"7","timestamp":"1e3"}`,
		`{"x":{"a":1,"a":2}}`, `{"_e":"𐀀\udc00"}`, "{\"a\":\"\x00\"}", `{"timestamp":-0.0e-0}`} {
		f.Add([]byte(s), uint8(1))
	}
	f.Fuzz(func(t *testing.T, doc []byte, variant uint8) {
		if len(doc) == 0 {
			return // the receiver never hands over an empty document
		}
		h := udpHead()
		cfg := DefaultConfig()
		switch variant % 4 {
		case 1:
			h.Transport, h.Compression, h.Chunks = item.TransportUDP, item.CompressionGzip, 1+int(variant)%128
		case 2:
			h.Transport, h.Compression = item.TransportHTTPS, item.CompressionDeflate
			h.TLS = &item.TLSInfo{Version: "TLS1.2"}
			cfg.KeepRaw = KeepRawAlways
		case 3:
			h.Transport = item.TransportTCP
			h.PeerIP, h.PeerPort = h.DstIP, 1
		}
		c := New(cfg)
		out, res := c.Append(nil, &h, doc)
		if err := jlhcheck.Validate(out); err != nil {
			t.Fatalf("oracle: %v\ndoc %q\n%s", err, doc, out)
		}
		if lv := c.LevelOf(doc); lv != res.Level {
			t.Fatalf("LevelOf %d != envelope level %d for %q", lv, res.Level, doc)
		}
		if !bytes.Contains(out, []byte(`"size":`+strconv.Itoa(len(doc))+`,`)) &&
			!bytes.Contains(out, []byte(`"size":`+strconv.Itoa(len(doc))+`}`)) {
			t.Fatalf("size is not len(doc)=%d\n%s", len(doc), out)
		}
		if len(out) > WorstJLH(len(doc)) {
			t.Fatalf("envelope %d > worst %d", len(out), WorstJLH(len(doc)))
		}
	})
}
