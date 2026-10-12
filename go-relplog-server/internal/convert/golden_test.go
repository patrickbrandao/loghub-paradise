package convert

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/netip"
	"reflect"
	"strconv"
	"testing"
	"time"

	uuidv7 "github.com/patrickbrandao/go-loghub-uuidv7"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/jlhcheck"
)

// The from and space every golden vector was generated with.
const (
	vectorFrom  = "01962a3f-07bc-70b4-ae05-c57e7541d8a8"
	vectorSpace = "default"
)

type vector struct {
	ID          string            `json:"id"`
	Stream      bool              `json:"stream"`
	Input       *string           `json:"input"`
	InputBase64 *string           `json:"input-base64"`
	Config      map[string]any    `json:"config"`
	Reception   reception         `json:"reception"`
	Expected    []json.RawMessage `json:"expected"`
}

type reception struct {
	Transport  string            `json:"transport"`
	Framing    string            `json:"framing"`
	SrcIP      string            `json:"src-ip"`
	SrcPort    int               `json:"src-port"`
	DstIP      string            `json:"dst-ip"`
	DstPort    int               `json:"dst-port"`
	ReceivedAt string            `json:"received-at"`
	TLS        *item.TLSInfo     `json:"-"`
	RawTLS     map[string]string `json:"tls"`
}

func loadVectors(t testing.TB) []vector {
	t.Helper()
	b, err := jlhcheck.Files.ReadFile("loghub/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vectors []vector `json:"vectors"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Vectors
}

func vectorConfig(t testing.TB, v vector) Config {
	cfg := DefaultConfig()
	for k, val := range v.Config {
		switch k {
		case "rfc3164-timezone":
			loc, err := time.LoadLocation(val.(string))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Location = loc
		case "include-raw":
			cfg.IncludeRaw = val.(bool)
		case "legacy-charset":
			cs, ok := ParseCharset(val.(string))
			if !ok {
				t.Fatalf("charset %v", val)
			}
			cfg.Charset = cs
		case "max-message-bytes":
			cfg.MaxMessageBytes = int(val.(float64))
		default:
			t.Fatalf("%s: unknown config %q", v.ID, k)
		}
	}
	return cfg
}

type frame struct {
	msg     []byte
	framing item.Framing
	size    int
}

// deframe is the reference converter's per-frame deframer, kept in the test so
// the vectors do not depend on the receiver package. RELP has no framing to
// detect (deviation D1 does not apply, SPEC §1.3), and every stream vector is
// uniform, so the per-frame and the per-connection rules give the same frames.
func deframe(t testing.TB, s []byte, max int) []frame {
	var out []frame
	pos := 0
	for pos < len(s) {
		if isDigit(s[pos]) {
			sp := bytes.IndexByte(s[pos:], ' ')
			if sp < 0 || s[pos] == '0' || sp > 10 {
				t.Fatal("invalid MSG-LEN in a vector")
			}
			n, err := strconv.Atoi(string(s[pos : pos+sp]))
			if err != nil {
				t.Fatal(err)
			}
			start := pos + sp + 1
			end := start + n
			if end > len(s) {
				end = len(s)
			}
			m := s[start:end]
			if len(m) > max {
				m = m[:max]
			}
			out = append(out, frame{m, item.FramingOctetCounting, n})
			pos = start + n
			continue
		}
		end := len(s)
		if i := bytes.IndexAny(s[pos:], "\n\x00"); i >= 0 {
			end = pos + i
		}
		f := bytes.TrimRight(s[pos:end], "\r")
		pos = end + 1
		if len(f) > 0 {
			size := len(f)
			if len(f) > max {
				f = f[:max]
			}
			out = append(out, frame{f, item.FramingNonTransparent, size})
		}
	}
	return out
}

func mustAddr(t testing.TB, s string) netip.Addr {
	if s == "" {
		return netip.Addr{}
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return item.Canonical(a)
}

// runVector converts a vector the way the server would and returns the
// envelopes.
func runVector(t testing.TB, v vector) [][]byte {
	cfg := vectorConfig(t, v)
	var data []byte
	if v.InputBase64 != nil {
		var err error
		if data, err = base64.StdEncoding.DecodeString(*v.InputBase64); err != nil {
			t.Fatal(err)
		}
	} else {
		data = []byte(*v.Input)
	}
	recv, err := time.Parse(time.RFC3339Nano, v.Reception.ReceivedAt)
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := item.ParseTransport(v.Reception.Transport)
	if !ok {
		t.Fatalf("transport %q", v.Reception.Transport)
	}
	var frames []frame
	if v.Stream {
		frames = deframe(t, data, cfg.MaxMessageBytes)
	} else {
		fr, ok := item.ParseFraming(v.Reception.Framing)
		if !ok {
			t.Fatalf("framing %q", v.Reception.Framing)
		}
		frames = []frame{{data, fr, len(data)}}
	}
	var tlsInfo *item.TLSInfo
	if m := v.Reception.RawTLS; m != nil {
		tlsInfo = &item.TLSInfo{Version: m["version"], Cipher: m["cipher"],
			PeerSubject: m["peer-subject"], PeerFingerprint: m["peer-fingerprint"]}
	}
	c := New(cfg)
	var out [][]byte
	for _, f := range frames {
		raw := f.msg
		if c.cfg.MaxMessageBytes > 0 && len(raw) > c.cfg.MaxMessageBytes {
			raw = raw[:c.cfg.MaxMessageBytes]
		}
		h := item.Head{
			UUID: jlh.NewUUID(recv), From: vectorFrom, Space: vectorSpace,
			Transport: tr, Framing: f.framing, TLS: tlsInfo, Size: f.size,
		}
		h.SetTime(recv)
		if tr.Network() {
			h.SrcIP, h.SrcPort = mustAddr(t, v.Reception.SrcIP), uint16(v.Reception.SrcPort)
			h.DstIP, h.DstPort = mustAddr(t, v.Reception.DstIP), uint16(v.Reception.DstPort)
		}
		b, _ := c.Append(nil, &h, raw)
		out = append(out, b)
	}
	return out
}

func decode(t testing.TB, b []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	return m
}

func uuidInstant(t testing.TB, s string) time.Time {
	u, err := uuidv7.FromString(s)
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := u.TimestampWithLevel(uuidv7.Level3)
	if !ok {
		t.Fatalf("%s is not v7", s)
	}
	return ts
}

// TestGoldenVectors is the conformance criterion of SPEC §18.2: every vector of
// the skill produces envelopes equal to the expected ones, field by field, with
// the uuid compared by its instant (ms, µs and ns of level 3).
func TestGoldenVectors(t *testing.T) {
	vectors := loadVectors(t)
	ran := 0
	for _, v := range vectors {
		t.Run(v.ID, func(t *testing.T) {
			if v.Reception.Transport == "file" {
				t.Skip("transport=file is out of v1 (SPEC §1.2, §18.2)")
			}
			ran++
			got := runVector(t, v)
			if len(got) != len(v.Expected) {
				t.Fatalf("%d envelopes, want %d", len(got), len(v.Expected))
			}
			for i := range got {
				if err := jlhcheck.Validate(got[i]); err != nil {
					t.Errorf("[%d] oracle: %v\n%s", i, err, got[i])
				}
				g, e := decode(t, got[i]), decode(t, v.Expected[i])
				if !uuidInstant(t, g["uuid"].(string)).Equal(uuidInstant(t, e["uuid"].(string))) {
					t.Errorf("[%d] uuid instant differs", i)
				}
				delete(g, "uuid")
				delete(e, "uuid")
				for k := range e {
					if !reflect.DeepEqual(g[k], e[k]) {
						gj, _ := json.Marshal(g[k])
						ej, _ := json.Marshal(e[k])
						t.Errorf("[%d] %s:\n got  %s\n want %s", i, k, gj, ej)
					}
				}
				for k := range g {
					if _, ok := e[k]; !ok {
						t.Errorf("[%d] unexpected key %s", i, k)
					}
				}
			}
		})
	}
	if ran != 23 {
		t.Fatalf("ran %d vectors, want 23 of 24", ran)
	}
}

// The envelope keys come out in the canonical order of SPEC §3.1 and §3.2. The
// order has no meaning in JSON; the point is reproducibility (SPEC §3.3).
func TestGoldenKeyOrder(t *testing.T) {
	topWant := []string{"uuid", "from", "space", "type", "level", "sid", "step", "event", "payload"}
	payloadOrder := []string{"host", "transport", "framing", "tls", "format", "version", "priority",
		"facility", "severity", "timestamp", "appname", "procid", "msgid", "structured-data",
		"message", "size", "raw", "raw-encoding", "parse", "x-proxy"}
	rank := map[string]int{}
	for i, k := range payloadOrder {
		rank[k] = i
	}
	for _, v := range loadVectors(t) {
		if v.Reception.Transport == "file" {
			continue
		}
		for _, b := range runVector(t, v) {
			top, payload, err := jlhcheck.Keys(b)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(top, topWant) {
				t.Fatalf("%s: top keys %v", v.ID, top)
			}
			last := -1
			for _, k := range payload {
				r, ok := rank[k]
				if !ok || r <= last {
					t.Fatalf("%s: payload keys out of order: %v", v.ID, payload)
				}
				last = r
			}
		}
	}
}
