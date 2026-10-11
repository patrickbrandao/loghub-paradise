package convert

import (
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/item"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlh"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/jlhcheck"
)

// convertUDP converts one line as a UDP datagram received at recv.
func convertUDP(t testing.TB, cfg Config, raw []byte, recv time.Time) ([]byte, map[string]any, Result) {
	t.Helper()
	h := item.Head{
		UUID: jlh.NewUUID(recv), From: vectorFrom, Space: vectorSpace,
		Transport: item.TransportUDP, Framing: item.FramingDatagram,
		SrcIP: netip.MustParseAddr("10.0.0.3"), SrcPort: 59922,
		DstIP: netip.MustParseAddr("10.0.0.10"), DstPort: 514, Size: len(raw),
	}
	h.SetTime(recv)
	out, res := New(cfg).Append(nil, &h, raw)
	if err := jlhcheck.Validate(out); err != nil {
		t.Fatalf("oracle: %v\n%s", err, out)
	}
	env := decode(t, out)
	return out, env["payload"].(map[string]any), res
}

// lookup walks a dotted path through the payload.
func lookup(p map[string]any, path string) (any, bool) {
	var cur any = p
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// TestCorpus runs the messages captured from real emitters (SPEC §18.4). The
// corpus never shrinks: three parser bugs of the standard server were found
// only by it, and no invented test would have caught them.
func TestCorpus(t *testing.T) {
	raws, err := filepath.Glob("testdata/corpus/*.raw")
	if err != nil {
		t.Fatal(err)
	}
	if len(raws) < 20 {
		t.Fatalf("corpus has %d entries; it never shrinks below 20", len(raws))
	}
	recv := time.Date(2026, 9, 13, 10, 0, 5, 0, time.UTC)
	for _, path := range raws {
		name := strings.TrimSuffix(filepath.Base(path), ".raw")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			wb, err := os.ReadFile(strings.TrimSuffix(path, ".raw") + ".want.json")
			if err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			dec := json.NewDecoder(strings.NewReader(string(wb)))
			dec.UseNumber()
			if err := dec.Decode(&want); err != nil {
				t.Fatal(err)
			}
			_, p, _ := convertUDP(t, DefaultConfig(), raw, recv)
			for k, w := range want {
				if k == "_absent" {
					for _, a := range w.([]any) {
						if v, ok := lookup(p, a.(string)); ok {
							t.Errorf("%s should be absent, got %v", a, v)
						}
					}
					continue
				}
				got, ok := lookup(p, k)
				if !ok {
					t.Errorf("%s missing", k)
					continue
				}
				if !reflect.DeepEqual(got, w) {
					t.Errorf("%s: got %#v want %#v", k, got, w)
				}
			}
		})
	}
}
