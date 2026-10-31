package source

import (
	"encoding/json"
	mrand "math/rand/v2"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/tmpl"
)

func TestGeneratorAndPad(t *testing.T) {
	tp, err := tmpl.Compile(GenBody(`we"ird{seq}`, 3))
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 50, 300, 20000} {
		g := &Template{T: tp, Run: "r1", Size: size}
		m, _ := g.Next(42, time.Now())
		var d struct {
			Version      string
			Host         string
			ShortMessage string `json:"short_message"`
			Level        int
			Seq          int    `json:"_seq"`
			Run          string `json:"_run"`
			Pad          string `json:"_pad"`
		}
		if err := json.Unmarshal(m.Data, &d); err != nil {
			t.Fatalf("size %d: %v: %s", size, err, m.Data)
		}
		if d.Seq != 42 || d.Run != "r1" || d.Level != 3 || d.ShortMessage != "seq=42 run=r1 injector=go-gelf-injector" {
			t.Fatalf("%s", m.Data)
		}
		if size > len(m.Data) || size >= 300 && len(m.Data) != size {
			t.Fatalf("size %d: got %d bytes", size, len(m.Data))
		}
	}
	var h struct{ Host string }
	m, _ := (&Template{T: tp}).Next(1, time.Now())
	json.Unmarshal(m.Data, &h)
	if h.Host != `we"ird{seq}` {
		t.Fatalf("host %q", h.Host)
	}
}

func TestPadIsRandom(t *testing.T) {
	r := mrand.New(mrand.NewPCG(1, 2))
	a := PadJSON([]byte(`{"a":1}`), 1000, r)
	b := PadJSON([]byte(`{"a":1}`), 1000, r)
	if len(a) != 1000 || string(a) == string(b) {
		t.Fatal("padding not random or wrong size")
	}
}

// Every size from len+10 up is reached exactly; len+1..len+9 cannot be (the
// empty "_pad" alone is 10 bytes) and leave the document as it is
// (review-gi #5: len+10 was not padded).
func TestPadSizes(t *testing.T) {
	r := mrand.New(mrand.NewPCG(1, 2))
	m := []byte(`{"a":1}`)
	for size := 0; size <= len(m)+30; size++ {
		out := PadJSON(m, size, r)
		switch {
		case size < len(m)+10:
			if string(out) != string(m) {
				t.Fatalf("size %d: %s", size, out)
			}
		case len(out) != size || !json.Valid(out):
			t.Fatalf("size %d: %d bytes %s", size, len(out), out)
		}
	}
}
