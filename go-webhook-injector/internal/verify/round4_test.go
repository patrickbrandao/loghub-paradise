package verify

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A malformed URL, or a list whose second element is malformed, is refused
// before any driver sees it, and no message quotes a password (review of
// 2026-11-07: nats.go's parse error quoted the URL whole, and the second
// element of a list was never parsed by the injector).
func TestMalformedURLNeverEchoed(t *testing.T) {
	for _, u := range []string{
		"nats://u:s3 cret@n1:4222",
		"nats://n1:4222,nats://u:s3 cret@n2:4222",
		"nats://n1:4222, nats://u:s3cret@[n2",
		"nats://n1:4222,http://u:s3cret@n2",
		"tls://n1:4222,nats://u:s3cret@",
		"postgres://u:s3 cret@db/x",
		"redis://u:s3cret@[bad",
		"valkeys://:s3cret@[bad",
	} {
		_, kerr := Kind(u)
		_, oerr := Open(context.Background(), Config{URL: u, Timeout: time.Second}, "r")
		for _, err := range []error{kerr, oerr} {
			if err == nil || strings.Contains(err.Error(), "s3") || strings.Contains(err.Error(), "cret") {
				t.Errorf("%s: %v", u, err)
			}
		}
	}
	tg, err := parseTarget("nats://tok3n@n1:4222,nats://u:s3cret@n2:4222")
	if err != nil {
		t.Fatal(err)
	}
	if tg.raw != "nats://tok3n@n1:4222,nats://u:s3cret@n2:4222" {
		t.Fatal(tg.raw)
	}
	// Every element's password is scrubbed from what a driver says, and a
	// *url.Error never passes.
	e := tg.wrap("x", errors.New("dial nats://u:s3cret@n2:4222 and tok3n failed"))
	if strings.Contains(e.Error(), "s3cret") || strings.Contains(e.Error(), "tok3n") {
		t.Fatal(e)
	}
	_, perr := url.Parse("nats://u:s3 cret@n")
	if e := tg.wrap("x", fmt.Errorf("nats: %w", perr)); strings.Contains(e.Error(), "cret") {
		t.Fatal(e)
	}
}

// failingReader fails its first fails reads, then serves rounds.
type failingReader struct {
	fakeReader
	fails int
}

func (f *failingReader) read(ctx context.Context, fn func(record)) error {
	if f.fails > 0 {
		f.fails--
		return errors.New("read failed: nats://u:s3cret@h")
	}
	return f.fakeReader.read(ctx, fn)
}

// A read that fails is tried again up to the timeout (SPEC §8.6.4): two
// failures then a good read pass; failures to the end leave the error, with
// no password, and make the check impossible.
func TestReadErrorRetried(t *testing.T) {
	r := &failingReader{fakeReader: fakeReader{rounds: [][]record{{rec("a", "u1", 1)}}}, fails: 2}
	v := newV(r)
	v.Accept(1, 200, "u1")
	if res := v.Check(context.Background(), 5*time.Second); !res.Complete || res.Error != "" || res.Found != 1 {
		t.Fatalf("%+v", res)
	}
	const many = 1 << 30
	r = &failingReader{fakeReader: fakeReader{rounds: [][]record{{}}}, fails: many}
	v = newV(r)
	v.t, _ = parseTarget("nats://u:s3cret@h")
	v.Accept(1, 200, "u1")
	res := v.Check(context.Background(), 600*time.Millisecond)
	if res.Error == "" || !res.Impossible() || strings.Contains(res.Error, "s3cret") || many-r.fails < 2 {
		t.Fatalf("%+v after %d failures", res, many-r.fails)
	}
}

// A destination where nothing ever appears (a Redis stream never created,
// an empty table) is every accepted request lost: exit 1, not 4 (SPEC
// §8.6.4, the same in the four injectors).
func TestNothingEverAppears(t *testing.T) {
	v := newV(&fakeReader{rounds: [][]record{{}}})
	v.Accept(1, 200, "u1")
	v.Accept(2, 200, "u2")
	res := v.Check(context.Background(), 300*time.Millisecond)
	if res.Lost != 2 || !res.Failed() || res.Impossible() || res.Complete {
		t.Fatalf("%+v", res)
	}
}
