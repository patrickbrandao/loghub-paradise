package verify

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Findings of the injectors' review, 2026-11-07 (SPEC §8.5).

// TestParseURLLists (findings 3 and 12): every element of a NATS list is
// parsed before nats.go sees it, the injector's parameters may sit in any of
// them, no error quotes one, and the destination shows every host.
func TestParseURLLists(t *testing.T) {
	tg, err := ParseURL("nats://u:sekret@n1:4222, tls://t0ken@n2:4222?stream=S&space=prod")
	if err != nil {
		t.Fatal(err)
	}
	if tg.Display != "nats://n1:4222,n2:4222" || tg.url != "nats://u:sekret@n1:4222,tls://t0ken@n2:4222" || tg.stream != "S" || tg.space != "prod" || !tg.tls {
		t.Fatalf("%+v", tg)
	}
	for _, bad := range []string{
		"nats://u:sekret@n1:4222,nats://u:sekret@n2:4222/%zz",
		"nats://u:sekret@n1:4222,nats://u:se%kret@n2",
		"nats://u:sekret@n1:4222,",
		"nats://u:sekret@n1:4222,redis://u:sekret@n2",
		"nats://n1?stream=a,nats://n2?stream=b",
	} {
		if _, err := ParseURL(bad); err == nil || strings.Contains(err.Error(), "sek") {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// TestClean (finding 3): a *url.Error is never forwarded (a password encoded
// otherwise than QueryEscape would escape the scrub), and the secrets of the
// URL are scrubbed from any other error text.
func TestClean(t *testing.T) {
	tg, err := ParseURL("nats://alice:se%6Bret@n1:4222,nats://t0ken@n2:4222")
	if err != nil {
		t.Fatal(err)
	}
	_, perr := url.Parse("nats://alice:se%6Bret@n1:4222/%zz")
	for _, e := range []error{
		fmt.Errorf("wrapped: %w", perr),
		errors.New("dial nats://alice:sekret@n1:4222 and t0ken@n2 failed"),
	} {
		got := tg.clean("nats", e).Error()
		if strings.Contains(got, "6Bret") || strings.Contains(got, "sekret") || strings.Contains(got, "t0ken") || strings.Contains(got, "alice") {
			t.Errorf("%q", got)
		}
	}
}

// TestSetCA (finding 7): --verify-tls-ca needs a TLS scheme, a PEM file, and
// not the ca= parameter too.
func TestSetCA(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "x.pem")
	os.WriteFile(notPEM, []byte("not a certificate"), 0o600)
	for _, c := range []struct{ url, file string }{
		{"nats://h:4222", notPEM},
		{"postgres://h/db", notPEM},
		{"tls://h:4222?ca=" + notPEM, notPEM},
		{"rediss://h:6380", notPEM},
		{"valkeys://h:6380", filepath.Join(dir, "missing.pem")},
	} {
		tg, err := ParseURL(c.url)
		if err != nil {
			t.Fatal(err)
		}
		if err := tg.SetCA(c.file); err == nil {
			t.Errorf("%s accepted", c.url)
		}
	}
	if err := SQLite("x.db").SetCA(notPEM); err == nil {
		t.Error("sqlite accepted")
	}
	// A real PEM: accepted on a TLS scheme, refused next to ca=.
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true}, &x509.Certificate{SerialNumber: big.NewInt(1)}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(dir, "ca.pem")
	os.WriteFile(good, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	if tg, _ := ParseURL("rediss://h:6380"); tg.SetCA(good) != nil || tg.ca != good {
		t.Error("a PEM CA refused")
	}
	if tg, _ := ParseURL("tls://h:4222?ca=" + good); tg.SetCA(good) == nil {
		t.Error("--verify-tls-ca with ca= accepted")
	}
}

// fakeReader serves batch i at Read i (an incremental reader), or every batch
// up to i (a full one); fail[i] fails Read i after a partial batch.
type fakeReader struct {
	batches [][]uint64
	fail    map[int]bool
	full    bool
	reads   *int
}

func (f *fakeReader) Read(_ context.Context, fn func(Entry)) error {
	i := *f.reads
	*f.reads++
	var b []uint64
	if f.full {
		for _, x := range f.batches[:min(i+1, len(f.batches))] {
			b = append(b, x...)
		}
	} else if i < len(f.batches) {
		b = f.batches[i]
	}
	if f.fail[i] {
		if len(b) > 0 {
			fn(Entry{Message: fmt.Sprintf("seq=%d", b[0])})
		}
		return errors.New("read failed")
	}
	for _, s := range b {
		fn(Entry{Message: fmt.Sprintf("seq=%d", s), UUID: fmt.Sprint(i)})
	}
	return nil
}
func (f *fakeReader) Incremental() bool { return !f.full }
func (f *fakeReader) Close()            {}

func withReader(t *testing.T, f *fakeReader) {
	t.Helper()
	old := openReader
	openReader = func(context.Context, *Target, time.Time, string, bool) (reader, error) { return f, nil }
	t.Cleanup(func() { openReader = old })
}

func ackedAll(n uint64) []uint64 {
	a := make([]uint64, n/64+1)
	for s := uint64(1); s <= n; s++ {
		a[s/64] |= 1 << (s % 64)
	}
	return a
}

func runCheck(t *testing.T, f *fakeReader, sent uint64, timeout time.Duration) (expected, found, lost, dup uint64, complete, unreadable bool, reads int) {
	t.Helper()
	n := 0
	f.reads = &n
	withReader(t, f)
	m, err := NewMatcher(`seq=([0-9]+)`)
	if err != nil {
		t.Fatal(err)
	}
	v := Check(context.Background(), &Target{Display: "fake"}, m, sent, ackedAll(sent), Options{Timeout: timeout, Poll: 20 * time.Millisecond, Settle: 100 * time.Millisecond})
	return v.Expected, v.Found, v.Lost, v.Duplicated, v.Complete, v.Unreadable, n
}

// TestCheckExtraRead (finding 11): once every acknowledged seq is there, the
// reads of the settle second see the copy that came right behind; without
// them the duplicate is not counted.
func TestCheckExtraRead(t *testing.T) {
	_, found, _, dup, complete, _, _ := runCheck(t, &fakeReader{batches: [][]uint64{{1, 2, 3}, {}, {2}}}, 3, 5*time.Second)
	if found != 3 || dup != 1 || !complete {
		t.Fatalf("found %d dup %d complete %v", found, dup, complete)
	}
}

// TestCheckFreshTally (finding 5): a full read that fails midway does not
// replace the last whole count; it reset the count first and left a false
// "lost". The last read failing makes the check unreadable (exit 4).
func TestCheckFreshTally(t *testing.T) {
	fail := map[int]bool{}
	for i := 1; i < 1000; i++ {
		fail[i] = true
	}
	_, found, lost, _, complete, unreadable, reads := runCheck(t, &fakeReader{batches: [][]uint64{{1, 2}, {3}}, fail: fail, full: true}, 3, 500*time.Millisecond)
	if found != 2 || lost != 1 || complete || !unreadable || reads < 3 {
		t.Fatalf("found %d lost %d complete %v unreadable %v reads %d", found, lost, complete, unreadable, reads)
	}
	// Failures first, then a whole read: retried until it works, complete.
	_, found, _, _, complete, unreadable, _ = runCheck(t, &fakeReader{batches: [][]uint64{{1, 2, 3}}, fail: map[int]bool{0: true, 1: true}, full: true}, 3, 5*time.Second)
	if found != 3 || !complete || unreadable {
		t.Fatalf("found %d complete %v unreadable %v", found, complete, unreadable)
	}
}

// TestTallyCopiesAndRecords (finding 6): duplicated is the copies past the
// first, unexpected the records of seqs not acked -- not seqs.
func TestTallyCopiesAndRecords(t *testing.T) {
	m, _ := NewMatcher(`seq=([0-9]+)`)
	tl := newTally(3)
	for _, s := range []string{"seq=1", "seq=1", "seq=1", "seq=2", "seq=2", "seq=2", "seq=7", "seq=7"} {
		tl.add(m, Entry{Message: s})
	}
	v := tl.result([]uint64{1 << 1}, true) // only 1 acked
	if v.Expected != 1 || v.Found != 1 || v.Duplicated != 4 || v.Unexpected != 5 || v.MissingUnacked != 1 || !v.OK {
		t.Fatalf("%+v", v)
	}
}
