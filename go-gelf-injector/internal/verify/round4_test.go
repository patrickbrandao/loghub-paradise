package verify

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// Review of 2026-11-07: every element of a NATS list is parsed before the
// driver sees it, no error quotes a password, and the scrub covers every
// element (it covered the first URL only).
func TestURLListNeverEchoed(t *testing.T) {
	for _, raw := range []string{
		"nats://u:s3 cret@h:1",
		"nats://h:1,nats://u:s3 cret@h2:1",
		"nats://h:1, tls://u:s3cret@[h2",
		"nats://h:1,http://u:s3cret@h2",
		"nats://h:1,nats://u:s3cret@",
		"postgres://u:s3 cret@h/db",
		"redis://u:s3cret@[bad",
	} {
		if _, err := Parse(raw, "default", ""); err == nil || strings.Contains(err.Error(), "s3") || strings.Contains(err.Error(), "cret") {
			t.Errorf("%s: %v", raw, err)
		}
	}
	s, err := Parse("nats://tok3n@h:1, nats://u:s3cret@h2:2", "default", "")
	if err != nil {
		t.Fatal(err)
	}
	if s.raw != "nats://tok3n@h:1,nats://u:s3cret@h2:2" || strings.Contains(s.Display, "s3cret") || strings.Contains(s.Display, "tok3n") || !strings.Contains(s.Display, "(+1)") {
		t.Fatalf("%s | %s", s.raw, s.Display)
	}
	if e := s.Err(errors.New("connect nats://u:s3cret@h2:2 with tok3n: refused")).Error(); strings.Contains(e, "s3cret") || strings.Contains(e, "tok3n") {
		t.Fatal(e)
	}
	_, perr := url.Parse("nats://u:s3 cret@h")
	if e := s.Err(fmt.Errorf("nats: %w", perr)).Error(); strings.Contains(e, "cret") {
		t.Fatal(e)
	}
	if !errors.Is(s.Err(ErrRunPresent), ErrRunPresent) {
		t.Fatal("ErrRunPresent lost its identity")
	}
}
