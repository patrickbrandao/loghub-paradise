package tmpl

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestExpand(t *testing.T) {
	tp, err := Compile("a{seq}-{run}-{host}-{pid} {unix} {unixms} {ts} {uuid} {rand:3} {{x}} }")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 12, 0, 0, 123456789, time.UTC)
	got := string(tp.Expand(nil, &Ctx{Seq: 42, Run: "r", Host: "h", PID: 7, Now: now}))
	re := regexp.MustCompile(`^a42-r-h-7 1793361600 1793361600123 2026-10-30T12:00:00\.123456Z [0-9a-f-]{36} [a-z0-9]{3} \{x\} \}$`)
	if !re.MatchString(got) {
		t.Fatalf("got %q", got)
	}
	if tp.Static() {
		t.Fatal("static")
	}
	if s, _ := Compile("plain {{ text"); !s.Static() || string(s.Expand(nil, &Ctx{})) != "plain { text" {
		t.Fatal("static template")
	}
}

func TestCompileErrors(t *testing.T) {
	for _, s := range []string{"{seq", "{nope}", "{rand:0}", "{rand:x}", "{rand:65537}", "{}"} {
		if _, err := Compile(s); err == nil {
			t.Errorf("%q compiled", s)
		}
	}
}

func TestUUIDv4(t *testing.T) {
	u := string(AppendUUID(nil))
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(u) {
		t.Fatalf("uuid %q", u)
	}
	if len(NewRunID()) != 8 || strings.Trim(NewRunID(), "0123456789abcdef") != "" {
		t.Fatal("run id")
	}
}
