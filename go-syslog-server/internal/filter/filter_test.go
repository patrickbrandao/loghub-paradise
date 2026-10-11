package filter

import (
	"strings"
	"sync"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/convert"
)

func mustCompile(t *testing.T, sev string, fac []string) *Rule {
	t.Helper()
	r, err := Compile(sev, fac)
	if err != nil {
		t.Fatalf("Compile(%q, %v): %v", sev, fac, err)
	}
	return r
}

// The direction of the severity comparison is the one thing here that is easy
// to get backwards and impossible to notice: reversed, it would keep exactly
// the noise the filter exists to cut.
func TestSeverityDirection(t *testing.T) {
	r := mustCompile(t, "warning", nil)
	for sev := 0; sev <= 4; sev++ {
		if !r.Allow(16, sev) {
			t.Errorf("severity %d dropped, want kept", sev)
		}
	}
	for sev := 5; sev <= 7; sev++ {
		if r.Allow(16, sev) {
			t.Errorf("severity %d kept, want dropped", sev)
		}
	}
}

// SPEC §16.5: min_severity defaults to 7 and facilities to empty, which must
// accept every one of the 192 priorities.
func TestDefaultKeepsEverything(t *testing.T) {
	r := mustCompile(t, "7", nil)
	for f := 0; f < 24; f++ {
		for s := 0; s < 8; s++ {
			if !r.Allow(f, s) {
				t.Fatalf("default rule dropped facility %d severity %d", f, s)
			}
		}
	}
	if r.Facilities() != nil {
		t.Fatalf("Facilities() = %v for an accept-all rule, want nil", r.Facilities())
	}
}

func TestFacilityAllowlist(t *testing.T) {
	r := mustCompile(t, "debug", []string{"local0", "auth", "23"})
	for _, f := range []int{16, 4, 23} {
		if !r.Allow(f, 6) {
			t.Errorf("facility %d dropped", f)
		}
	}
	for _, f := range []int{0, 1, 15, 17} {
		if r.Allow(f, 6) {
			t.Errorf("facility %d kept", f)
		}
	}
	// Out of range can only come from a bug upstream, and dropping is the safe
	// answer: an accepted event with a nonsense facility would land in the
	// destination as a real row.
	if r.Allow(-1, 0) || r.Allow(24, 0) {
		t.Error("out-of-range facility accepted")
	}
	if got := r.Facilities(); len(got) != 3 || got[0] != 4 || got[1] != 16 || got[2] != 23 {
		t.Errorf("Facilities() = %v", got)
	}
}

// SPEC §6.2: the names are RFC 5427's, the same table the converter writes
// into payload.facility.name. Every name the payload can carry must be a name
// the filter accepts, and map to the same code.
func TestEveryPayloadNameIsAccepted(t *testing.T) {
	for code, name := range convert.FacilityNames {
		v, ok := ParseFacility(name)
		if !ok || v != code {
			t.Errorf("facility %q -> %d %v, want %d", name, v, ok, code)
		}
	}
	for code, name := range convert.SeverityNames {
		v, ok := ParseSeverity(name)
		if !ok || v != code {
			t.Errorf("severity %q -> %d %v, want %d", name, v, ok, code)
		}
	}
	// The two names that changed against the standard server.
	if v, _ := ParseFacility("console"); v != 14 {
		t.Errorf("console = %d, want 14", v)
	}
	if v, _ := ParseFacility("cron2"); v != 15 {
		t.Errorf("cron2 = %d, want 15", v)
	}
}

// The standard server's "alert" (14) and "clock" (15) are refused, with a
// message that says what to write instead: accepting them would make the
// filter disagree with the name the destination shows.
func TestStandardFacilityNamesAreRefusedWithAHint(t *testing.T) {
	for in, want := range map[string]string{"alert": "console", "clock": "cron2"} {
		_, err := Compile("debug", []string{in})
		if err == nil {
			t.Fatalf("facility %q accepted", in)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error for %q does not point at %q: %v", in, want, err)
		}
	}
}

func TestNamesAndNumbersAgree(t *testing.T) {
	a := mustCompile(t, "warning", []string{"local0"})
	b := mustCompile(t, "4", []string{"16"})
	for f := 0; f < 24; f++ {
		for s := 0; s < 8; s++ {
			if a.Allow(f, s) != b.Allow(f, s) {
				t.Fatalf("name and number disagree at %d/%d", f, s)
			}
		}
	}
}

// An INI value is typed by a human: case and blanks around a name are not a
// reason to fail the boot.
func TestCaseAndBlanksAreTolerated(t *testing.T) {
	r := mustCompile(t, " Warning ", []string{" LOCAL0", "auth "})
	if !r.Allow(16, 4) || !r.Allow(4, 0) || r.Allow(16, 5) || r.Allow(1, 0) {
		t.Fatal("case or blanks changed the decision")
	}
}

// An empty item in the list (a trailing comma) is ignored, and a list made only
// of empty items means "every facility", like an empty list.
func TestEmptyItemsAreIgnored(t *testing.T) {
	r := mustCompile(t, "debug", []string{"local0", "", " "})
	if !r.Allow(16, 0) || r.Allow(17, 0) {
		t.Fatal("an empty item changed the allowlist")
	}
	r = mustCompile(t, "debug", []string{"", " "})
	if !r.Allow(0, 0) || !r.Allow(23, 7) {
		t.Fatal("a list of empty items must accept everything")
	}
}

func TestCompileRejectsUnknown(t *testing.T) {
	for _, sev := range []string{"loud", "8", "-1", "", "warn", "error"} {
		if _, err := Compile(sev, nil); err == nil {
			t.Errorf("severity %q accepted", sev)
		}
	}
	for _, fac := range []string{"nope", "24", "-1", "security"} {
		if _, err := Compile("info", []string{fac}); err == nil {
			t.Errorf("facility %q accepted", fac)
		}
	}
}

func TestAllowPRI(t *testing.T) {
	r := mustCompile(t, "err", []string{"local4"})
	// <163> is local4.err, <167> local4.debug, <11> user.err.
	if !r.AllowPRI(163) || r.AllowPRI(167) || r.AllowPRI(11) {
		t.Fatal("AllowPRI does not split facility and severity like the payload")
	}
}

func TestReplaceIsAtomicSwap(t *testing.T) {
	f := New(mustCompile(t, "debug", nil))
	if !f.Allow(16, 7) {
		t.Fatal("debug dropped before reload")
	}
	f.Replace(mustCompile(t, "err", nil))
	if f.Allow(16, 7) {
		t.Fatal("debug kept after reload")
	}
	if !f.Allow(16, 3) || !f.AllowPRI(16*8+3) {
		t.Fatal("err dropped after reload")
	}
	if f.Rule().MinSeverity() != 3 {
		t.Fatalf("live rule min severity = %d", f.Rule().MinSeverity())
	}
}

// Run with -race: readers never see a torn rule while SIGHUP swaps it.
func TestConcurrentReplace(t *testing.T) {
	f := New(mustCompile(t, "debug", nil))
	a, b := mustCompile(t, "err", nil), mustCompile(t, "debug", []string{"kern"})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			if i%2 == 0 {
				f.Replace(a)
			} else {
				f.Replace(b)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = f.Allow(i%24, i%8)
		}
	}()
	wg.Wait()
}

func TestNilRulePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(nil) did not panic")
		}
	}()
	New(nil)
}
