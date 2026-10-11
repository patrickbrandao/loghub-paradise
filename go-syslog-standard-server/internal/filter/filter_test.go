package filter

import "testing"

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

func TestDefaultKeepsEverything(t *testing.T) {
	r := mustCompile(t, "7", nil)
	for f := 0; f < 24; f++ {
		for s := 0; s < 8; s++ {
			if !r.Allow(f, s) {
				t.Fatalf("default rule dropped facility %d severity %d", f, s)
			}
		}
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

func TestCompileRejectsUnknown(t *testing.T) {
	if _, err := Compile("loud", nil); err == nil {
		t.Error("unknown severity accepted")
	}
	if _, err := Compile("8", nil); err == nil {
		t.Error("severity 8 accepted")
	}
	if _, err := Compile("info", []string{"nope"}); err == nil {
		t.Error("unknown facility accepted")
	}
	if _, err := Compile("info", []string{"24"}); err == nil {
		t.Error("facility 24 accepted")
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
	if !f.Allow(16, 3) {
		t.Fatal("err dropped after reload")
	}
}
