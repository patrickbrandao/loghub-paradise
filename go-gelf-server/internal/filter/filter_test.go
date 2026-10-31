package filter

import (
	"sync"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-server/internal/convert"
)

func mustCompile(t *testing.T, min string, levels []string) *Rule {
	t.Helper()
	r, err := Compile(min, levels)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A lower level is more severe: min_level=4 keeps 0..4 (SPEC §7).
func TestLevelDirection(t *testing.T) {
	r := mustCompile(t, "warning", nil)
	for l := 0; l <= 7; l++ {
		if got, want := r.Allow(l), l <= 4; got != want {
			t.Errorf("level %d: allow=%v want %v", l, got, want)
		}
	}
	if !r.Active() {
		t.Error("min_level=4 must be active")
	}
}

func TestDefaultKeepsEverythingAndIsInactive(t *testing.T) {
	r := mustCompile(t, "7", nil)
	for l := 0; l <= 7; l++ {
		if !r.Allow(l) {
			t.Errorf("level %d refused by the default", l)
		}
	}
	if r.Active() {
		t.Error("the default rule must not make the receiver scan levels")
	}
	if r.Allow(-1) || r.Allow(8) {
		t.Error("out-of-range level accepted")
	}
	// Every level listed is the same as none listed.
	all := mustCompile(t, "7", []string{"0", "1", "2", "3", "4", "5", "6", "7"})
	if all.Active() {
		t.Error("a list of all eight levels is not a restriction")
	}
}

func TestLevelList(t *testing.T) {
	r := mustCompile(t, "debug", []string{"err", " 6 ", "", "EMERG"})
	want := map[int]bool{0: true, 3: true, 6: true}
	for l := 0; l <= 7; l++ {
		if r.Allow(l) != want[l] {
			t.Errorf("level %d: allow=%v", l, r.Allow(l))
		}
	}
	if !r.Active() || len(r.Levels()) != 3 {
		t.Errorf("active=%v levels=%v", r.Active(), r.Levels())
	}
	// Both rules apply: listed AND not above min.
	r = mustCompile(t, "3", []string{"err", "info"})
	if !r.Allow(3) || r.Allow(6) {
		t.Error("min_level and levels must both hold")
	}
}

func TestEveryPayloadNameIsAccepted(t *testing.T) {
	for i, n := range convert.LevelNames {
		if v, ok := ParseLevel(n); !ok || v != i {
			t.Errorf("%s: %d %v", n, v, ok)
		}
	}
}

func TestCompileRejectsUnknown(t *testing.T) {
	for _, bad := range []string{"warn", "error", "info2", "8", "-1", ""} {
		if _, err := Compile(bad, nil); err == nil {
			t.Errorf("min_level %q accepted", bad)
		}
	}
	if _, err := Compile("7", []string{"err", "fatal"}); err == nil {
		t.Error("levels with an unknown name accepted")
	}
}

func TestReplaceIsAtomicSwap(t *testing.T) {
	f := New(mustCompile(t, "7", nil))
	if !f.AllowLevel(7) || f.Active() {
		t.Fatal("initial")
	}
	f.Replace(mustCompile(t, "3", nil))
	if f.AllowLevel(7) || !f.Active() {
		t.Fatal("replace did not take")
	}
}

func TestConcurrentReplace(t *testing.T) {
	f := New(mustCompile(t, "7", nil))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				f.AllowLevel(j % 8)
			}
		}()
	}
	for j := 0; j < 200; j++ {
		f.Replace(mustCompile(t, []string{"3", "7"}[j%2], nil))
	}
	wg.Wait()
}

func TestNilRulePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	New(nil)
}
