package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/emergency"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/ident"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-server/internal/item"
)

// fixture is the slice of go-relplog-nats-server's test fixture that its
// review tests (review_cut_test.go, review_rn_test.go, ported 2026-10-13)
// read: an identity and the arguments of an instance. Here the destination is
// the local sink=sqlite of baseArgs and the ring is memory, the shape those
// tests were written for.
type fixture struct{ id ident.Identity }

func newFixture(*testing.T) *fixture { return &fixture{id: testID} }

func (f *fixture) args(dir string, extra ...string) []string {
	return append(append(baseArgs(dir), "--ring.mode=memory"), extra...)
}

// dumpFiles lists the emergency dumps of the agent from in dir (this server
// writes the binary format of SPEC §12.1, emg-<agent>-*.bin).
func dumpFiles(t *testing.T, dir, from string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "emg-"+from+"-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// dumpOf writes items as an emergency dump of id under dataDir, as a previous
// life's shutdown would have, and returns its path.
func dumpOf(t *testing.T, id ident.Identity, dataDir string, items []*item.Item) string {
	t.Helper()
	p, err := emergency.NewStore(filepath.Join(dataDir, "emergency"), id.From).Write(items)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// withoutFlag drops every argument that starts with prefix.
func withoutFlag(args []string, prefix string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if !strings.HasPrefix(a, prefix) {
			out = append(out, a)
		}
	}
	return out
}
