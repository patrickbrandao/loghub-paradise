package sink

import (
	"context"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/redis/rueidis"
)

// initSlot is rueidis' cmds.InitSlot: the slot of a keyless command built by
// a cluster client's builder.
const initSlot = uint16(1 << 14)

// clusterBuilder returns the builder of a rueidis cluster client, without a
// cluster. rueidis.Builder is a struct holding one uint16, the slot a keyless
// command gets (cmds.NewBuilder(cmds.InitSlot) in cluster.go); the test sets
// it, and checks that the result builds commands with the slots a cluster
// client gives them -- a rueidis upgrade that changes the layout fails here,
// not silently.
func clusterBuilder(t *testing.T) rueidis.Builder {
	t.Helper()
	var b rueidis.Builder
	if unsafe.Sizeof(b) != unsafe.Sizeof(uint16(0)) {
		t.Fatalf("rueidis.Builder is %d bytes: its layout changed", unsafe.Sizeof(b))
	}
	*(*uint16)(unsafe.Pointer(&b)) = initSlot
	info := b.Info().Build()
	xlen := b.Xlen().Key("k").Build()
	if info.Slot() != initSlot || xlen.Slot() == initSlot {
		t.Fatalf("the forged builder does not behave as a cluster one (slots %d, %d)", info.Slot(), xlen.Slot())
	}
	return b
}

// fakeCluster is a rueidis.Client in cluster mode that routes like the
// cluster client (rueidis cluster.go _pickMulti, v1.0.77): a DoMulti with a
// keyless command and keyed commands of DIFFERENT slots panics there; with
// keyed commands of ONE slot the keyless one goes to that slot's primary; a
// keyless command alone (Do, or a DoMulti of keyless commands only) goes to
// an arbitrary node. It records where each keyless command went instead of
// panicking, and answers every command with an empty reply.
type fakeCluster struct {
	rueidis.Client
	b rueidis.Builder

	mu        sync.Mutex
	onSlot    map[string][]uint16 // keyless command -> slots of the primaries it went to
	arbitrary []string            // keyless commands sent to an arbitrary node
	mixed     [][]string          // DoMultis that panic in rueidis
}

func newFakeCluster(t *testing.T) *fakeCluster {
	return &fakeCluster{b: clusterBuilder(t), onSlot: map[string][]uint16{}}
}

func (f *fakeCluster) B() rueidis.Builder       { return f.b }
func (f *fakeCluster) Mode() rueidis.ClientMode { return rueidis.ClientModeCluster }
func (f *fakeCluster) Close()                   {}

func cmdName(c rueidis.Completed) string {
	args := c.Commands()
	name := strings.ToUpper(args[0])
	if len(args) > 1 && (name == "INFO" || name == "CONFIG") {
		name += " " + strings.ToLower(args[1])
	}
	return name
}

func (f *fakeCluster) DoMulti(_ context.Context, cmds ...rueidis.Completed) []rueidis.RedisResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keyless []string
	slots := map[uint16]bool{}
	var slot uint16
	for _, c := range cmds {
		if s := c.Slot(); s == initSlot {
			keyless = append(keyless, cmdName(c))
		} else {
			slots[s], slot = true, s
		}
	}
	switch {
	case len(keyless) == 0:
	case len(slots) == 0:
		f.arbitrary = append(f.arbitrary, keyless...)
	case len(slots) > 1:
		names := make([]string, len(cmds))
		for i, c := range cmds {
			names[i] = cmdName(c)
		}
		f.mixed = append(f.mixed, names)
	default:
		for _, k := range keyless {
			f.onSlot[k] = append(f.onSlot[k], slot)
		}
	}
	out := make([]rueidis.RedisResult, len(cmds))
	for i := range out {
		out[i] = rueidis.NewResult(rueidis.RedisMessage{}, nil)
	}
	return out
}

func (f *fakeCluster) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	return f.DoMulti(ctx, cmd)[0]
}

// SPEC §8.2.5, §8.2.6 (independent review of the Valkey round, 2026-11-07):
// in cluster mode the policy (INFO memory, CONFIG GET in the pipeline and as
// the fallback) and the identity (INFO server) are read on the primary that
// holds the probe's stream: each keyless command rides a DoMulti whose keyed
// commands are all of the stream's slot. Sent with Do they went to an
// arbitrary node, replicas included; mixed with the probe's XADD (another
// slot) rueidis panics. No server: the fake applies rueidis' routing rule.
func TestRedisClusterProbeRoutesThePolicyToTheStreamPrimary(t *testing.T) {
	f := newFakeCluster(t)
	// No hash tag: the stream and its :healthprobe land in different slots,
	// the case where one DoMulti of the three commands panics.
	st := newTopic("loghub.cluster-test", "default")
	r := &Redis{client: f, log: quietLog(), m: testMetrics(), stream: st, probeKey: st.own + ":healthprobe"}
	own := f.b.Xlen().Key(r.stream.own).Build()
	probe := f.b.Xlen().Key(r.probeKey).Build()
	if own.Slot() == probe.Slot() {
		t.Fatalf("the stream and the probe key share slot %d: pick another stream name", own.Slot())
	}
	ctx := context.Background()

	_ = r.Health(ctx) // INFO memory in the pipeline; the empty reply sends it on to CONFIG GET
	r.policyRead.Store(policyByConfig)
	_ = r.Health(ctx) // CONFIG GET in the pipeline
	r.identify(ctx)   // INFO server

	if len(f.mixed) != 0 {
		t.Errorf("DoMultis that mix a keyless command with keyed commands of different slots "+
			"(rueidis panics): %v", f.mixed)
	}
	if len(f.arbitrary) != 0 {
		t.Errorf("keyless commands sent to an arbitrary node: %v", f.arbitrary)
	}
	for _, name := range []string{"INFO memory", "CONFIG get", "INFO server"} {
		got := f.onSlot[name]
		if len(got) == 0 {
			t.Errorf("%s was never sent to the stream's primary", name)
		}
		for _, s := range got {
			if s != own.Slot() {
				t.Errorf("%s went to the primary of slot %d, want the stream's %d", name, s, own.Slot())
			}
		}
	}
	if n := len(f.onSlot["CONFIG get"]); n != 2 {
		t.Errorf("CONFIG GET reached the stream's primary %d times, want 2 (fallback and pipeline)", n)
	}
}
