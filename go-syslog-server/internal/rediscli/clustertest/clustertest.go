// Package clustertest is a server-free stand-in for a rueidis Redis Cluster
// client, for tests only (nothing outside a _test.go file imports it). It
// locks the rule of rediscli.Pinned (SPEC §10.3.1): on a cluster, every
// keyless command (INFO, CONFIG GET) travels in a DoMulti whose keyed
// commands all share ONE slot -- the slot of the stream whose primary must
// answer. The live cluster test is opt-in, and a one-node cluster cannot tell
// one node from another, so without this nothing in the default run would
// notice a reading sent to an arbitrary node, or a pipeline that makes
// rueidis panic.
//
// The file is identical in go-syslog-server, go-relplog-server and
// go-gelf-server (the sibling rule).
package clustertest

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/redis/rueidis"
)

// initSlot and noSlot mirror rueidis/internal/cmds: the slot a cluster
// builder gives a keyless command, and the flag a standalone builder sets.
const (
	initSlot = uint16(1 << 14)
	noSlot   = uint16(1 << 15)
)

// clusterBuilder is a rueidis.Builder in cluster mode: keyed commands carry
// their slot, keyless ones initSlot. rueidis builds it only inside a cluster
// client that has reached a server, so here its one field is set by hand;
// the checks below fail loudly if rueidis ever changes that layout, rather
// than letting the tests pass on slots that mean nothing.
func clusterBuilder() rueidis.Builder {
	var b rueidis.Builder
	if unsafe.Sizeof(b) != unsafe.Sizeof(uint16(0)) {
		panic("clustertest: rueidis.Builder is no longer one uint16; update clusterBuilder")
	}
	*(*uint16)(unsafe.Pointer(&b)) = initSlot
	info := b.Info().Build()
	k1, k2 := b.Get().Key("{a}x").Build(), b.Get().Key("{a}y").Build()
	other := b.Get().Key("{b}x").Build()
	if info.Slot() != initSlot || k1.Slot()&(initSlot|noSlot) != 0 || k1.Slot() != k2.Slot() ||
		k1.Slot() == other.Slot() {
		panic("clustertest: the forged builder does not compute cluster slots; update clusterBuilder")
	}
	return b
}

// Cmd is one recorded command.
type Cmd struct {
	Args []string
	Slot uint16 // the key slot, or initSlot for a keyless command
}

// Keyless reports whether the command has no key.
func (c Cmd) Keyless() bool { return c.Slot == initSlot }

func (c Cmd) String() string { return strings.Join(c.Args, " ") }

// Client says it is a Redis Cluster and records every Do and DoMulti; no
// command is sent anywhere. Each reply is Reply(args) when Reply is set, else
// an empty message without error (a "" for ToString, so a policy read by INFO
// finds no policy line and goes on to the CONFIG GET fallback). Any method of
// rueidis.Client it does not override panics on the nil embedded interface.
type Client struct {
	rueidis.Client
	Reply func(args []string) rueidis.RedisResult

	b     rueidis.Builder
	mu    sync.Mutex
	calls [][]Cmd
}

// New returns an empty recorder.
func New() *Client { return &Client{b: clusterBuilder()} }

// Slot is the cluster slot of key.
func (c *Client) Slot(key string) uint16 {
	cmd := c.b.Get().Key(key).Build()
	return cmd.Slot()
}

func (c *Client) B() rueidis.Builder               { return c.b }
func (c *Client) Mode() rueidis.ClientMode         { return rueidis.ClientModeCluster }
func (c *Client) Close()                           {}
func (c *Client) Nodes() map[string]rueidis.Client { return map[string]rueidis.Client{"fake:6379": c} }

func (c *Client) Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	return c.DoMulti(ctx, cmd)[0]
}

func (c *Client) DoMulti(_ context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	call := make([]Cmd, len(multi))
	out := make([]rueidis.RedisResult, len(multi))
	for i := range multi {
		call[i] = Cmd{Args: append([]string(nil), multi[i].Commands()...), Slot: multi[i].Slot()}
		if c.Reply != nil {
			out[i] = c.Reply(call[i].Args)
		} else {
			out[i] = rueidis.NewResult(rueidis.RedisMessage{}, nil)
		}
	}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	return out
}

// Calls returns every recorded call, one slice per Do or DoMulti.
func (c *Client) Calls() [][]Cmd {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]Cmd(nil), c.calls...)
}

// Reset forgets the recorded calls.
func (c *Client) Reset() {
	c.mu.Lock()
	c.calls = nil
	c.mu.Unlock()
}

// Violations checks the recorded calls against the rule, with key the stream
// whose primary every keyless command must reach. It returns one line per
// broken call, and the number of keyless commands seen, so a test can also
// insist the reading it meant to lock was actually made.
//
// A call breaks the rule when it holds a keyless command and
//   - no keyed command at all: rueidis sends it to an arbitrary node;
//   - keyed commands of two slots: rueidis panics ("Mixing no-slot and cross
//     slot commands");
//   - keyed commands of one slot that is not key's: the wrong primary.
func (c *Client) Violations(key string) (lines []string, keyless int) {
	want := c.Slot(key)
	for _, call := range c.Calls() {
		slots := map[uint16]bool{}
		n := 0
		for _, cmd := range call {
			if cmd.Keyless() {
				n++
			} else {
				slots[cmd.Slot] = true
			}
		}
		keyless += n
		if n == 0 {
			continue
		}
		switch {
		case len(slots) == 0:
			lines = append(lines, fmt.Sprintf("%v: a keyless command alone goes to an arbitrary node", call))
		case len(slots) > 1:
			lines = append(lines, fmt.Sprintf("%v: keyless with keyed commands of %d slots (rueidis panics)",
				call, len(slots)))
		case !slots[want]:
			lines = append(lines, fmt.Sprintf("%v: pinned to a slot that is not the one of %s", call, key))
		}
	}
	return lines, keyless
}

// Replies --------------------------------------------------------------------

// The constructors below build replies the way a server would send them, for
// a Reply function. rueidis has no public constructor for a RedisMessage; its
// client-side-cache codec (CacheMarshal/CacheUnmarshalView) is the public way
// in: 7 bytes of TTL, the RESP type byte, an 8-byte length (or the integer)
// and the bytes. Each constructor checks the read back, so a change of that
// codec fails here instead of feeding the tests replies that mean something
// else.

func message(typ byte, n int64, s string) rueidis.RedisMessage {
	buf := make([]byte, 7, 7+9+len(s))
	buf = append(buf, typ)
	buf = binary.BigEndian.AppendUint64(buf, uint64(n))
	buf = append(buf, s...)
	var m rueidis.RedisMessage
	if err := m.CacheUnmarshalView(buf); err != nil {
		panic("clustertest: rueidis cache codec changed: " + err.Error())
	}
	return m
}

// String is a bulk string reply.
func String(s string) rueidis.RedisResult {
	m := message('$', int64(len(s)), s)
	if got, err := m.ToString(); err != nil || got != s {
		panic("clustertest: the forged string reply does not read back")
	}
	return rueidis.NewResult(m, nil)
}

// Int is an integer reply.
func Int(n int64) rueidis.RedisResult {
	m := message(':', n, "")
	if got, err := m.AsInt64(); err != nil || got != n {
		panic("clustertest: the forged integer reply does not read back")
	}
	return rueidis.NewResult(m, nil)
}

// Nil is a nil reply.
func Nil() rueidis.RedisResult {
	m := message('_', 0, "")
	if !m.IsNil() {
		panic("clustertest: the forged nil reply does not read back")
	}
	return rueidis.NewResult(m, nil)
}

// Err is an error reply, such as "NOPERM ..." or "ERR unknown command".
func Err(s string) rueidis.RedisResult {
	m := message('-', int64(len(s)), s)
	if _, ok := rueidis.IsRedisErr(m.Error()); !ok {
		panic("clustertest: the forged error reply does not read back")
	}
	return rueidis.NewResult(m, nil)
}
