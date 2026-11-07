package rediscli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/redis/rueidis"
)

// Redis and Valkey (SPEC §10.3.1). Both speak the same protocol and this
// package treats them the same; the few things that differ live here.

// Server identity -----------------------------------------------------------

// Identity names the server a client reached, for the boot log of the sink
// and the ring: the first question of an incident is "Redis 7.4.12 or Valkey
// 8.1.11?".
//
// It is for people, never for decisions. Valkey reports redis_version:7.2.4
// in every release (7.2, 8.1, 9.0 alike) and its real version in
// valkey_version, with server_name:valkey; a feature gate read from
// redis_version would refuse a Valkey 9. If a feature ever needs a gate, it
// is decided by the command's presence (COMMAND INFO), not by a version.
type Identity struct {
	Server  string // "redis", "valkey" or "unknown"
	Version string // valkey_version for Valkey, redis_version otherwise; "" when unknown
}

// ParseServerInfo reads the text of INFO server. server_name:valkey (present
// since Valkey 7.2) selects valkey_version; anything else is Redis and its
// redis_version. HELLO is not used: Valkey 7.2 answers "server redis" there.
func ParseServerInfo(info string) Identity {
	fields := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), ":")
		if ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	if strings.EqualFold(fields["server_name"], "valkey") {
		if v := fields["valkey_version"]; v != "" {
			return Identity{Server: "valkey", Version: v}
		}
		return Identity{Server: "valkey"}
	}
	if v := fields["redis_version"]; v != "" {
		return Identity{Server: "redis", Version: v}
	}
	return Identity{Server: "unknown"}
}

// Identify runs INFO server once. An ACL without INFO, a proxy that filters
// it or any other failure is Identity{Server: "unknown"}, never an error: the
// identity is a log line, and refusing to boot over it would lock out the
// least-privilege ACL that the policy check already tolerates.
func Identify(ctx context.Context, c rueidis.Client) Identity {
	info, err := c.Do(ctx, c.B().Info().Section("server").Build()).ToString()
	if err != nil {
		return Identity{Server: "unknown"}
	}
	return ParseServerInfo(info)
}

// Replies that waiting cannot fix ---------------------------------------------

// dbUnavailable reports the handshake replies to SELECT <db> of a server that
// does not have that database: a Redis Cluster or a Valkey < 9 in cluster mode
// ("ERR SELECT is not allowed in cluster mode"), or a Valkey 9 cluster whose
// cluster-databases does not reach the db ("ERR DB index is out of range", the
// same text a standalone server gives above its databases). rueidis sends the
// SELECT in the handshake of every connection whenever db != 0.
func dbUnavailable(upper string) bool {
	upper = strings.TrimPrefix(upper, "ERR ")
	return strings.HasPrefix(upper, "SELECT IS NOT ALLOWED IN CLUSTER MODE") ||
		strings.HasPrefix(upper, "DB INDEX IS OUT OF RANGE")
}

// The text of a reply is matched with and without its "ERR " code: the
// handshake error of rueidis's NewClient carries the reply without it
// ("SELECT is not allowed in cluster mode", seen against Redis 7.4 and Valkey
// 7.2-9.0 on 2026-11-07), a command reply keeps it.

// dbUnavailableHint is appended to the error of New for those replies.
const dbUnavailableHint = "the db does not exist on this server: Redis Cluster and Valkey < 9 only " +
	"have db 0; on Valkey 9 the db must exist (cluster-databases)"

// scriptingMissing reports "ERR unknown command 'evalsha'" (or 'eval'): a
// server built without Lua, or a managed service that blocks scripting. The
// Redis ring cannot work without it; waiting does not add it.
func scriptingMissing(upper string) bool {
	upper = strings.TrimPrefix(upper, "ERR ")
	for _, p := range []string{"UNKNOWN COMMAND 'EVALSHA'", "UNKNOWN COMMAND 'EVAL'",
		"UNKNOWN COMMAND `EVALSHA`", "UNKNOWN COMMAND `EVAL`"} {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}

// ScriptingMissing reports whether err is the reply of a server without Lua
// scripting (see scriptingMissing). The ring uses it to say why it is fatal.
func ScriptingMissing(err error) bool {
	rerr, ok := rueidis.IsRedisErr(err)
	return ok && scriptingMissing(strings.ToUpper(rerr.Error()))
}

// explainNew adds the reason to a handshake error of New when the reply alone
// would send the operator after the wrong thing.
func explainNew(err error) error {
	var rerr *rueidis.RedisError
	if errors.As(err, &rerr) && dbUnavailable(strings.ToUpper(rerr.Error())) {
		return fmt.Errorf("%w (%s)", err, dbUnavailableHint)
	}
	return err
}

// The eviction policy, at every probe -------------------------------------------

// How the policy is read, learned by the first reading that needed more than
// INFO (the PolicyWatch).
const (
	policyByInfo int32 = iota
	policyByConfig
	policyNone
)

// PolicyWatch re-reads maxmemory-policy at every probe of its user -- the
// sink's Health and the ring's RefreshStats -- because CONFIG SET changes the
// policy of a live server, and a managed Redis or Valkey changes it from a
// panel without a restart (SPEC §9.5, §10.3). It remembers which reading
// works (INFO memory, CONFIG GET, or neither) so a probe pipelines exactly
// one command for it and stays one round trip, as go-webhook-server does
// (W SPEC §8.2.5).
//
// The zero value reads by INFO first. It is safe for concurrent use.
type PolicyWatch struct {
	read   atomic.Int32
	warned atomic.Bool
}

// Cmd is the command to pipeline with the probe, and false when the policy
// is known to be unreadable (both readings refused): then nothing is asked.
func (w *PolicyWatch) Cmd(c rueidis.Client) (rueidis.Completed, bool) {
	switch w.read.Load() {
	case policyByInfo:
		return c.B().Info().Section("memory").Build(), true
	case policyByConfig:
		return c.B().ConfigGet().Parameter("maxmemory-policy").Build(), true
	}
	return rueidis.Completed{}, false
}

// Judge reads the reply of Cmd. It returns what CheckEviction returns: nil,
// an ErrEvictionPolicy error (fatal), an ErrPolicyUnknown error (WARN, see
// FirstUnknown), or another error (the connection, a transient reply). When
// INFO answered without the policy line, or refused, the CONFIG GET fallback
// runs here, one extra round trip, only until the watch has learned.
func (w *PolicyWatch) Judge(ctx context.Context, c rueidis.Client, resp rueidis.RedisResult) (string, error) {
	switch w.read.Load() {
	case policyNone:
		return "", nil
	case policyByConfig:
		kv, cerr := resp.AsStrMap()
		policy, err := fromConfig(kv, cerr, nil)
		if errors.Is(err, ErrPolicyUnknown) {
			// The ACL changed under us: learn again from INFO next time.
			w.read.Store(policyByInfo)
		}
		return policy, err
	}
	info, ierr := resp.ToString()
	if ierr == nil {
		if policy, ok := policyFromInfo(info); ok {
			return policy, verdict(policy)
		}
	} else if !replyRefused(ierr) {
		return "", ierr
	}
	kv, cerr := c.Do(ctx, c.B().ConfigGet().Parameter("maxmemory-policy").Build()).AsStrMap()
	policy, err := fromConfig(kv, cerr, ierr)
	switch {
	case errors.Is(err, ErrPolicyUnknown):
		w.read.Store(policyNone)
	case cerr == nil:
		w.read.Store(policyByConfig)
	}
	return policy, err
}

// Check is one standalone reading: Cmd and Judge without a probe around them.
func (w *PolicyWatch) Check(ctx context.Context, c rueidis.Client) (string, error) {
	cmd, ok := w.Cmd(c)
	if !ok {
		return "", nil
	}
	return w.Judge(ctx, c, c.Do(ctx, cmd))
}

// FirstUnknown reports true the first time it is called, so the WARN of an
// unreadable policy is logged once per process and not at every probe.
func (w *PolicyWatch) FirstUnknown() bool { return !w.warned.Swap(true) }

// fromConfig reads the policy from a CONFIG GET reply; ierr is the INFO error
// that sent the reading here, if any, for the message.
func fromConfig(kv map[string]string, cerr, ierr error) (string, error) {
	if cerr == nil {
		if policy, ok := kv["maxmemory-policy"]; ok {
			return policy, verdict(policy)
		}
	} else if !replyRefused(cerr) {
		return "", cerr
	}
	reason := "INFO memory has no maxmemory_policy line"
	if ierr != nil {
		reason = ierr.Error()
	}
	if cerr != nil {
		reason += "; CONFIG GET: " + cerr.Error()
	}
	return "", fmt.Errorf("%w (%s)", ErrPolicyUnknown, reason)
}
