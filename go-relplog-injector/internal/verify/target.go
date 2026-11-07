// Package verify checks, after a run, what reached the destination the
// server writes to (SPEC §8.5): the NATS JetStream stream, the PostgreSQL
// loghub.inbox queue, the Redis/Valkey stream or the SQLite events table. It
// counts, by the run's seq marker, the acknowledged messages that are missing
// (lost) and the ones stored more than once (duplicated).
//
// It is the only package of the module with dependencies outside the
// standard library: the clients the servers use, at the versions of
// go-relplog-server/go.mod (decision 20).
package verify

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Kinds of destination.
const (
	KindNATS     = "nats"
	KindRedis    = "redis"
	KindPostgres = "postgres"
	KindSQLite   = "sqlite"
)

// Target is a parsed --verify-url or --verify-sqlite.
type Target struct {
	Kind string
	// Display names the destination without secrets: scheme, host and port,
	// or the file (SPEC §8.5).
	Display string

	url    string // what the client library gets (the injector's params removed)
	tls    bool
	stream string
	space  string
	ca     string
	table  string
	file   string
}

// Defaults of the destinations (go-relplog-server SPEC §3.6, §3.7.1, §3.8).
const (
	DefaultNATSStream = "LOGHUB_SYSLOG"
	DefaultSpace      = "default"
	DefaultTable      = "loghub.inbox"
	SQLiteTable       = "syslog_events"
)

// errors never quote the URL: it may carry a password.
var errBadURL = errors.New("--verify-url: not a valid URL")

// ParseURL parses --verify-url. Errors name the scheme or the parameter,
// never a value.
func ParseURL(raw string) (*Target, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Host == "" && !strings.HasPrefix(strings.ToLower(u.Scheme), "postgres")) {
		return nil, errBadURL // postgres may name a socket with ?host=/dir
	}
	t := &Target{space: DefaultSpace}
	scheme := strings.ToLower(u.Scheme)
	own := map[string]*string{}
	switch scheme {
	case "nats", "tls":
		t.Kind, t.tls, t.stream = KindNATS, scheme == "tls", DefaultNATSStream
		own = map[string]*string{"stream": &t.stream, "space": &t.space, "ca": &t.ca}
	case "redis", "rediss", "valkey", "valkeys":
		t.Kind = KindRedis
		t.tls = scheme == "rediss" || scheme == "valkeys"
		own = map[string]*string{"stream": &t.stream, "space": &t.space, "ca": &t.ca}
	case "postgres", "postgresql":
		t.Kind, t.table = KindPostgres, DefaultTable
		own = map[string]*string{"table": &t.table}
	default:
		return nil, fmt.Errorf("--verify-url: unsupported scheme %q (want nats, tls, postgres, postgresql, redis, rediss, valkey or valkeys)", u.Scheme)
	}
	q := u.Query()
	for k, vs := range q {
		dst, mine := own[k]
		switch {
		case mine:
			if len(vs) != 1 || vs[0] == "" {
				return nil, fmt.Errorf("--verify-url: parameter %q must appear once, not empty", k)
			}
			*dst = vs[0]
			q.Del(k)
		case t.Kind == KindRedis && k == "db":
			// passed to rueidis
		case t.Kind == KindPostgres:
			// sslmode, sslrootcert, ...: pgx's
		default:
			return nil, fmt.Errorf("--verify-url: unknown parameter %q for %s", k, scheme)
		}
	}
	if t.Kind == KindRedis {
		if t.stream == "" {
			t.stream = "loghub." + t.space + ".syslog"
		}
		// rueidis takes valkey:// and valkeys:// as redis:// and rediss://;
		// say it in the canonical form anyway.
		switch scheme {
		case "valkey":
			u.Scheme = "redis"
		case "valkeys":
			u.Scheme = "rediss"
		}
	}
	if t.ca != "" && !t.tls {
		return nil, fmt.Errorf("--verify-url: parameter %q needs a TLS scheme (tls, rediss, valkeys)", "ca")
	}
	u.RawQuery = q.Encode()
	t.url = u.String()
	t.Display = scheme + "://" + u.Host
	if t.Kind == KindPostgres && len(u.Path) > 1 {
		t.Display += u.Path // the database name
	}
	return t, nil
}

// SQLite returns the target of --verify-sqlite.
func SQLite(path string) *Target {
	return &Target{Kind: KindSQLite, file: path, Display: "sqlite:" + path, table: SQLiteTable}
}
