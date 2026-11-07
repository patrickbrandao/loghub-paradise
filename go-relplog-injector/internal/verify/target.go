// Package verify checks, after a run, what reached the destination the
// server writes to (SPEC §8.5): the NATS JetStream stream, the PostgreSQL
// loghub.inbox queue, the Redis/Valkey stream or the SQLite events table. It
// counts, by the run's seq marker, the acknowledged messages that are missing
// (lost) and the copies stored more than once (duplicated).
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
	// Display names the destination without secrets: scheme, hosts and
	// ports, or the file (SPEC §8.5).
	Display string

	url     string // what the client library gets (the injector's params removed)
	tls     bool
	stream  string
	space   string
	ca      string   // ca= of the URL, or --verify-tls-ca
	secrets []string // users, passwords and tokens of the URL, scrubbed from errors

	spaceSet bool
	table    string
	file     string

	start streamStart // set by Probe (SPEC §8.5)
}

// Defaults of the destinations (go-relplog-server SPEC §3.6, §3.7.1, §3.8).
const (
	DefaultNATSStream = "LOGHUB_SYSLOG"
	DefaultSpace      = "default"
	DefaultTable      = "loghub.inbox"
	SQLiteTable       = "syslog_events"
)

// errors never quote the URL: it may carry a password.
var errBadURL = errors.New("--verify-url: not a valid URL (encode %, /, #, ? and @ in the password)")

// ParseURL parses --verify-url. Errors name the scheme or the parameter,
// never a value. A NATS URL may be a comma-separated list, as the server's
// sink.nats.urls: every element is parsed here, before nats.go sees it (its
// parse error quoted the element, password included; SPEC §8.5).
func ParseURL(raw string) (*Target, error) {
	raw = strings.TrimSpace(raw)
	scheme, _, ok := strings.Cut(raw, "://")
	if !ok {
		return nil, errBadURL
	}
	scheme = strings.ToLower(scheme)
	t := &Target{space: DefaultSpace}
	own := map[string]*string{}
	switch scheme {
	case "nats", "tls":
		t.Kind, t.stream = KindNATS, DefaultNATSStream
		own = map[string]*string{"stream": &t.stream, "space": &t.space, "ca": &t.ca}
	case "redis", "rediss", "valkey", "valkeys":
		t.Kind = KindRedis
		t.tls = scheme == "rediss" || scheme == "valkeys"
		own = map[string]*string{"stream": &t.stream, "space": &t.space, "ca": &t.ca}
	case "postgres", "postgresql":
		t.Kind, t.table = KindPostgres, DefaultTable
		own = map[string]*string{"table": &t.table}
	default:
		return nil, fmt.Errorf("--verify-url: unsupported scheme %q (want nats, tls, postgres, postgresql, redis, rediss, valkey or valkeys)", scheme)
	}
	elems := []string{raw}
	if t.Kind == KindNATS {
		elems = strings.Split(raw, ",")
	}
	seen := map[string]bool{}
	var urls, hosts []string
	for _, el := range elems {
		u, err := url.Parse(strings.TrimSpace(el))
		if err != nil || (u.Host == "" && t.Kind != KindPostgres) {
			return nil, errBadURL // postgres may name a socket with ?host=/dir
		}
		es := strings.ToLower(u.Scheme)
		if t.Kind == KindNATS {
			switch es {
			case "nats":
			case "tls":
				t.tls = true
			default:
				return nil, errors.New("--verify-url: every element of a NATS list is nats:// or tls://")
			}
		}
		t.addSecrets(u)
		q := u.Query()
		for k, vs := range q {
			dst, mine := own[k]
			switch {
			case mine:
				if len(vs) != 1 || vs[0] == "" || seen[k] {
					return nil, fmt.Errorf("--verify-url: parameter %q must appear once, not empty", k)
				}
				seen[k] = true
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
			// rueidis takes valkey:// and valkeys:// as redis:// and
			// rediss://; say it in the canonical form anyway.
			switch es {
			case "valkey":
				u.Scheme = "redis"
			case "valkeys":
				u.Scheme = "rediss"
			}
		}
		u.RawQuery = q.Encode()
		urls = append(urls, u.String())
		hosts = append(hosts, u.Host)
		if t.Kind == KindPostgres && len(u.Path) > 1 {
			hosts[len(hosts)-1] += u.Path // the database name
		}
	}
	if t.ca != "" && !t.tls {
		return nil, fmt.Errorf("--verify-url: parameter %q needs a TLS scheme (tls, rediss, valkeys)", "ca")
	}
	t.url = strings.Join(urls, ",")
	t.Display = scheme + "://" + strings.Join(hosts, ",")
	return t, nil
}

func (t *Target) addSecrets(u *url.URL) {
	if u.User == nil {
		return
	}
	if p, ok := u.User.Password(); ok && p != "" {
		t.secrets = append(t.secrets, p)
	}
	if n := u.User.Username(); n != "" {
		t.secrets = append(t.secrets, n) // a NATS token goes as the user
	}
}

// SetCA applies --verify-tls-ca (SPEC §8.5): the same as the ca= parameter,
// one or the other.
func (t *Target) SetCA(file string) error {
	switch {
	case file == "":
		return nil
	case t.Kind == KindSQLite:
		return errors.New("--verify-tls-ca does not apply to --verify-sqlite")
	case !t.tls:
		return errors.New("--verify-tls-ca needs a TLS destination: tls://, rediss:// or valkeys:// (PostgreSQL takes sslrootcert= in the URL)")
	case t.ca != "":
		return errors.New("--verify-tls-ca and the ca= parameter of --verify-url: give the CA once")
	}
	if _, err := caPool(file); err != nil {
		return fmt.Errorf("--verify-tls-ca: %w", err)
	}
	t.ca = file
	return nil
}

// clean is the error of a client library as the report may show it: a
// *url.Error is never forwarded (its text is the URL), and every user,
// password and token of the URL is scrubbed from the rest (SPEC §8.5).
func (t *Target) clean(prefix string, err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return errors.New(prefix + ": the URL does not parse (encode %, /, #, ? and @ in the password)")
	}
	s := err.Error()
	for _, sec := range t.secrets {
		for _, v := range []string{sec, url.QueryEscape(sec), url.PathEscape(sec)} {
			s = strings.ReplaceAll(s, v, "xxxxx")
		}
	}
	return errors.New(prefix + ": " + s)
}

// Override applies --verify-space and --verify-key (the NATS stream, the
// Redis stream key or the PostgreSQL table); empty keeps the URL's or the
// default.
func (t *Target) Override(space, key string) error {
	if space != "" {
		t.space = space
		t.spaceSet = true // PostgreSQL filters the space column only when asked
	}
	if key != "" {
		switch t.Kind {
		case KindPostgres:
			t.table = key
		default:
			t.stream = key
		}
	}
	return nil
}

// redisStream is the stream key: the given one, or the server's default
// loghub.<space>.syslog.
func (t *Target) redisStream() string {
	if t.stream != "" {
		return t.stream
	}
	return "loghub." + t.space + ".syslog"
}

// SQLite returns the target of --verify-sqlite.
func SQLite(path string) *Target {
	return &Target{Kind: KindSQLite, file: path, Display: "sqlite:" + path, table: SQLiteTable}
}
