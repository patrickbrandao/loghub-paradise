package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// ebTarget is what a LOGHUB_EB_URL expands to (SPEC §10.9): the sink it
// selects, the values it fills, and the keys it owns.
type ebTarget struct {
	sinkType string
	values   []ebValue

	// owned are the keys the URL can express. With an eb_url set, an explicit
	// value for any of them is a second source of truth and is refused, even
	// when this particular URL left the part out: the URL still decides it
	// (an absent password in the URL means "no password", not "look
	// elsewhere"). Keys a URL cannot express -- a .creds file, a CA bundle,
	// tls_insecure, a Sentinel master, the table, the stream -- stay free to
	// complement it.
	owned []string
}

type ebValue struct{ name, raw string }

// applyEBURL expands loghub.eb_url into the sink keys, after every explicit
// source has been applied, and enforces "one source of truth": an explicit
// sink.type different from the scheme, or an explicit owned key, is exit 1
// naming both sources.
func applyEBURL(l *Loaded, ix *index) error {
	c := l.Config
	if strings.TrimSpace(c.LogHub.EBURL) == "" {
		return nil
	}
	ebOrigin := l.Origin["loghub.eb_url"]

	t, err := parseEBURL(c.LogHub.EBURL)
	if err != nil {
		// Never the URL itself in the message: it may carry a password.
		return fmt.Errorf("loghub.eb_url (%s): %w", ebOrigin, err)
	}

	if l.SetIn["sink.type"] && c.Sink.Type != t.sinkType {
		return fmt.Errorf("loghub.eb_url (%s) selects sink.type=%s, but sink.type=%s is set "+
			"explicitly (%s): with LOGHUB_EB_URL the scheme is the one source of truth for "+
			"the sink (SPEC §10.9); remove one of them",
			ebOrigin, t.sinkType, c.Sink.Type, l.Origin["sink.type"])
	}
	for _, name := range t.owned {
		if l.SetIn[name] {
			return fmt.Errorf("loghub.eb_url (%s) fills %s, but %s is also set explicitly (%s): "+
				"with LOGHUB_EB_URL the URL is the one source of truth for the connection "+
				"(SPEC §10.9); remove one of them", ebOrigin, name, name, l.Origin[name])
		}
	}

	l.ebOwned = make(map[string]bool, len(t.owned))
	for _, name := range t.owned {
		l.ebOwned[name] = true
	}

	all := append([]ebValue{{"sink.type", t.sinkType}}, t.values...)
	for _, v := range all {
		p := ix.byName[v.name]
		if p == nil {
			panic("config: eb_url fills unknown parameter " + v.name)
		}
		if l.SetIn[v.name] {
			continue // only sink.type can get here, and it already agrees
		}
		if err := setValue(p, v.raw); err != nil {
			return fmt.Errorf("loghub.eb_url (%s): %s: %w", ebOrigin, v.name, err)
		}
		l.Source[v.name] = FromEBURL
		l.Origin[v.name] = "loghub.eb_url"
	}
	return nil
}

// schemeRE is what a URL scheme may look like (RFC 3986 §3.1). Anything else is
// reported as "unrecognised" instead of being echoed: a mangled value may hold
// a password where the scheme should be.
var schemeRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]{0,31}$`)

// parseEBURL maps the URL onto a sink by its scheme (SPEC §10.9). Every error
// names the scheme at most, never the URL.
func parseEBURL(raw string) (ebTarget, error) {
	s := strings.TrimSpace(raw)
	scheme, _, ok := strings.Cut(s, "://")
	if !ok {
		return ebTarget{}, errors.New("not a URL: want nats://, tls://, redis://, rediss://, " +
			"postgres:// or postgresql://")
	}
	switch strings.ToLower(scheme) {
	case "nats", "tls":
		return parseNATSURL(s)
	case "redis", "rediss", "postgres", "postgresql":
		// One server only. Splitting on commas is not an option here: a
		// password may legally contain one.
		if strings.Count(s, "://") > 1 {
			return ebTarget{}, fmt.Errorf("a %s:// URL names one server; only nats:// takes a list",
				strings.ToLower(scheme))
		}
		if strings.HasPrefix(strings.ToLower(scheme), "redis") {
			return parseRedisURL(s)
		}
		return parsePostgresURL(s)
	}
	shown := "an unrecognised scheme"
	if schemeRE.MatchString(scheme) {
		shown = strconv.Quote(strings.ToLower(scheme) + "://")
	}
	return ebTarget{}, fmt.Errorf("%s is not supported: the eventbus must be nats://, tls://, "+
		"redis://, rediss://, postgres:// or postgresql:// (amqp:// included, it is refused)", shown)
}

// parseURL is url.Parse without the URL in the error: url.Error quotes the
// whole input, password included.
func parseURL(s string) (*url.URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			return nil, fmt.Errorf("not a valid URL: %v", ue.Err)
		}
		return nil, errors.New("not a valid URL")
	}
	return u, nil
}

// parseNATSURL fills sink.nats.urls with the URL itself (user and password
// included, which the client reads from there) and turns tls on for tls://.
// A comma list is accepted because that is how nats.go spells a cluster, and
// it is what sink.nats.urls already is; the schemes must agree, or half the
// cluster would be dialled in clear.
func parseNATSURL(s string) (ebTarget, error) {
	parts := parseList(s)
	var plain, secure bool
	for _, part := range parts {
		u, err := parseURL(part)
		if err != nil {
			return ebTarget{}, err
		}
		switch strings.ToLower(u.Scheme) {
		case "nats":
			plain = true
		case "tls":
			secure = true
		default:
			return ebTarget{}, errors.New("a NATS URL list must use nats:// or tls:// in every entry")
		}
		if u.Hostname() == "" {
			return ebTarget{}, errors.New("a NATS URL needs a host")
		}
		if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return ebTarget{}, errors.New("a NATS URL carries no path or query; use the [sink.nats] keys")
		}
	}
	if plain && secure {
		return ebTarget{}, errors.New("the NATS URL list mixes nats:// and tls://")
	}
	return ebTarget{
		sinkType: SinkTypeNATS,
		values: []ebValue{
			{"sink.nats.urls", strings.Join(parts, ",")},
			{"sink.nats.tls", strconv.FormatBool(secure)},
		},
		owned: []string{"sink.nats.urls", "sink.nats.tls", "sink.nats.username",
			"sink.nats.password", "sink.nats.token"},
	}, nil
}

// parseRedisURL follows the redis:// URI convention (the one go-redis and
// redis-py read): userinfo is username:password, the path is the database
// number, rediss:// is TLS.
func parseRedisURL(s string) (ebTarget, error) {
	u, err := parseURL(s)
	if err != nil {
		return ebTarget{}, err
	}
	host := u.Hostname()
	if host == "" {
		return ebTarget{}, errors.New("a Redis URL needs a host")
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	var user, pass string
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	db := 0
	if path := strings.TrimPrefix(u.Path, "/"); path != "" {
		n, err := strconv.Atoi(path)
		if err != nil || n < 0 {
			return ebTarget{}, errors.New("the path of a Redis URL is the database number (redis://host:6379/0)")
		}
		db = n
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return ebTarget{}, errors.New("query parameters of a Redis URL are not supported; use the [sink.redis] keys")
	}
	return ebTarget{
		sinkType: SinkTypeRedis,
		values: []ebValue{
			{"sink.redis.addrs", net.JoinHostPort(host, port)},
			{"sink.redis.username", user},
			{"sink.redis.password", pass},
			{"sink.redis.db", strconv.Itoa(db)},
			{"sink.redis.tls", strconv.FormatBool(strings.EqualFold(u.Scheme, "rediss"))},
		},
		owned: []string{"sink.redis.addrs", "sink.redis.username", "sink.redis.password",
			"sink.redis.db", "sink.redis.tls"},
	}, nil
}

// parsePostgresURL reads the libpq URI form. Only two query parameters mean
// something here: sslmode, which decides sink.postgres.tls, and host, the libpq
// way to name a unix socket directory. Any other parameter is refused rather
// than silently ignored, because an operator who wrote it expected an effect.
func parsePostgresURL(s string) (ebTarget, error) {
	u, err := parseURL(s)
	if err != nil {
		return ebTarget{}, err
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ebTarget{}, errors.New("the query of the PostgreSQL URL is malformed")
	}
	for k := range q {
		if k != "sslmode" && k != "host" {
			return ebTarget{}, fmt.Errorf("query parameter %q of a PostgreSQL URL is not supported; "+
				"use the [sink.postgres] keys", k)
		}
	}

	var addr string
	switch socket := q.Get("host"); {
	case socket != "":
		if !strings.HasPrefix(socket, "/") || u.Hostname() != "" {
			return ebTarget{}, errors.New("host= in a PostgreSQL URL must be a unix socket " +
				"directory starting with /, with no host before the path")
		}
		addr = socket
	case u.Hostname() == "":
		return ebTarget{}, errors.New("a PostgreSQL URL needs a host")
	default:
		port := u.Port()
		if port == "" {
			port = "5432"
		}
		addr = net.JoinHostPort(u.Hostname(), port)
	}

	// require, verify-ca and verify-full all refuse a plaintext connection,
	// so they all turn TLS on; SPEC §10.9 names require and verify-full, and
	// verify-ca demands TLS just the same. Verification itself follows
	// sink.postgres.tls_ca and tls_insecure, which the URL cannot express.
	var tlsOn bool
	switch mode := q.Get("sslmode"); mode {
	case "", "disable", "allow", "prefer":
	case "require", "verify-ca", "verify-full":
		tlsOn = true
	default:
		return ebTarget{}, fmt.Errorf("sslmode=%q is not a libpq sslmode", mode)
	}

	values := []ebValue{
		{"sink.postgres.addr", addr},
		{"sink.postgres.tls", strconv.FormatBool(tlsOn)},
	}
	var pass string
	if u.User != nil {
		if user := u.User.Username(); user != "" {
			values = append(values, ebValue{"sink.postgres.username", user})
		}
		pass, _ = u.User.Password()
	}
	values = append(values, ebValue{"sink.postgres.password", pass})
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		values = append(values, ebValue{"sink.postgres.database", db})
	}
	return ebTarget{
		sinkType: SinkTypePostgres,
		values:   values,
		owned: []string{"sink.postgres.addr", "sink.postgres.username", "sink.postgres.password",
			"sink.postgres.database", "sink.postgres.tls"},
	}, nil
}

// redactURL replaces the userinfo of every URL in a comma list. A password is
// replaced alone (user:***); a bare user part may be a NATS token, so it is
// replaced whole.
//
// It works on the text, not through net/url, on purpose. url.URL.String()
// would print "%2A%2A%2A", and -- worse -- a password may legally hold a comma
// or a slash, which a parser splits on: a comma-split "postgres://u:pa,ss@h"
// would print "pa" in clear. So a segment without "://" is glued back onto the
// previous one, everything up to the LAST '@' is userinfo, and a value that
// does not even start with a scheme is replaced entirely. Over-redaction is
// the safe failure.
func redactURL(v string) string {
	if v == "" {
		return ""
	}
	var parts []string
	for _, p := range strings.Split(v, ",") {
		if len(parts) > 0 && !strings.Contains(p, "://") {
			parts[len(parts)-1] += "," + p
			continue
		}
		parts = append(parts, p)
	}
	for i, p := range parts {
		parts[i] = redactOneURL(p)
	}
	return strings.Join(parts, ",")
}

func redactOneURL(s string) string {
	i := strings.Index(s, "://")
	if i < 0 {
		if strings.TrimSpace(s) == "" {
			return s
		}
		return "***"
	}
	rest := s[i+3:]
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return s
	}
	red := "***"
	if user, _, hasPass := strings.Cut(rest[:at], ":"); hasPass {
		red = user + ":***"
	}
	return s[:i+3] + red + rest[at:]
}
