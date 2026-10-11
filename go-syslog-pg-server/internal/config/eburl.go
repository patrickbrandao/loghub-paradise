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

// ebTarget is what a LOGHUB_EB_URL expands to (SPEC §10.9): the values it
// fills, and the keys it owns.
type ebTarget struct {
	values []ebValue

	// owned are the keys the URL can express. With an eb_url set, an explicit
	// value for any of them is a second source of truth and is refused, even
	// when this particular URL left the part out: the URL still decides it
	// (an absent password in the URL means "no password", not "look
	// elsewhere"). Keys a URL cannot express -- the table, the pool size --
	// stay free to complement it.
	owned []string
}

type ebValue struct{ name, raw string }

// applyEBURL expands loghub.eb_url into the sink keys, after every explicit
// source has been applied, and enforces "one source of truth": an explicit
// owned key is exit 1 naming both sources.
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

	for _, v := range t.values {
		p := ix.byName[v.name]
		if p == nil {
			panic("config: eb_url fills unknown parameter " + v.name)
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

// parseEBURL reads the URL (SPEC §10.9). Every error names the scheme at most,
// never the URL. Only PostgreSQL is an eventbus here: this server has no other
// sink, and a nats:// or redis:// URL left over from a sibling deployment must
// fail the boot, not be quietly ignored.
func parseEBURL(raw string) (ebTarget, error) {
	s := strings.TrimSpace(raw)
	scheme, _, ok := strings.Cut(s, "://")
	if !ok {
		return ebTarget{}, errors.New("not a URL: want postgres:// or postgresql://")
	}
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		// One server only. Splitting on commas is not an option here: a
		// password may legally contain one.
		if strings.Count(s, "://") > 1 {
			return ebTarget{}, errors.New("a postgres:// URL names one server")
		}
		return parsePostgresURL(s)
	}
	shown := "an unrecognised scheme"
	if schemeRE.MatchString(scheme) {
		shown = strconv.Quote(strings.ToLower(scheme) + "://")
	}
	return ebTarget{}, fmt.Errorf("%s is not supported: go-syslog-pg-server only writes to "+
		"PostgreSQL, so the eventbus must be postgres:// or postgresql://", shown)
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

// parsePostgresURL reads the libpq URI form. Only two query parameters mean
// something here: sslmode, which may only say "no TLS required", and host, the
// libpq way to name a unix socket directory. Any other parameter is refused
// rather than silently ignored, because an operator who wrote it expected an
// effect.
func parsePostgresURL(s string) (ebTarget, error) {
	u, err := parseURL(s)
	if err != nil {
		return ebTarget{}, err
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ebTarget{}, errors.New("the query of the PostgreSQL URL is malformed")
	}
	for k, vs := range q {
		if k != "sslmode" && k != "host" {
			return ebTarget{}, fmt.Errorf("query parameter %q of a PostgreSQL URL is not supported; "+
				"use the [sink.postgres] keys", k)
		}
		// url.Values.Get returns the first value and libpq takes the last:
		// "sslmode=disable&sslmode=require" would pass the TLS refusal below
		// and mean require to every other reader of the same URL.
		if len(vs) > 1 {
			return ebTarget{}, fmt.Errorf("query parameter %q is repeated in the PostgreSQL URL", k)
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
		// postgres://u:p@:5433/db?host=/run/pg: libpq dials
		// /run/pg/.s.PGSQL.5433. Dropping the port would dial 5432, and the
		// URL owns sink.postgres.addr, so the operator could not correct it.
		if port := u.Port(); port != "" {
			addr = socket + ":" + port
		}
	case u.Hostname() == "":
		return ebTarget{}, errors.New("a PostgreSQL URL needs a host")
	default:
		port := u.Port()
		if port == "" {
			port = "5432"
		}
		addr = net.JoinHostPort(u.Hostname(), port)
	}

	// This server has no TLS (SPEC §1.2) and always connects with
	// sslmode=disable. allow and prefer accept a plaintext connection, and
	// prefer is the libpq default that many generated URLs spell out, so they
	// pass. require, verify-ca and verify-full refuse plaintext: accepting
	// them would send the password and every event in clear to an operator
	// who asked for the opposite.
	switch mode := q.Get("sslmode"); mode {
	case "", "disable", "allow", "prefer":
	case "require", "verify-ca", "verify-full":
		return ebTarget{}, fmt.Errorf("sslmode=%s demands TLS, which go-syslog-pg-server does "+
			"not support (it always connects with sslmode=disable): use a trusted network or a "+
			"unix socket, and drop sslmode or set it to disable", mode)
	default:
		return ebTarget{}, fmt.Errorf("sslmode=%q is not a libpq sslmode", mode)
	}

	values := []ebValue{{"sink.postgres.addr", addr}}
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
		values: values,
		owned: []string{"sink.postgres.addr", "sink.postgres.username", "sink.postgres.password",
			"sink.postgres.database"},
	}, nil
}

// redactURL replaces the userinfo of every URL in a comma list. A password is
// replaced alone (user:***); a bare user part is replaced whole, because a
// user name alone may still be a secret somebody pasted into the wrong place.
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
