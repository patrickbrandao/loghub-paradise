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
	if c.LogHub.EBURL == "" {
		return nil
	}
	ebOrigin := l.Origin["loghub.eb_url"]
	if strings.TrimSpace(c.LogHub.EBURL) == "" {
		// Set, to nothing visible: an operator who meant to unset it would
		// not know the [sink.postgres] keys are what runs, and the dump
		// showed the blanks as if they were a URL (independent review,
		// 2026-10-13).
		return fmt.Errorf("loghub.eb_url (%s) holds only blanks: unset it, or give a postgres:// URL",
			ebOrigin)
	}

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
			// A second "://" is a second URL -- or one inside the password,
			// which is the case the message must also cover.
			return ebTarget{}, errors.New("a postgres:// URL names one server, and holds \"://\" " +
				"only once: a \"://\" in the password must be percent-encoded (%3A%2F%2F)")
		}
		return parsePostgresURL(s)
	}
	shown := "an unrecognised scheme"
	if schemeRE.MatchString(scheme) {
		shown = strconv.Quote(strings.ToLower(scheme) + "://")
	}
	return ebTarget{}, fmt.Errorf("%s is not supported: go-relplog-pg-server only writes to "+
		"PostgreSQL, so the eventbus must be postgres:// or postgresql://", shown)
}

// percentEncodeHint is how a URL that does not parse is explained without
// quoting it (SPEC §10.9, §13.2).
const percentEncodeHint = "percent-encode %, /, #, ? and , in a user, password or token"

// parseURL is url.Parse without any of the URL in the error. url.Error quotes
// the whole input, and its inner error still quotes a piece of it: an
// unescaped '/' in a password gives `invalid port ":Sup3r" after host`, which
// is the password up to the slash ('#', '?' and the ',' of a list cut it the
// same way), and an EscapeError quotes the escape. So nothing of the parser's
// text is kept, only its kind.
func parseURL(s string) (*url.URL, error) {
	if slashedUserinfo(s) {
		// "postgres://u:12/ss@h/db" parses: the authority ends at the first
		// '/' (or '?', '#'), so the host is "u:12" (12 a port), the password's
		// tail lands in the path, query or fragment, and a piece of the
		// password surfaces later in a dial error or the dump. None of the URL
		// is quoted here either.
		return nil, errors.New("not a valid URL: an '@' after the first '/', '?' or '#' means one of " +
			"them in the user or password that is not percent-encoded (%2F, %3F, %23)")
	}
	u, err := url.Parse(s)
	if err != nil {
		var ee url.EscapeError
		if errors.As(err, &ee) {
			if strings.Contains(authorityHost(s), "%") {
				// libpq reads "postgres://%2Frun%2Fpg/db" as a socket
				// directory; Go's parser refuses an escape in a host, and
				// "invalid percent-escape" sent the operator after the
				// password (independent review, 2026-10-13).
				return nil, errors.New("not a valid URL: a percent-encoded host is not supported; " +
					"give a unix socket directory as ?host=/dir, with no host before the path")
			}
			return nil, errors.New("not a valid URL: it holds an invalid percent-escape")
		}
		return nil, errors.New("not a valid URL")
	}
	return u, nil
}

// authorityHost is the host part of s's authority, on the text: what follows
// the last '@' before the first '/', '?' or '#' after "://".
func authorityHost(s string) string {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return ""
	}
	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		rest = rest[:end]
	}
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	return rest
}

// slashedUserinfo reports an '@' past the end of the authority: after the
// first '/', '?' or '#' that follows "://" -- in the path, the query or the
// fragment. No URL of these schemes holds a literal '@' there (a database
// name, a Redis db number, sslmode, a WebSocket endpoint), so it can only be a
// userinfo that an unescaped '/', '?' or '#' in the password cut short: the
// authority then ends early, the user name becomes the host and the start of
// the password the port ("postgres://app:39182#Zsecret9@db/x" dialled
// app:39182 with no password and its ?sslmode thrown away in the fragment).
func slashedUserinfo(s string) bool {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return false
	}
	end := strings.IndexAny(rest, "/?#")
	return end >= 0 && strings.Contains(rest[end:], "@")
}

// parsePostgresURL reads the libpq URI form. Only two query parameters mean
// something here: sslmode, which may only say "no TLS required", and host, the
// libpq way to name a unix socket directory. Any other parameter is refused
// rather than silently ignored, because an operator who wrote it expected an
// effect.
func parsePostgresURL(s string) (ebTarget, error) {
	u, err := parseURL(s)
	if err != nil {
		return ebTarget{}, fmt.Errorf("%w (%s)", err, percentEncodeHint)
	}
	if u.Fragment != "" || strings.HasSuffix(s, "#") {
		// libpq has no fragment: a '#' here is a password cut short or a
		// query that would be thrown away (?sslmode=require after it, which
		// the TLS refusal below would then never see).
		return ebTarget{}, fmt.Errorf("a PostgreSQL URL has no fragment ('#'); %s", percentEncodeHint)
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ebTarget{}, errors.New("the query of the PostgreSQL URL is malformed")
	}
	// Refused keys first, then repeated ones: the order of a map is random,
	// and "x=1&x=2" must get the same answer at every boot.
	for k := range q {
		if k != "sslmode" && k != "host" {
			// The key is named only when libpq knows it: a password pasted
			// after a '?' or a '&' ("?Sup3rSecret", "&hunter2=") is a key
			// too, and the message goes to stderr (SPEC §10.9, §13.2).
			if libpqParams[k] {
				return ebTarget{}, fmt.Errorf("query parameter %q of a PostgreSQL URL is not supported; "+
					"use the [sink.postgres] keys", k)
			}
			return ebTarget{}, errors.New("the PostgreSQL URL has a query parameter that is not a " +
				"libpq one, so it is not shown; only sslmode and host are read, the rest goes " +
				"in the [sink.postgres] keys (" + percentEncodeHint + ")")
		}
	}
	for k, vs := range q {
		// url.Values.Get returns the first value and libpq takes the last:
		// "sslmode=disable&sslmode=require" would pass the TLS refusal below
		// and mean require to every other reader of the same URL.
		if len(vs) > 1 {
			return ebTarget{}, fmt.Errorf("query parameter %q is repeated in the PostgreSQL URL; "+
				"give it once", k)
		}
	}

	var addr string
	switch socket := q.Get("host"); {
	case socket != "":
		if !strings.HasPrefix(socket, "/") || u.Hostname() != "" {
			return ebTarget{}, errors.New("host= in a PostgreSQL URL must be a unix socket " +
				"directory starting with /, with no host before the path")
		}
		// libpq reads host= literally: "/run/pg,/x" is two servers and
		// "/run/pg:5433" a directory with a ':' in its name, where this
		// server would read a directory and a port. Neither is one server
		// in both readings (independent review, 2026-10-13).
		if strings.ContainsAny(socket, ",: \t") {
			return ebTarget{}, errors.New("host= in a PostgreSQL URL names one socket directory, " +
				"with no ',', ':' or blank; the port goes in the URL (postgres://u@:5433/db?host=/run/pg)")
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
	if vs, ok := q["sslmode"]; ok && vs[0] == "" {
		// libpq refuses an empty sslmode; read as "not set" here, the URL
		// meant two things to two readers.
		return ebTarget{}, errors.New("the sslmode of the PostgreSQL URL is empty: drop it or set " +
			"it to disable")
	}
	switch mode := q.Get("sslmode"); mode {
	case "", "disable", "allow", "prefer":
	case "require", "verify-ca", "verify-full":
		return ebTarget{}, fmt.Errorf("sslmode=%s demands TLS, which go-relplog-pg-server does "+
			"not support (it always connects with sslmode=disable): use a trusted network or a "+
			"unix socket, and drop sslmode or set it to disable", mode)
	default:
		// The value is not shown, for the reason the keys are not.
		return ebTarget{}, errors.New("the sslmode of the PostgreSQL URL is not a libpq sslmode " +
			"(disable, allow, prefer, require, verify-ca, verify-full)")
	}

	// The user and the database are required, by decision (SPEC §10.9,
	// DECISOES §2). Left out, libpq would take the operating-system user and
	// a database of the same name, and this server took the registry's
	// default, "syslog": two readers of one LOGHUB_EB_URL reaching two
	// databases. The URL owns both keys, so the operator could not fix it
	// elsewhere either.
	if u.User == nil || u.User.Username() == "" {
		return ebTarget{}, errors.New("the PostgreSQL URL names no user: put it before the '@' " +
			"(postgres://user:password@host/database)")
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return ebTarget{}, errors.New("the PostgreSQL URL names no database: put it in the path " +
			"(postgres://user:password@host/database)")
	}
	pass, _ := u.User.Password()
	values := []ebValue{{"sink.postgres.addr", addr},
		{"sink.postgres.username", u.User.Username()},
		{"sink.postgres.password", pass},
		{"sink.postgres.database", db}}
	return ebTarget{
		values: values,
		owned: []string{"sink.postgres.addr", "sink.postgres.username", "sink.postgres.password",
			"sink.postgres.database"},
	}, nil
}

// libpqParams are the connection keywords libpq documents (PostgreSQL 18,
// "Parameter Key Words"). It is a fixed list on purpose: it decides only
// whether a refused query key is safe to name, and a key outside it may be a
// secret.
var libpqParams = map[string]bool{
	"host": true, "hostaddr": true, "port": true, "dbname": true, "user": true,
	"password": true, "passfile": true, "require_auth": true, "channel_binding": true,
	"connect_timeout": true, "client_encoding": true, "options": true,
	"application_name": true, "fallback_application_name": true, "keepalives": true,
	"keepalives_idle": true, "keepalives_interval": true, "keepalives_count": true,
	"tcp_user_timeout": true, "replication": true, "gssencmode": true, "sslmode": true,
	"requiressl": true, "sslnegotiation": true, "sslcompression": true, "sslcert": true,
	"sslkey": true, "sslkeylogfile": true, "sslpassword": true, "sslcertmode": true,
	"sslrootcert": true, "sslcrl": true, "sslcrldir": true, "sslsni": true,
	"requirepeer": true, "ssl_min_protocol_version": true, "ssl_max_protocol_version": true,
	"min_protocol_version": true, "max_protocol_version": true, "krbsrvname": true,
	"gsslib": true, "gssdelegation": true, "scram_client_key": true, "scram_server_key": true,
	"service": true, "target_session_attrs": true, "load_balance_hosts": true,
	"oauth_issuer": true, "oauth_client_id": true, "oauth_client_secret": true,
	"oauth_scope": true,
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
