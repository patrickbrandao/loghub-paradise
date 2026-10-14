package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// ebTarget is what a LOGHUB_EB_URL expands to (SPEC §8.7.11): the values it
// fills, and the keys it owns.
type ebTarget struct {
	values []ebValue

	// owned are the keys the URL can express. With an eb_url set, an explicit
	// value for any of them is a second source of truth and is refused, even
	// when this particular URL left the part out: the URL still decides it
	// (an absent password in the URL means "no password", not "look
	// elsewhere"). Keys a URL cannot express -- a .creds file, the mode, the
	// stream -- stay free to complement it.
	owned []string
}

// percentEncodeHint is how a URL that does not parse is explained without
// quoting it.
const percentEncodeHint = "percent-encode %, /, #, ? and , in a user, password or token"

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
		// not know the [sink.nats] keys are what runs, and the dump showed
		// the blanks as if they were a URL (go-relplog-pg-server's review,
		// ported from go-syslog-nats-server on 2026-10-13).
		return fmt.Errorf("loghub.eb_url (%s) holds only blanks: unset it, or give a nats:// URL",
			ebOrigin)
	}

	t, err := parseEBURL(c.LogHub.EBURL)
	if err != nil {
		// Never the URL itself in the message: it may carry a password.
		return fmt.Errorf("loghub.eb_url (%s): %w", ebOrigin, err)
	}

	for _, name := range t.owned {
		// An explicit empty value is no second truth: it says nothing the URL
		// could contradict. It is also what a compose file produces for a
		// variable it forwards but the operator left unset
		// (KEY: ${KEY:-}), which would otherwise make LOGHUB_EB_URL
		// unusable there without editing the file.
		if l.SetIn[name] && formatValue(ix.byName[name].Kind, ix.byName[name].Ptr) != "" {
			return fmt.Errorf("loghub.eb_url (%s) fills %s, but %s is also set explicitly (%s): "+
				"with LOGHUB_EB_URL the URL is the one source of truth for the connection "+
				"(SPEC §8.7.11); remove one of them", ebOrigin, name, name, l.Origin[name])
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
		if err := setValue(p.Kind, p.Ptr, v.raw); err != nil {
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

// parseEBURL reads the URL (SPEC §8.7.11). Every error names the scheme at
// most, never the URL. Only NATS is an eventbus here: this server has no other
// sink, and a postgres:// or redis:// URL left over from a sibling deployment
// must fail the boot, not be quietly ignored.
func parseEBURL(raw string) (ebTarget, error) {
	s := strings.TrimSpace(raw)
	scheme, _, ok := strings.Cut(s, "://")
	if !ok {
		return ebTarget{}, errors.New("not a URL: want nats://")
	}
	switch strings.ToLower(scheme) {
	case "nats":
		return parseNATSURL(s)
	case "tls":
		return ebTarget{}, errors.New("tls:// demands TLS, which go-webhook-nats-server does not " +
			"support (the connection to NATS is always plaintext): use nats:// over a trusted network")
	}
	shown := "an unrecognised scheme"
	if schemeRE.MatchString(scheme) {
		shown = strconv.Quote(strings.ToLower(scheme) + "://")
	}
	return ebTarget{}, fmt.Errorf("%s is not supported: go-webhook-nats-server only publishes to "+
		"NATS, so the eventbus must be nats://", shown)
}

// parseURL is url.Parse without any of the URL in the error. url.Error quotes
// the whole input, and its inner error still quotes a piece of it: an
// unescaped '/' in a password gives `invalid port ":Sup3r" after host`, which
// is the password up to the slash ('#', '?' and the ',' of a list cut it the
// same way), and an EscapeError quotes the escape. So nothing of the parser's
// text is kept, only its kind.
func parseURL(s string) (*url.URL, error) {
	if slashedUserinfo(s) {
		// "nats://u:12/ss@h" parses: the authority ends at the first '/' (or
		// '?', '#'), so the host is "u:12" (12 a port), the password's tail
		// lands in the path, query or fragment, and a piece of the password
		// surfaces later in a dial error or the dump. None of the URL is
		// quoted here either (go-relplog-server, SPEC §16.2).
		return nil, errors.New("not a valid URL: an '@' after the first '/', '?' or '#' means one of " +
			"them in the user or password that is not percent-encoded (%2F, %3F, %23)")
	}
	u, err := url.Parse(s)
	if err != nil {
		var ee url.EscapeError
		if errors.As(err, &ee) {
			if strings.Contains(authorityHost(s), "%") {
				// Go's parser refuses an escape in a host, and "invalid
				// percent-escape" sent the operator after the password
				// (independent review of go-relplog-pg-server, 2026-10-13).
				return nil, errors.New("not a valid URL: a percent-encoded host is not supported; " +
					"a NATS host is a name or an IP literal")
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
// fragment. No nats:// URL holds a literal '@' there (it has no path, query
// or fragment at all), so it can only be a userinfo that an unescaped '/', '?'
// or '#' in the password cut short: the authority then ends early, the user
// name becomes the host and the start of the password the port
// ("nats://app:39182#Zsecret9@nats-1" would dial app:39182 with no password).
func slashedUserinfo(s string) bool {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return false
	}
	end := strings.IndexAny(rest, "/?#")
	return end >= 0 && strings.Contains(rest[end:], "@")
}

// parseNATSURL fills sink.nats.urls with the URL itself (user and password
// included, which the client reads from there). A comma list is accepted
// because that is how nats.go spells a cluster, and it is what sink.nats.urls
// already is; every entry must be nats://, or part of the cluster would be
// dialled with a handshake this server does not do.
func parseNATSURL(s string) (ebTarget, error) {
	parts := parseList(s)
	for _, part := range parts {
		u, err := parseURL(part)
		if err != nil {
			return ebTarget{}, fmt.Errorf("%w (%s)", err, percentEncodeHint)
		}
		if !strings.EqualFold(u.Scheme, "nats") {
			return ebTarget{}, errors.New("a NATS URL list must use nats:// in every entry (tls:// is refused: there is no TLS)")
		}
		if u.Hostname() == "" {
			return ebTarget{}, errors.New("a NATS URL needs a host")
		}
		// An empty '#' or '?' counts: "nats://h:4222#" parses with no
		// fragment, but a NATS URL has none at all, and the character is
		// most likely what is left of a value cut short (ported from the
		// syslog/RELP family, 2026-10-13).
		if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
			strings.Contains(part, "#") {
			return ebTarget{}, errors.New("a NATS URL carries no path, query or fragment (" + percentEncodeHint + ")")
		}
	}
	return ebTarget{
		values: []ebValue{{"sink.nats.urls", strings.Join(parts, ",")}},
		owned:  []string{"sink.nats.urls", "sink.nats.username", "sink.nats.password", "sink.nats.token"},
	}, nil
}

// redactURL replaces the userinfo of every URL in a comma list. A password is
// replaced alone (user:***); a bare user part may be a NATS token, so it is
// replaced whole.
//
// It works on the text, not through net/url, on purpose. url.URL.String()
// would print "%2A%2A%2A", and -- worse -- a password may legally hold a comma
// or a slash, which a parser splits on: a comma-split "nats://u:pa,ss@h"
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
