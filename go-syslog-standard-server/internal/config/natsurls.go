package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// validateNATSURLs holds the sink.nats.urls rules of SPEC §10.8.9 and §15.1
// (pendência 4 de §21.1, ported from go-syslog-server's natsurls.go).
//
// Each entry is parsed here, and never quoted in the message: a password with
// an unescaped %, / or # failed inside nats.Connect as a network error,
// retried for the whole sink.boot_timeout with the URL -- or the piece of it
// url.Parse quotes -- in every log line.
func validateNATSURLs(n *SinkNATS) error {
	userinfo := false
	for _, raw := range n.URLs {
		raw = strings.TrimSpace(raw)
		if !strings.Contains(raw, "://") {
			// nats.go dials a bare host:port as nats://.
			raw = "nats://" + raw
		}
		u, err := parseURL(raw)
		if err != nil {
			return fmt.Errorf("sink.nats.urls: an entry is %v (%s)", err, percentEncodeHint)
		}
		scheme := strings.ToLower(u.Scheme)
		switch scheme {
		case "nats", "tls", "ws", "wss":
		default:
			// The scheme is named only when it has the shape of one: a
			// mangled value may hold a secret there.
			return fmt.Errorf("sink.nats.urls: an entry has the scheme %s; nats.go dials only "+
				"nats://, tls://, ws:// and wss://", schemeOf(raw))
		}
		if u.Hostname() == "" {
			return fmt.Errorf("sink.nats.urls: an entry has no host (%s)", percentEncodeHint)
		}
		// This is also what catches the secret that parses:
		// nats://user:1234/5678@host is host "user", port 1234 and a path,
		// with no credentials at all, and nats://tok/en@host is host "tok" --
		// the client's dial error quoted that host, the token up to the slash,
		// in every log line until sink.boot_timeout. A websocket URL may carry
		// a path to the server's endpoint, so the rule is the one of nats://
		// and tls://.
		if scheme == "nats" || scheme == "tls" {
			if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
				return fmt.Errorf("sink.nats.urls: an entry carries a path, a query or a fragment, "+
					"which a NATS URL has none of (%s)", percentEncodeHint)
			}
		}
		if u.User != nil {
			userinfo = true
		}
	}
	// nats.go prefers the URL's userinfo over every explicit credential: the
	// keys would be accepted and silently ignored, against "one method only"
	// of SPEC §10.8.9.
	if userinfo && (n.Username != "" || n.Password != "" || n.Token != "" || n.Credentials != "") {
		return errors.New("sink.nats.urls carries credentials (user:password@ or token@) and " +
			"sink.nats.username/password/token/credentials is set too: the client would use the " +
			"URL's and ignore the key; keep one of them")
	}
	return nil
}

// schemeRE is the shape of a URL scheme (RFC 3986 §3.1), bounded.
var schemeRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]{0,31}$`)

// schemeOf names the scheme of a URL for an error message, or says why it
// does not: anything that is not shaped like a scheme may be a secret typed
// in the wrong place.
func schemeOf(u string) string {
	scheme, _, ok := strings.Cut(u, "://")
	if !ok {
		return "none"
	}
	if !schemeRE.MatchString(scheme) {
		return "unrecognised"
	}
	return strconv.Quote(strings.ToLower(scheme) + "://")
}

// percentEncodeHint is how a URL that does not parse is explained without
// quoting it (SPEC §15.1).
const percentEncodeHint = "percent-encode %, /, #, ? and , in a user, password or token"

// parseURL is url.Parse without any of the URL in the error. url.Error quotes
// the whole input, and its inner error still quotes a piece of it: an
// unescaped '/' in a password gives `invalid port ":Sup3r" after host`, which
// is the password up to the slash, and an EscapeError quotes the escape. So
// nothing of the parser's text is kept, only its kind.
func parseURL(s string) (*url.URL, error) {
	if slashedUserinfo(s) {
		return nil, errors.New("not a valid URL: an '@' after the first '/', '?' or '#' means one of " +
			"them in the user or password that is not percent-encoded (%2F, %3F, %23)")
	}
	u, err := url.Parse(s)
	if err != nil {
		var ee url.EscapeError
		if errors.As(err, &ee) {
			return nil, errors.New("not a valid URL: it holds an invalid percent-escape")
		}
		return nil, errors.New("not a valid URL")
	}
	return u, nil
}

// slashedUserinfo reports an '@' past the end of the authority: after the
// first '/', '?' or '#' that follows "://". No NATS URL holds a literal '@'
// there, so it can only be a userinfo that an unescaped '/', '?' or '#' in the
// password cut short: the authority then ends early, the user name becomes
// the host and the start of the password the port.
func slashedUserinfo(s string) bool {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return false
	}
	end := strings.IndexAny(rest, "/?#")
	return end >= 0 && strings.Contains(rest[end:], "@")
}

// redactURL replaces the userinfo of every URL in a comma list. A password is
// replaced alone (user:***); a bare user part may be a NATS token, so it is
// replaced whole.
//
// It works on the text, not through net/url, on purpose. url.URL.String()
// would print "%2A%2A%2A", and -- worse -- a password may legally hold a comma
// or a slash, which a parser splits on: a comma-split "nats://u:pa,ss@h" would
// print "pa" in clear. So a segment without "://" is glued back onto the
// previous one, everything up to the LAST '@' is userinfo, and a value that
// does not even start with a scheme is replaced entirely when it holds an
// '@'. Over-redaction is the safe failure.
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
		// A bare host:port (nats.go dials it as nats://) is shown; anything
		// with an '@' may be a credential and is not.
		if strings.Contains(s, "@") {
			return "***"
		}
		return s
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
