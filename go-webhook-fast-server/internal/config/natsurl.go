package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// percentEncodeHint is what a URL refused for its shape usually needs.
const percentEncodeHint = "percent-encode %, /, #, ? and , in a user, password or token"

// parseURL is url.Parse without any of the URL in the error. url.Error quotes
// the whole input, and its inner error still quotes a piece of it: an
// unescaped '/' in a password gives `invalid port ":Sup3r" after host`, which
// is the password up to the slash. So nothing of the parser's text is kept
// (SPEC §8.7.9; found in go-webhook-nats-server).
func parseURL(s string) (*url.URL, error) {
	if slashedUserinfo(s) {
		// "nats://u:12/ss@h" parses: the authority ends at the first '/' (or
		// '?', '#'), so the host is "u:12" (12 a port), the password's tail
		// lands in the path, query or fragment, and a piece of the password
		// surfaces later in a dial error or the dump. None of the URL is
		// quoted here either (go-relplog-server, porte da família syslog/RELP,
		// 2026-10-13).
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
// fragment. No NATS URL holds a literal '@' there (a WebSocket endpoint
// neither), so it can only be a userinfo that an unescaped '/', '?' or '#' in
// the password cut short: the authority then ends early, the user name becomes
// the host and the start of the password the port.
func slashedUserinfo(s string) bool {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return false
	}
	end := strings.IndexAny(rest, "/?#")
	return end >= 0 && strings.Contains(rest[end:], "@")
}

// schemeRE is what a URL scheme may look like (RFC 3986 §3.1). Anything else is
// reported as "unrecognised" instead of being echoed: a mangled value may hold
// a password where the scheme should be.
var schemeRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]{0,31}$`)

// validateNATSURLs holds the sink.nats.urls rules of SPEC §8.7.9, the ones of
// go-syslog-server's natsurls.go (porte da família syslog/RELP, 2026-10-13).
// Each entry is parsed here, and never quoted in the message: a password with
// an unescaped %, / or # failed inside nats.Connect as a network error,
// retried for the whole sink.boot_timeout with the URL -- or the piece of it
// url.Parse quotes -- in every log line (found in go-webhook-nats-server).
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
			// parseURL's text quotes nothing of the entry (see there), and
			// names %2F, %3F, %23 when an '@' follows a '/', '?' or '#'.
			return fmt.Errorf("sink.nats.urls: an entry is %v (%s)", err, percentEncodeHint)
		}
		scheme := strings.ToLower(u.Scheme)
		switch scheme {
		case "nats", "tls", "ws", "wss":
		default:
			// A scheme nats.go does not dial was accepted and failed only at
			// connect time, retried until sink.boot_timeout. The scheme is
			// named only when it has the shape of one (schemeOf): a mangled
			// value may hold a secret there.
			return fmt.Errorf("sink.nats.urls: an entry has the scheme %s; nats.go dials only "+
				"nats://, tls://, ws:// and wss://", schemeOf(raw))
		}
		if u.Hostname() == "" {
			return fmt.Errorf("sink.nats.urls: an entry has no host (%s)", percentEncodeHint)
		}
		// It is also what catches the secret that parses:
		// nats://user:1234/5678@host is host "user", port 1234 and a path,
		// with no credentials at all, and nats://tok/en@host is host "tok" --
		// the client would dial it until sink.boot_timeout. A websocket URL
		// may carry a path to the server's endpoint, so the rule is the one of
		// nats:// and tls://.
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
	// two together would leave the explicit one unused, silently.
	if userinfo && (n.Username != "" || n.Password != "" || n.Token != "" || n.Credentials != "") {
		return fmt.Errorf("sink.nats.urls carries credentials (user:password@ or token@) and " +
			"sink.nats.username/password/token/credentials is set too: the client would use the " +
			"URL's and ignore the other; keep one")
	}
	return nil
}

// schemeOf names the scheme of a URL for an error message, or says why it
// does not: anything that is not shaped like a scheme (RFC 3986 §3.1) may be
// a secret typed in the wrong place.
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

// urlKeys hold URLs that may carry credentials in their user part. They are
// printed with the userinfo masked (redactURL), never in full.
var urlKeys = map[string]bool{
	"sink.nats.urls": true,
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
// does not even start with a scheme keeps only what follows its last '@'.
// Over-redaction is the safe failure.
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
	scheme, rest := "", s
	if i := strings.Index(s, "://"); i >= 0 {
		scheme, rest = s[:i+3], s[i+3:]
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return s
	}
	red := "***"
	if user, _, hasPass := strings.Cut(rest[:at], ":"); hasPass {
		red = user + ":***"
	}
	return scheme + red + rest[at:]
}
