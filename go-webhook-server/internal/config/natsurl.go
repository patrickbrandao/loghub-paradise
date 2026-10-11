package config

import (
	"errors"
	"net/url"
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
