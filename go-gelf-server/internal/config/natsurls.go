package config

import (
	"fmt"
	"strconv"
	"strings"
)

// validateNATSURLs holds the sink.nats.urls rules of SPEC §10.7 and §13.2
// (pendência 13 de 2026-10-11, ported from go-relplog-server's validateNATS).
// It uses parseURL and percentEncodeHint from eburl.go; parseURL's text
// quotes nothing of its input, so the message stays free of the entry.
func validateNATSURLs(n *SinkNATS) error {
	// Each entry is parsed here, and never quoted in the message: a password
	// with an unescaped %, / or # failed inside nats.Connect as a network
	// error, retried for the whole sink.boot_timeout with the URL -- or the
	// piece of it url.Parse quotes -- in every log line (SPEC §10.7, §13.2;
	// pendência 13 de 2026-10-11, found in go-syslog-nats-server and fixed
	// in go-relplog-server).
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
			// The scheme is named only when it has the shape of one
			// (schemeOf): a mangled value may hold a secret there.
			return fmt.Errorf("sink.nats.urls: an entry has the scheme %s; nats.go dials only "+
				"nats://, tls://, ws:// and wss://", schemeOf(raw))
		}
		if u.Hostname() == "" {
			return fmt.Errorf("sink.nats.urls: an entry has no host (%s)", percentEncodeHint)
		}
		// The rule of LOGHUB_EB_URL (SPEC §10.9). It is also what catches the
		// secret that parses: nats://user:1234/5678@host is host "user", port
		// 1234 and a path, with no credentials at all, and nats://tok/en@host
		// is host "tok" -- the client's dial error quoted that host, the token
		// up to the slash, in every log line until sink.boot_timeout. A
		// websocket URL may carry a path to the server's endpoint, so the rule
		// is the one of nats:// and tls://.
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
	// below. LOGHUB_EB_URL refuses the same conflict for the keys it owns;
	// the plain keys, and the .creds file it does not own, must be refused too.
	if userinfo && (n.Username != "" || n.Password != "" || n.Token != "" || n.Credentials != "") {
		return fmt.Errorf("sink.nats.urls carries credentials (user:password@ or token@) and " +
			"sink.nats.username/password/token/credentials is set too: the client would use the " +
			"URL's and ignore the key; keep one of them")
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
