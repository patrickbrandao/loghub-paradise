package verify

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// target is a --verify-url split and parsed before any driver sees it (SPEC
// §8.6.1). The drivers' own parse errors quote the URL whole, password
// included (nats.go: `parse "nats://u:sekret@..."`), and a NATS list handed
// to nats.go unparsed leaked its second element the same way (review of
// 2026-11-07). Every element is parsed here, the errors say only which
// element and why, and the passwords of every element are kept to scrub
// whatever a driver says later.
type target struct {
	kind    string // nats | postgres | redis
	raw     string // what the driver gets: the elements as given, joined by ","
	first   *url.URL
	secrets []string
}

// parseTarget parses a --verify-url. Errors never quote it.
func parseTarget(raw string) (*target, error) {
	scheme, _, ok := strings.Cut(raw, "://")
	if !ok {
		return nil, errors.New("--verify-url: want scheme://...")
	}
	t := &target{}
	switch scheme {
	case "nats", "tls":
		t.kind = "nats"
	case "postgres", "postgresql":
		t.kind = "postgres"
	case "redis", "rediss", "valkey", "valkeys":
		t.kind = "redis"
	default:
		// The scheme is not a secret; the rest of the URL may be.
		return nil, fmt.Errorf("--verify-url: scheme %q is none of nats, tls, postgres, postgresql, redis, rediss, valkey, valkeys", scheme)
	}
	// Only NATS takes a list of servers (as nats.go does); a PostgreSQL
	// multi-host URL keeps its commas inside the one authority.
	elems := []string{raw}
	if t.kind == "nats" {
		elems = strings.Split(raw, ",")
	}
	clean := make([]string, 0, len(elems))
	for i, e := range elems {
		e = strings.TrimSpace(e)
		u, err := url.Parse(e)
		if err != nil {
			// Never the *url.Error text: it quotes the element.
			return nil, fmt.Errorf("--verify-url %s: server %d of the list is not a URL", t.kind, i+1)
		}
		es := strings.ToLower(u.Scheme)
		if t.kind == "nats" && es != "nats" && es != "tls" {
			return nil, fmt.Errorf("--verify-url nats: server %d of the list is not nats:// or tls://", i+1)
		}
		if u.Host == "" && !(t.kind == "postgres" && u.Query().Has("host")) {
			return nil, fmt.Errorf("--verify-url %s: server %d of the list has no host", t.kind, i+1)
		}
		if p, ok := u.User.Password(); ok && p != "" {
			t.secrets = append(t.secrets, p)
		} else if t.kind == "nats" && u.User != nil && u.User.Username() != "" {
			t.secrets = append(t.secrets, u.User.Username()) // a NATS token goes as the user
		}
		if p := u.Query().Get("password"); p != "" {
			t.secrets = append(t.secrets, p) // libpq's password= parameter
		}
		if i == 0 {
			t.first = u
		}
		clean = append(clean, e)
	}
	t.raw = strings.Join(clean, ",")
	return t, nil
}

// scrub replaces every password of the target in a driver's error text.
func (t *target) scrub(s string) string {
	for _, p := range t.secrets {
		for _, f := range []string{p, url.QueryEscape(p), url.PathEscape(p), url.UserPassword("", p).String()[1:]} {
			if f != "" {
				s = strings.ReplaceAll(s, f, "xxxxx")
			}
		}
	}
	return s
}

// wrap is a driver error fit to print: a *url.Error (which quotes a URL)
// becomes a generic text, and any password left is scrubbed.
func (t *target) wrap(prefix string, err error) error {
	if err == nil {
		return nil
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: a server URL does not parse", prefix)
	}
	return fmt.Errorf("%s: %s", prefix, t.scrub(err.Error()))
}
