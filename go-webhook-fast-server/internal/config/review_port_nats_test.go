package config

import (
	"errors"
	"strings"
	"testing"
)

// Porte das correções da família syslog/RELP (2026-10-13), area NATS: the
// sink.nats.urls rules of go-syslog-server's natsurls.go (from
// go-relplog-server), and the ceiling of ring.max_inflight that keeps the NATS
// PublishAsyncMaxPending an int (go-syslog-server review, C3).

func natsLoad(t *testing.T, args ...string) {
	t.Helper()
	if _, err := Load(Options{Args: append([]string{"--sink.type=nats"}, args...), LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("%v was refused: %v", args, err)
	}
}

func natsFail(t *testing.T, want string, args ...string) error {
	t.Helper()
	_, err := Load(Options{Args: append([]string{"--sink.type=nats"}, args...), LookupEnv: envMap(nil)})
	if err == nil {
		t.Errorf("%v was accepted", args)
		return errors.New("not refused")
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("%v: error %q does not mention %q", args, err, want)
	}
	return err
}

// SPEC §8.7.9: every entry of sink.nats.urls is parsed at validation, and no
// error quotes it. A scheme nats.go does not dial is refused at once; a
// websocket endpoint may carry a path; an '@' after the first '/', '?' or '#'
// is a password with one of them unescaped, and the message names the escape.
func TestNATSURLsFollowTheFamilyRules(t *testing.T) {
	// Controls: escaped credentials, a bare host:port (nats.go dials it as
	// nats://), a root path, a cluster, and a websocket endpoint with a path.
	for _, u := range []string{
		"nats://bob:s3cr%25t@n1:4222",
		"n1:4222",
		"n1:4222,n2:4222",
		"[::1]:4222",
		"nats://n1:4222/",
		"tls://n1:4222",
		"ws://n1:8080/nats",
		"wss://bob:s3cr%2Ft@n1:443/nats",
		"nats://user:1234%2F5678@n1:4222/",
	} {
		natsLoad(t, "--sink.nats.urls="+u)
	}

	for _, u := range []string{
		"nats://bob:Sup3rSecret/xyz@n1:4222",
		"nats://bob:Sup3rSecret#xyz@n1:4222",
		"nats://bob:Sup3rSecret?xyz@n1:4222",
		"nats://bob:Sup3rSecret%zz@n1:4222",
		"nats://bob:Sup3rSecret,xyz@n1:4222",
		"nats://Sup3rSecretToken/x@n1:4222",
		"tls://Sup3rSecretToken/x@n1:4222",
		"bob:Sup3rSecret/x@n1:4222",
		// A websocket path is allowed; a userinfo cut short by a '/' in it
		// is not.
		"ws://bob:Sup3rSecret/xyz@n1:8080/nats",
		"wss://Sup3rSecretToken/x@n1:443",
	} {
		err := natsFail(t, "sink.nats.urls", "--sink.nats.urls="+u)
		if strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// The secret that parses: a password that reads as a port and a path, or
	// a query or a fragment. No credentials at all, dialled until
	// boot_timeout.
	for _, u := range []string{"tls://n1:4222/x", "nats://n1:4222?x=1", "nats://n1:4222#f",
		"nats://user:1234/5678@n1:4222", "nats://user:1234#5678@n1:4222", "nats://user:1234?5678@n1:4222"} {
		err := natsFail(t, "sink.nats.urls", "--sink.nats.urls="+u)
		if strings.Contains(err.Error(), "5678") || strings.Contains(err.Error(), "1234") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// The '@' after a '/', '?' or '#' gets the escapes to use.
	for _, u := range []string{"nats://bob:Sup3rSecret/xyz@n1:4222", "ws://bob:Sup3rSecret/xyz@n1:8080/nats"} {
		if err := natsFail(t, "sink.nats.urls", "--sink.nats.urls="+u); !strings.Contains(err.Error(), "%2F") {
			t.Errorf("%q = %v, want the %%2F hint", u, err)
		}
	}
	// A percent-escape in the host is about the host, not the password.
	if err := natsFail(t, "percent-encoded host", "--sink.nats.urls=nats://bob:pw@n%31:4222"); strings.Contains(err.Error(), "pw") {
		t.Errorf("the password is echoed: %v", err)
	}
	natsFail(t, "no host", "--sink.nats.urls=nats://:4222")
	natsFail(t, "no host", "--sink.nats.urls=nats://u:p@")
	// A scheme nats.go does not dial: named when it has the shape of one,
	// never echoed when it does not.
	natsFail(t, `"http://"`, "--sink.nats.urls=http://n1:4222")
	natsFail(t, `"nast://"`, "--sink.nats.urls=nast://n1:4222")
	if err := natsFail(t, "sink.nats.urls", "--sink.nats.urls=Sup3r$ecret://n1:4222"); strings.Contains(err.Error(), "Sup3r") {
		t.Errorf("the scheme of a mangled URL is echoed: %v", err)
	}
	if got := schemeOf("Sup3r$ecret://n1"); got != "unrecognised" {
		t.Errorf("schemeOf of a mangled scheme = %s", got)
	}

	// Credentials in the URL and in a key: the client would silently use the
	// URL's.
	for _, key := range []string{"--sink.nats.username=other", "--sink.nats.token=x",
		"--sink.nats.credentials=/etc/x.creds"} {
		natsFail(t, "carries credentials", "--sink.nats.urls=nats://u:p@n1:4222", key)
		natsFail(t, "carries credentials", "--sink.nats.urls=tok@n1:4222", key)
	}
	// Controls: one place at a time; and the URLs are a NATS matter only.
	natsLoad(t, "--sink.nats.urls=nats://n1:4222", "--sink.nats.username=u", "--sink.nats.password=p")
	if _, err := Load(Options{Args: []string{"--sink.type=redis", "--sink.nats.urls=nats://u:p/x@n1"},
		LookupEnv: envMap(nil)}); err != nil {
		t.Errorf("sink.nats.urls checked with sink.type=redis: %v", err)
	}
}

// SPEC §8.7.2, §13.5: ring.max_inflight is one term of the NATS
// PublishAsyncMaxPending, 2 × (workers × batch + max_inflight); unbounded, a
// typo overflowed it into a negative number that the sink silently replaced
// with 4096. The ceiling is 1 Mi; 1 Mi itself is the control.
func TestRingMaxInflightHasACeiling(t *testing.T) {
	if _, err := Load(Options{Args: []string{"--ring.max_inflight=1048576"}, LookupEnv: envMap(nil)}); err != nil {
		t.Fatalf("control: ring.max_inflight at the ceiling: %v", err)
	}
	for _, v := range []string{"1048577", "4611686018427387904", "0"} {
		_, err := Load(Options{Args: []string{"--ring.max_inflight=" + v}, LookupEnv: envMap(nil)})
		if err == nil || !strings.Contains(err.Error(), "ring.max_inflight") {
			t.Errorf("ring.max_inflight=%s: %v, want a refusal naming the key", v, err)
		}
	}
}
