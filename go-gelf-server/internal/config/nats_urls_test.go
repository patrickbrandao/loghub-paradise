package config

import (
	"strings"
	"testing"
)

// SPEC §22 pendência 13 (ported from go-relplog-server): every entry of
// sink.nats.urls is parsed at validation, and no error quotes it; credentials
// in the URL and in a key are refused together.
func TestNATSURLsAreParsedWithoutEcho(t *testing.T) {
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
		"nats://user:1234%2F5678@n1:4222/",
	} {
		mustLoad(t, []string{"--sink.nats.urls=" + u}, nil)
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
	} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "sink.nats.urls")
		if strings.Contains(err.Error(), "Sup3r") || strings.Contains(err.Error(), "zz") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v, want a refusal that quotes nothing of the URL and says what to escape", u, err)
		}
	}
	// The secret that parses: a password that reads as a port and a path, or a
	// query or a fragment. No credentials at all, dialled until boot_timeout.
	for _, u := range []string{"tls://n1:4222/x", "nats://n1:4222?x=1", "nats://n1:4222#f",
		"nats://user:1234/5678@n1:4222", "nats://user:1234#5678@n1:4222", "nats://user:1234?5678@n1:4222"} {
		err := mustFail(t, []string{"--sink.nats.urls=" + u}, nil, "sink.nats.urls")
		if strings.Contains(err.Error(), "5678") || strings.Contains(err.Error(), "1234") ||
			!strings.Contains(err.Error(), "percent-encode") {
			t.Errorf("%q: err = %v", u, err)
		}
	}
	// The '@' after a '/' gets the escapes to use.
	if err := mustFail(t, []string{"--sink.nats.urls=nats://bob:Sup3rSecret/xyz@n1:4222"}, nil, "sink.nats.urls"); !strings.Contains(err.Error(), "%2F") {
		t.Errorf("a '/' in the password = %v, want the %%2F hint", err)
	}
	mustFail(t, []string{"--sink.nats.urls=nats://:4222"}, nil, "no host")
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@"}, nil, "no host")
	mustFail(t, []string{"--sink.nats.urls=http://n1:4222"}, nil, `"http://"`)
	if err := mustFail(t, []string{"--sink.nats.urls=Sup3r$ecret://n1:4222"}, nil, "sink.nats.urls"); strings.Contains(err.Error(), "Sup3r") {
		t.Errorf("the scheme of a mangled URL is echoed: %v", err)
	}
	if got := schemeOf("Sup3r$ecret://n1"); got != "unrecognised" {
		t.Errorf("schemeOf of a mangled scheme = %s", got)
	}

	// Credentials in the URL and in a key: the client would silently use the
	// URL's. Every key, the .creds file included, and through both doors.
	for _, key := range []string{"--sink.nats.username=other", "--sink.nats.token=x",
		"--sink.nats.credentials=/etc/x.creds"} {
		mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", key}, nil, "carries credentials")
		mustFail(t, []string{"--sink.nats.urls=tok@n1:4222", key}, nil, "carries credentials")
	}
	mustFail(t, []string{"--sink.nats.urls=nats://u:p@n1:4222", "--sink.nats.username=u",
		"--sink.nats.password=p"}, nil, "carries credentials")
	mustFail(t, []string{"--sink.nats.credentials=/etc/x.creds"},
		map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"}, "carries credentials")
	// Controls: one place at a time.
	mustLoad(t, []string{"--sink.nats.urls=nats://n1:4222", "--sink.nats.username=u", "--sink.nats.password=p"}, nil)
	mustLoad(t, []string{"--sink.nats.credentials=/etc/x.creds"}, map[string]string{"LOGHUB_EB_URL": "nats://n1:4222"})
	mustLoad(t, nil, map[string]string{"LOGHUB_EB_URL": "nats://u:p@n1:4222"})
	// The URLs are a NATS matter only.
	mustLoad(t, []string{"--sink.type=redis", "--sink.nats.urls=nats://u:p/x@n1"}, nil)
}
