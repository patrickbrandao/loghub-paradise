package rediscli

import (
	"context"
	"errors"
	"testing"
)

func TestClassifyReply(t *testing.T) {
	cases := map[string]Class{
		"OOM command not allowed when used memory > 'maxmemory'": Fatal,
		"WRONGTYPE Operation against a key":                      Fatal,
		"NOPERM this user has no permissions":                    Fatal,
		"NOAUTH Authentication required":                         Fatal,
		"WRONGPASS invalid username-password pair":               Fatal,
		"READONLY You can't write against a read only replica":   Network,
		"LOADING Redis is loading the dataset in memory":         Network,
		"BUSY Redis is busy running a script":                    Network,
		"MASTERDOWN Link with MASTER is down":                    Network,
		"CLUSTERDOWN The cluster is down":                        Network,
		"TRYAGAIN Multiple keys request during rehashing":        Network,
		"ERR something nobody planned for":                       Network,
	}
	for msg, want := range cases {
		if got := ClassifyReply(msg); got != want {
			t.Errorf("%q: %d want %d", msg, got, want)
		}
	}
	if Classify(context.DeadlineExceeded) != Network || Classify(errors.New("dial tcp: refused")) != Network {
		t.Error("connection errors must be network")
	}
}

func TestSameServer(t *testing.T) {
	a := Config{Addrs: []string{"10.0.0.1:6379", "10.0.0.2:6379"}}
	b := Config{Addrs: []string{"10.0.0.2:6379 ", "10.0.0.1:6379"}}
	if !SameServer(a, b) {
		t.Error("same set in another order")
	}
	b.DB = 1
	if SameServer(a, b) {
		t.Error("another db is another server for this purpose")
	}
}
