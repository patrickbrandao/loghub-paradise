package syslog

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCorpus runs every message in testdata/corpus against its expected fields
// (SPEC §17.3).
//
// Invented messages cover the grammar; real ones cover what vendors actually do
// with it, which is something else. Three bugs in this parser were found by
// adding entries here and not by any invented test: the missing hostname of
// syslog(3) on /dev/log, rsyslog's RFC 3339 timestamp inside an RFC 3164 line,
// and the Cisco IOS sequence number that became an app-name.
//
// A "want" file holds key=|value| lines. The bars make leading and trailing
// spaces visible, which matters: "sudo:  patrick" keeps one of its two spaces.
func TestCorpus(t *testing.T) {
	raws, err := filepath.Glob(filepath.Join("testdata", "corpus", "*.raw"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raws) < 20 {
		t.Fatalf("corpus has %d entries; it should never shrink below its original twenty", len(raws))
	}

	now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	var p Parsed

	for _, rawPath := range raws {
		name := strings.TrimSuffix(filepath.Base(rawPath), ".raw")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(rawPath)
			if err != nil {
				t.Fatal(err)
			}
			want := readWant(t, strings.TrimSuffix(rawPath, ".raw")+".want")

			pri, hdr, ok := PRI(raw)
			if !ok {
				pri, hdr = 13, 0
			}
			Parse(&p, raw, hdr, now, Options{})

			got := map[string]string{
				"rfc":           p.RFC.String(),
				"pri":           strconv.Itoa(pri),
				"facility":      strconv.Itoa(pri / 8),
				"severity":      strconv.Itoa(pri % 8),
				"hostname":      string(p.Hostname),
				"app-name":      string(p.AppName),
				"procid":        string(p.ProcID),
				"msgid":         string(p.MsgID),
				"msg-ts-source": p.MsgTSSrc.String(),
				"msg":           string(raw[p.MsgOffset : p.MsgOffset+p.MsgSize]),
				"pri-source":    map[bool]string{true: "wire", false: "default"}[ok],
			}
			for _, el := range p.SD {
				for _, prm := range el.Params {
					got["sd:"+string(el.ID)+":"+string(prm.Name)] = string(prm.Value)
				}
			}

			for k, w := range want {
				if g, present := got[k]; !present {
					t.Errorf("%s: expected %q, but the parse produced no such field", k, w)
				} else if g != w {
					t.Errorf("%s = %q, want %q", k, g, w)
				}
			}
		})
	}
}

func readWant(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("every .raw needs a .want beside it: %v", err)
	}
	defer f.Close()

	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok || len(v) < 2 || v[0] != '|' || v[len(v)-1] != '|' {
			t.Fatalf("%s: malformed line %q, want key=|value|", path, line)
		}
		out[k] = v[1 : len(v)-1]
	}
	return out
}
