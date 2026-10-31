package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The live-emitter suite of SPEC §18.7.
//
// The corpus proves the converter against real DOCUMENTS; this proves the
// wire against a real EMITTER, which is where incompatibilities live: GELF
// chunking and gzip on UDP and the \0 framing on TCP and TLS, as fluent-bit's
// gelf output produces them.
//
// fluent-bit's http output in gelf format is NOT here: it sends every record
// of a flush in ONE request, newline-separated, and SPEC §4.5 takes one
// document per POST /gelf -- the batch arrives as one malformed document
// (found by this suite, 2026-10-31; a SPEC question, not a test to keep red).
// It needs Docker (fluent/fluent-bit), so it runs only when asked:
//
//	GELF_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/

// fluentBitImage is the emitter. A pinned tag: a test that changed with the
// image would prove nothing about this server.
const fluentBitImage = "fluent/fluent-bit:4.0.3"

func requireEmitters(t *testing.T) {
	t.Helper()
	if os.Getenv("GELF_TEST_EMITTERS") == "" {
		t.Skip("set GELF_TEST_EMITTERS=1 to run against a real fluent-bit in Docker")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
}

// publicPort binds 0.0.0.0 on a free port and releases it: a container reaches
// the host through host.docker.internal, which does not arrive on loopback
// everywhere.
func publicPort(t *testing.T, network string) int {
	t.Helper()
	var addr string
	if network == "udp" {
		c, err := net.ListenPacket("udp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		addr = c.LocalAddr().String()
		c.Close()
	} else {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		addr = l.Addr().String()
		l.Close()
	}
	p, _ := strconv.Atoi(portOf(t, addr))
	return p
}

type emitterStack struct {
	*instance
	udp, tcp, tls int
	pki           *pki
}

func startForEmitters(t *testing.T) *emitterStack {
	t.Helper()
	s := &emitterStack{
		udp: publicPort(t, "udp"), tcp: publicPort(t, "tcp"), tls: publicPort(t, "tcp"),
		pki: newPKI(t, "host.docker.internal"),
	}
	s.instance = start(t, t.TempDir(),
		"--listen.udp=0.0.0.0:"+strconv.Itoa(s.udp),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(s.tcp),
		"--listen.tls=0.0.0.0:"+strconv.Itoa(s.tls),
		"--tls.cert_file="+s.pki.serverCert, "--tls.key_file="+s.pki.serverKey,
		// Every container arrives from the same gateway address.
		"--ratelimit.enabled=false",
	)
	return s
}

// byTransport waits until want is reached and returns the envelopes carrying
// marker, grouped by transport. Every envelope is validated with the oracle on
// the way.
func (s *emitterStack) byTransport(marker string, want map[string]int) map[string][]jlhEnv {
	s.t.Helper()
	var got map[string][]jlhEnv
	eventually(90*time.Second, func() bool {
		got = map[string][]jlhEnv{}
		if sqliteCount(s.t, s.eventsDB(), "gelf_events") <= 0 {
			return false
		}
		for _, r := range sqliteRows(s.t, s.eventsDB()) {
			if !strings.Contains(string(r.Env), marker) {
				continue
			}
			e := checkRow(s.t, r)
			got[e.Payload.Transport] = append(got[e.Payload.Transport], e)
		}
		for k, n := range want {
			if len(got[k]) < n {
				return false
			}
		}
		return true
	})
	return got
}

// runFluentBit starts fluent-bit with conf (and the files beside it) in a
// detached container that can reach the host, and removes it when the test
// ends; its log is printed when the test failed.
func runFluentBit(t *testing.T, name, conf string, files map[string]string) {
	t.Helper()
	dir := t.TempDir()
	files["fluent-bit.conf"] = conf
	for f, body := range files {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name,
		"--add-host", "host.docker.internal:host-gateway",
		"-v", dir+":/fluent-bit/etc:ro",
		fluentBitImage, "/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf").CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Logf("fluent-bit log:\n%s", logs)
		}
		exec.Command("docker", "rm", "-f", name).Run()
	})
}

// blobLua adds a random field large enough that the document, even gzipped,
// passes Packet_Size and leaves in several GELF chunks. Random on purpose: a
// repeated text would compress below one chunk and prove nothing.
const blobLua = `
local alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
math.randomseed(os.time())
function add_blob(tag, timestamp, record)
    local parts = {}
    for i = 1, 12000 do
        local n = math.random(1, #alphabet)
        parts[i] = alphabet:sub(n, n)
    end
    record["blob"] = table.concat(parts)
    return 1, timestamp, record
end
`

// fluent-bit's gelf output over UDP (gzip, chunked by a 1420-byte
// Packet_Size), TCP and TLS (\0 framing, the server verified by name). Every
// document arrives exactly once, whole, through the transport it was sent on.
func TestEmittersFluentBit(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)

	const n = 5
	marker := fmt.Sprintf("fluentbit-%d", time.Now().UnixNano())
	input := func(tag string, level int) string {
		return fmt.Sprintf(`
[INPUT]
    Name         dummy
    Tag          %s
    Dummy        {"short_message":"%s %s","level":%d,"host":"fluent-bit","origin":"%s"}
    Samples      %d
    Rate         5
`, tag, marker, tag, level, tag, n)
	}
	gelfOut := func(tag, mode string, port int, extra string) string {
		return fmt.Sprintf(`
[OUTPUT]
    Name                    gelf
    Match                   %s
    Host                    host.docker.internal
    Port                    %d
    Mode                    %s
    Gelf_Short_Message_Key  short_message
    Gelf_Host_Key           host
    Gelf_Level_Key          level
%s`, tag, port, mode, extra)
	}
	conf := "[SERVICE]\n    Flush        1\n    Log_Level    info\n" +
		input("udp", 6) + input("tcp", 5) + input("tls", 4) + `
[FILTER]
    Name         lua
    Match        udp
    Script       /fluent-bit/etc/blob.lua
    Call         add_blob
` +
		gelfOut("udp", "udp", s.udp, "    Compress                On\n    Packet_Size             1420\n") +
		gelfOut("tcp", "tcp", s.tcp, "") +
		gelfOut("tls", "tls", s.tls, "    tls                     On\n    tls.verify              On\n"+
			"    tls.ca_file             /fluent-bit/etc/ca.pem\n    tls.vhost               host.docker.internal\n")
	runFluentBit(t, "gls-emit-fluentbit-"+strconv.Itoa(os.Getpid()), conf,
		map[string]string{"blob.lua": blobLua, "ca.pem": s.pki.caPEM(t)})

	want := map[string]int{"udp": n, "tcp": n, "tls": n}
	got := s.byTransport(marker, want)
	for k, w := range want {
		if len(got[k]) != w {
			t.Errorf("%s: %d envelopes, want %d", k, len(got[k]), w)
		}
	}
	level := map[string]int{"udp": 6, "tcp": 5, "tls": 4}
	for k, envs := range got {
		for _, e := range envs {
			p := e.Payload
			if p.Format != "gelf" || p.message() != marker+" "+k || show(p.hostname()) != "fluent-bit" ||
				e.Level != level[k] || p.Fields["origin"] != k {
				t.Errorf("%s: parsed as %s", k, e.bytes)
				continue
			}
			switch k {
			case "udp":
				if show(p.Compression) != "gzip" || p.Chunks == nil || *p.Chunks < 2 {
					t.Errorf("udp: compression %s chunks %s, want gzip in several chunks", show(p.Compression), show(p.Chunks))
				}
				if blob, _ := p.Fields["blob"].(string); len(blob) != 12000 || p.Size < 12000 {
					t.Errorf("udp: blob of %d characters, size %d: the reassembled document is not whole", len(blob), p.Size)
				}
			case "tcp":
				if p.Compression != nil || p.Chunks != nil || p.TLS != nil {
					t.Errorf("%s: %s", k, e.bytes)
				}
			case "tls":
				if p.TLS == nil || p.TLS.Version == "" {
					t.Errorf("tls: no session in the payload: %s", e.bytes)
				}
			}
		}
	}
	for series, min := range map[string]int{
		`gelf_messages_dropped_total{reason="chunk_invalid"}`: 0,
		`gelf_messages_dropped_total{reason="decompress"}`:    0,
		`gelf_messages_dropped_total{reason="too_large"}`:     0,
	} {
		if v := s.metric(series); v != strconv.Itoa(min) {
			t.Errorf("%s = %s", series, v)
		}
	}
}
