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
// chunking and gzip on UDP and the \0 framing on TCP, as fluent-bit's gelf
// output and the Docker daemon's gelf log driver produce them -- each document
// followed to the JetStream stream and validated by the oracle.
//
// fluent-bit's http output in gelf format is NOT here: it sends every record
// of a flush in ONE request, newline-separated, and SPEC §4.5 takes one
// document per POST /gelf -- the batch arrives as one malformed document
// (found by this suite, 2026-10-31; a SPEC question, not a test to keep red).
// It needs Docker (fluent/fluent-bit, alpine) and the test NATS, so it runs
// only when asked:
//
//	GELF_TEST_EMITTERS=1 GELF_TEST_NATS_URL=... go test -race -run Emitters -timeout 600s ./internal/app/

// fluentBitImage is the emitter. A pinned tag: a test that changed with the
// image would prove nothing about this server.
const fluentBitImage = "fluent/fluent-bit:4.0.3"

func requireEmitters(t *testing.T) {
	t.Helper()
	if os.Getenv("GELF_TEST_EMITTERS") == "" {
		t.Skip("set GELF_TEST_EMITTERS=1 to run against real emitters in Docker")
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
	f        *fixture
	udp, tcp int
}

func startForEmitters(t *testing.T) *emitterStack {
	t.Helper()
	s := &emitterStack{f: newFixture(t), udp: publicPort(t, "udp"), tcp: publicPort(t, "tcp")}
	s.instance = s.f.start(t.TempDir(),
		"--listen.udp=0.0.0.0:"+strconv.Itoa(s.udp),
		"--listen.tcp=0.0.0.0:"+strconv.Itoa(s.tcp),
	)
	return s
}

// byTransport waits until want is reached and returns the envelopes of the
// stream carrying marker, grouped by transport. Every message is validated with
// the oracle and its NATS headers on the way (checkMsg).
func (s *emitterStack) byTransport(marker string, want map[string]int) map[string][]jlhEnv {
	s.t.Helper()
	var got map[string][]jlhEnv
	eventually(90*time.Second, func() bool {
		got = map[string][]jlhEnv{}
		if s.f.count() <= 0 {
			return false
		}
		for _, m := range s.f.msgs() {
			if !strings.Contains(string(m.data), marker) {
				continue
			}
			e := checkMsg(s.t, m)
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
// Packet_Size) and TCP (\0 framing). Every document arrives exactly once,
// whole, through the transport it was sent on, and reaches the stream.
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
		input("udp", 6) + input("tcp", 5) + `
[FILTER]
    Name         lua
    Match        udp
    Script       /fluent-bit/etc/blob.lua
    Call         add_blob
` +
		gelfOut("udp", "udp", s.udp, "    Compress                On\n    Packet_Size             1420\n") +
		gelfOut("tcp", "tcp", s.tcp, "")
	runFluentBit(t, "gelf-nats-emit-fluentbit-"+strconv.Itoa(os.Getpid()), conf,
		map[string]string{"blob.lua": blobLua})

	want := map[string]int{"udp": n, "tcp": n}
	got := s.byTransport(marker, want)
	for k, w := range want {
		if len(got[k]) != w {
			t.Errorf("%s: %d envelopes, want %d", k, len(got[k]), w)
		}
	}
	level := map[string]int{"udp": 6, "tcp": 5}
	for k, envs := range got {
		for _, e := range envs {
			p := e.Payload
			if p.Format != "gelf" || p.message() != marker+" "+k || show(p.hostname()) != "fluent-bit" ||
				e.Level != level[k] || p.Fields["origin"] != k || p.TLS != nil {
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
				if p.Compression != nil || p.Chunks != nil {
					t.Errorf("%s: %s", k, e.bytes)
				}
			}
		}
	}
	s.noDrops(t)
}

// noDrops is the control of every emitter test: nothing was refused on the way.
func (s *emitterStack) noDrops(t *testing.T) {
	t.Helper()
	for _, series := range []string{
		`gelf_messages_dropped_total{reason="chunk_invalid"}`,
		`gelf_messages_dropped_total{reason="chunk_timeout"}`,
		`gelf_messages_dropped_total{reason="decompress"}`,
		`gelf_messages_dropped_total{reason="too_large"}`,
	} {
		if v := s.metric(series); v != "0" {
			t.Errorf("%s = %s", series, v)
		}
	}
}

// The Docker daemon's own gelf log driver (`docker run --log-driver=gelf`):
// the daemon, not the container, is the emitter. UDP with gzip (the driver's
// default compression) and TCP (\0 framing); the container's output lines
// become short_message, and the driver adds its _container_* fields.
func TestEmittersDockerLogDriver(t *testing.T) {
	requireEmitters(t)
	s := startForEmitters(t)
	marker := fmt.Sprintf("dockerdrv-%d", time.Now().UnixNano())
	run := func(proto string, port int, extra ...string) {
		t.Helper()
		name := fmt.Sprintf("gelf-nats-emit-docker-%s-%d", proto, os.Getpid())
		args := append([]string{"run", "--rm", "--name", name, "--log-driver=gelf",
			"--log-opt", fmt.Sprintf("gelf-address=%s://host.docker.internal:%d", proto, port)}, extra...)
		args = append(args, "alpine:3.22", "sh", "-c",
			fmt.Sprintf("for i in 1 2 3; do echo %s %s $i; done", marker, proto))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
			t.Fatalf("docker %v: %v\n%s", args, err, out)
		}
	}
	run("udp", s.udp, "--log-opt", "gelf-compression-type=gzip")
	run("tcp", s.tcp)

	want := map[string]int{"udp": 3, "tcp": 3}
	got := s.byTransport(marker, want)
	for k, w := range want {
		if len(got[k]) != w {
			t.Errorf("%s: %d envelopes, want %d", k, len(got[k]), w)
		}
	}
	for k, envs := range got {
		for _, e := range envs {
			p := e.Payload
			if p.Format != "gelf" || !strings.HasPrefix(p.message(), marker+" "+k+" ") ||
				p.Fields["container_name"] == nil || p.TLS != nil {
				t.Errorf("%s: parsed as %s", k, e.bytes)
			}
			if k == "udp" && show(p.Compression) != "gzip" {
				t.Errorf("udp: compression %s, want gzip", show(p.Compression))
			}
		}
	}
	s.noDrops(t)
}
