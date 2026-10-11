package app

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/obs"
	"github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server/internal/sink"
)

// signalRun is a subprocess whose log can be waited on line by line.
type signalRun struct {
	t   *testing.T
	cmd *exec.Cmd

	mu    sync.Mutex
	log   strings.Builder
	lines chan string
	ended chan struct{}
}

func startBinary(t *testing.T, args ...string) *signalRun {
	t.Helper()
	return startBinaryIn(t, t.TempDir(), args...)
}

// startBinaryIn is startBinary with the working directory chosen by the test,
// for a run that reads a config file the test edits.
func startBinaryIn(t *testing.T, dir string, args ...string) *signalRun {
	t.Helper()
	bin := buildBinary(t)
	cmd := exec.Command(bin, args...)
	cmd.Dir, cmd.Env = dir, identityEnv(filepath.Join(dir, "data"))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r := &signalRun{t: t, cmd: cmd, lines: make(chan string, 1024), ended: make(chan struct{})}
	go func() {
		defer close(r.ended)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			r.mu.Lock()
			r.log.WriteString(sc.Text() + "\n")
			r.mu.Unlock()
			select {
			case r.lines <- sc.Text():
			default:
			}
		}
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-r.ended; _ = cmd.Wait() })
	return r
}

// waitLine waits for a log line containing s.
func (r *signalRun) waitLine(s string) {
	r.t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case l := <-r.lines:
			if strings.Contains(l, s) {
				return
			}
		case <-r.ended:
			r.t.Fatalf("the binary ended before logging %q:\n%s", s, r.String())
		case <-deadline:
			r.t.Fatalf("no log line with %q:\n%s", s, r.String())
		}
	}
}

func (r *signalRun) signal(sig syscall.Signal) {
	r.t.Helper()
	if err := r.cmd.Process.Signal(sig); err != nil {
		r.t.Fatal(err)
	}
}

// exit waits for the process and returns its exit code and how long it took.
func (r *signalRun) exit(within time.Duration) (int, time.Duration) {
	r.t.Helper()
	start := time.Now()
	select {
	case <-r.ended:
	case <-time.After(within):
		r.t.Fatalf("the binary did not exit within %s:\n%s", within, r.String())
	}
	err := r.cmd.Wait()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, time.Since(start)
	case errors.As(err, &ee):
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			// Killed by the signal's default action: what the shell shows as
			// 128+n, and what this file exists to rule out.
			return 128 + int(ws.Signal()), time.Since(start)
		}
		return ee.ExitCode(), time.Since(start)
	}
	r.t.Fatal(err)
	return -1, 0
}

func (r *signalRun) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.log.String()
}

// unreachableBoot keeps the binary in boot step 7 (WaitReady) for a few
// seconds: a PostgreSQL address nothing listens on, whose refused dial is a
// network failure retried until sink.boot_timeout.
var unreachableBoot = []string{
	"--listen.udp=127.0.0.1:0", "--listen.tcp=", "--admin.bind=127.0.0.1:0", "--log.format=json",
	"--sink.postgres.addr=127.0.0.1:1", "--sink.boot_timeout=4s",
}

// Review finding: SIGHUP was watched only between the end of the boot and the
// start of the shutdown, and outside that window Go's default action ended the
// process with 129 and no log line -- in the middle of a boot that may last
// sink.boot_timeout (300 s). SIGQUIT got Go's default too (goroutine dump,
// exit 2) instead of SPEC §13.4's "SIGQUIT aborta sempre".
func TestBinarySignalsOutsideTheRunWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	t.Run("SIGHUP during the boot is logged and ignored", func(t *testing.T) {
		r := startBinary(t, unreachableBoot...)
		r.waitLine(`"msg":"starting"`)
		time.Sleep(300 * time.Millisecond)
		r.signal(syscall.SIGHUP)
		r.waitLine("SIGHUP ignored")
		if code, _ := r.exit(30 * time.Second); code != ExitSink {
			t.Fatalf("exit %d, want %d (the boot_timeout, not the signal):\n%s", code, ExitSink, r)
		}
	})
	t.Run("SIGQUIT during the boot aborts with 130", func(t *testing.T) {
		r := startBinary(t, unreachableBoot...)
		r.waitLine(`"msg":"starting"`)
		time.Sleep(300 * time.Millisecond)
		r.signal(syscall.SIGQUIT)
		code, took := r.exit(30 * time.Second)
		if code != ExitAborted {
			t.Fatalf("exit %d, want %d:\n%s", code, ExitAborted, r)
		}
		if took > 3*time.Second {
			t.Fatalf("took %s: SIGQUIT waited for the boot_timeout", took)
		}
	})
	t.Run("SIGHUP while up reloads, SIGTERM then exits 0", func(t *testing.T) {
		r := startBinary(t, serverArgs(t)...)
		r.waitLine("transport open")
		r.signal(syscall.SIGHUP)
		r.waitLine("SIGHUP: configuration unchanged")
		r.signal(syscall.SIGTERM)
		if code, _ := r.exit(30 * time.Second); code != ExitOK {
			t.Fatalf("exit %d, want 0:\n%s", code, r)
		}
	})
	t.Run("SIGQUIT while up aborts with 130", func(t *testing.T) {
		r := startBinary(t, serverArgs(t)...)
		r.waitLine("transport open")
		r.signal(syscall.SIGQUIT)
		if code, _ := r.exit(30 * time.Second); code != ExitAborted {
			t.Fatalf("exit %d, want %d:\n%s", code, ExitAborted, r)
		}
		if !strings.Contains(r.String(), "SIGQUIT") {
			t.Fatalf("no log line about the abort:\n%s", r)
		}
	})
}

// SPEC §13.4: "sai com 0 (3 se o sink estiver FATAL)". Review finding of the
// standard server: a sink that went FATAL DURING the drain of a SIGTERM left the
// exit code at 0 with a persistent ring, and every ring waited out the whole
// drain_timeout for a drain that could no longer happen. With the memory ring
// the undelivered messages must also be dumped.
func TestFatalDuringTheDrainExitsThreeAtOnce(t *testing.T) {
	dir := t.TempDir()
	args := append(baseArgs(t, dir), "--listen.udp=", "--shutdown.drain_timeout=30s")
	fs := newFaultySink(true)
	in := launch(t, testID, args, fs.wrap)
	in.waitReady()
	const n = 5
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("<13>1 2026-09-12T14:33:01Z host app - - - drain-%02d", i)
	}
	sendLF(t, in.app.TransportAddr("tcp"), lines...)
	waitFor(t, 10*time.Second, "accepted", func() bool {
		return in.app.metrics.MessagesAcceptedTotal[obs.TransportTCP].Load() == n
	})
	in.cancel() // SIGTERM
	time.Sleep(300 * time.Millisecond)
	fs.failFatal.Store(true)
	fs.open()
	waitFor(t, 5*time.Second, "FATAL", func() bool { return in.app.health.State() == sink.StateFatal })
	start := time.Now()
	code := in.waitExit(30 * time.Second)
	if code != ExitSink {
		t.Errorf("exit %d, want %d", code, ExitSink)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("exit took %s after FATAL: the drain_timeout was waited out", took)
	}
	dumps := dumpFiles(t, in.emgDir(), testID.From)
	if len(dumps) != 1 {
		t.Fatalf("%d dumps, want 1: the undelivered messages must be dumped", len(dumps))
	}
	if got := len(dumpLines(t, dumps[0])); got != n {
		t.Errorf("the dump holds %d of %d undelivered messages", got, n)
	}
}

// SPEC §16.3 through the real signal: SIGHUP applies log.level and names the
// restart-only key it ignored, by name, at WARN. The boot level is error, so
// the INFO line "SIGHUP: applied" -- logged after the swap -- is printed only
// if the new level really is in force.
func TestBinarySIGHUPAppliesTheLogLevel(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the binary")
	}
	dir := t.TempDir()
	ini := filepath.Join(dir, "syslog.ini")
	write := func(level, workers string) {
		t.Helper()
		body := "[log]\nlevel = " + level + "\n\n[sink]\nworkers = " + workers + "\n"
		if err := os.WriteFile(ini, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("error", "4")
	admin := freePort(t)
	r := startBinaryIn(t, dir, append(serverArgs(t), "--config="+ini, "--admin.bind="+admin)...)
	// At level=error nothing announces the open transports: /readyz does.
	waitFor(t, 30*time.Second, "/readyz 200", func() bool {
		resp, err := http.Get("http://" + admin + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	if strings.Contains(r.String(), "transport open") {
		t.Fatalf("an INFO line at level=error:\n%s", r)
	}

	write("info", "2")
	r.signal(syscall.SIGHUP)
	r.waitLine(`"msg":"SIGHUP: applied"`)
	r.waitLine(`"key":"sink.workers"`)
	if s := r.String(); !strings.Contains(s, `"keys":["log.level"]`) ||
		!strings.Contains(s, "requires a restart and was ignored") {
		t.Errorf("the reload log does not name what it applied and what it ignored:\n%s", s)
	}
	r.signal(syscall.SIGTERM)
	if code, _ := r.exit(30 * time.Second); code != ExitOK {
		t.Fatalf("exit %d, want 0:\n%s", code, r)
	}
	if !strings.Contains(r.String(), `"msg":"shutdown complete"`) {
		t.Errorf("the INFO level did not stay in force until the exit:\n%s", r)
	}
}
