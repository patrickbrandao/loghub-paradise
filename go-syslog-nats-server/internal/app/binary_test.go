package app

import (
	"bufio"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server/internal/ident"
)

// The identity exit codes 100..114 of SPEC §13.5 come from go-loghub-ident,
// whose Initialize() calls os.Exit itself and may run only once per process
// (SPEC §13.1). They cannot be observed in-process: the only proof is the real
// binary, built from cmd/go-syslog-nats-server and run as a subprocess with a
// clean environment.

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

// buildBinary builds cmd/go-syslog-nats-server once per test process.
func buildBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sls-bin")
		if err != nil {
			binaryErr = err
			return
		}
		binaryPath = filepath.Join(dir, "go-syslog-nats-server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/go-syslog-nats-server")
		cmd.Dir = filepath.Join("..", "..")
		if out, err := cmd.CombinedOutput(); err != nil {
			binaryErr = errors.New(err.Error() + "\n" + string(out))
		}
	})
	if binaryErr != nil {
		t.Fatalf("building cmd/go-syslog-nats-server: %v", binaryErr)
	}
	return binaryPath
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binaryPath != "" {
		os.RemoveAll(filepath.Dir(binaryPath))
	}
	if natsShared != nil {
		natsShared.nc.Close()
	}
	os.Exit(code)
}

// identityEnv is the environment of a subprocess with testID: the identity
// variables the library reads, and nothing inherited -- a DATADIR or a
// SYSLOG_* of the developer's shell must not leak into the run.
func identityEnv(dataDir string, without ...string) []string {
	return identityEnvFor(testID, dataDir, without...)
}

// identityEnvFor is identityEnv with the identity of a fixture, whose space
// its stream covers.
func identityEnvFor(id ident.Identity, dataDir string, without ...string) []string {
	env := map[string]string{
		"DATADIR":    dataDir,
		"AGENT_UUID": id.From,
		"AGENT_NAME": id.AgentName,
		"WORKSPACE":  id.Space,
		"HOSTNAME":   id.Hostname,
		"MACHINE_ID": id.MachineID,
	}
	for _, k := range without {
		delete(env, k)
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

// localServerArgs keeps the subprocess off every well-known port. It names no
// destination, which is enough for a run that must end before boot step 7.
var localServerArgs = []string{
	"--listen.udp=127.0.0.1:0", "--listen.tcp=", "--admin.bind=127.0.0.1:0", "--log.format=json",
}

// serverArgs is localServerArgs plus the fixture's NATS and stream: the only
// destination there is. The subprocess must run with identityEnvFor(f.id, ...),
// whose space the stream covers.
func (f *fixture) serverArgs() []string {
	return append(append([]string(nil), localServerArgs...), f.sinkArgs()...)
}

// runToExit runs the binary with localServerArgs and returns its exit code
// and combined output.
func runToExit(t *testing.T, bin, dir string, env []string) (int, string) {
	t.Helper()
	return runArgsToExit(t, bin, dir, env, localServerArgs...)
}

// runArgsToExit runs the binary with args and returns its exit code and
// combined output.
func runArgsToExit(t *testing.T, bin, dir string, env []string, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir, cmd.Env = dir, env
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &ee):
		return ee.ExitCode(), string(out)
	}
	t.Fatalf("running the binary: %v\n%s", err, out)
	return -1, ""
}

// runUntilOpen starts the binary with args, waits for "transport open" in its
// log, sends SIGTERM and returns the exit code and the log.
func runUntilOpen(t *testing.T, bin, dir string, env, args []string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir, cmd.Env = dir, env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var (
		mu  sync.Mutex
		log strings.Builder
	)
	open := make(chan struct{})
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		once := sync.Once{}
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			mu.Lock()
			log.WriteString(sc.Text() + "\n")
			mu.Unlock()
			if strings.Contains(sc.Text(), "transport open") {
				once.Do(func() { close(open) })
			}
		}
	}()
	select {
	case <-open:
	case <-scanned:
		_ = cmd.Wait()
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("the binary ended before opening a transport (exit %d):\n%s", cmd.ProcessState.ExitCode(), log.String())
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the binary never opened a transport")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	<-scanned
	err = cmd.Wait()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return code, log.String()
}

// SPEC §13.1 steps 2-4, §13.5, §16.6.
func TestBinaryIdentityExitCodes(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the real binary")
	}
	bin := buildBinary(t)

	// A relative DATADIR never reaches the library: the app resolves it
	// against the working directory and exports the absolute path before
	// Initialize (SPEC §13.1 step 2, §16.5 "convertida em absoluto"), so the
	// library's own refusal of a relative path (exit 100) cannot happen through
	// the binary. The proof is that the identity the library GENERATES lands
	// in <cwd>/<relative>.
	t.Run("relative DATADIR is resolved before the library", func(t *testing.T) {
		f := newFixture(t)
		cwd := t.TempDir()
		code, log := runUntilOpen(t, bin, cwd, identityEnvFor(f.id, "data", "AGENT_UUID"), f.serverArgs())
		if code != 0 {
			t.Fatalf("exit %d, want 0\n%s", code, log)
		}
		if _, err := os.Stat(filepath.Join(cwd, "data", "agent_uuid")); err != nil {
			t.Fatalf("the generated agent_uuid is not under <cwd>/data: %v\n%s", err, log)
		}
		if !strings.Contains(log, "resolved to an absolute path") {
			t.Errorf("the boot log does not say data_dir was resolved:\n%s", log)
		}
	})

	// Exit 100 straight from the library: an identity file in DATADIR that is
	// a symlink is an invalid source, never treated as absent.
	t.Run("invalid identity source in DATADIR exits 100", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Symlink("/etc/hosts", filepath.Join(dir, "agent_uuid")); err != nil {
			t.Fatal(err)
		}
		code, out := runToExit(t, bin, dir, identityEnv(dir, "AGENT_UUID"))
		if code != 100 {
			t.Fatalf("exit %d, want 100\n%s", code, out)
		}
		if !strings.Contains(out, "lib-loghub-ident: DATADIR") {
			t.Errorf("the reason is not the library's:\n%s", out)
		}
	})

	t.Run("WORKSPACE=bad_space exits 111", func(t *testing.T) {
		dir := t.TempDir()
		env := append(identityEnv(dir, "WORKSPACE"), "WORKSPACE=bad_space")
		code, out := runToExit(t, bin, dir, env)
		if code != 111 {
			t.Fatalf("exit %d, want 111\n%s", code, out)
		}
		if !strings.Contains(out, "lib-loghub-ident: WORKSPACE") {
			t.Errorf("the reason is not the library's:\n%s", out)
		}
	})

	// The five identity variables from the environment: the boot completes,
	// SIGTERM ends it with 0, and an AGENT_UUID given by the environment is
	// never written to disk (go-loghub-ident level 1).
	t.Run("full identity from the environment", func(t *testing.T) {
		f := newFixture(t)
		dir := t.TempDir()
		code, log := runUntilOpen(t, bin, dir, identityEnvFor(f.id, dir), f.serverArgs())
		if code != 0 {
			t.Fatalf("exit %d, want 0\n%s", code, log)
		}
		if _, err := os.Stat(filepath.Join(dir, "agent_uuid")); !os.IsNotExist(err) {
			t.Errorf("agent_uuid was written although AGENT_UUID came from the environment: %v", err)
		}
		for _, want := range []string{`"from":"` + f.id.From + `"`, `"space":"` + f.space + `"`,
			`"agent_name":"` + f.id.AgentName + `"`, `"msg":"shutdown complete"`} {
			if !strings.Contains(log, want) {
				t.Errorf("the log lacks %s:\n%s", want, log)
			}
		}
	})
}

// SPEC §13.5, §16: --version names THIS program, and a configuration the
// registry refuses is exit 1 with the reason on stderr -- before the identity,
// before any socket. The keys of the parent go-syslog-server that this server
// removed (sink.type, ring.mode, listen.tls, the [tls] section, the NATS client
// TLS, the facility-severity subjects, the other sinks) are unknown here, not
// silently ignored: an operator carrying a parent configuration over must learn
// at once that it does not apply. And there is no TLS anywhere: a tls:// URL,
// in LOGHUB_EB_URL or in sink.nats.urls, is refused rather than dialled.
func TestBinaryVersionAndConfigErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the real binary")
	}
	bin := buildBinary(t)

	t.Run("--version", func(t *testing.T) {
		dir := t.TempDir()
		code, out := runArgsToExit(t, bin, dir, identityEnv(dir), "--version")
		if code != ExitOK || !strings.HasPrefix(out, "go-syslog-nats-server ") {
			t.Fatalf("exit %d, output %q: want 0 and a line starting with the program name", code, out)
		}
		if strings.Contains(out, "go-syslog-server ") {
			t.Errorf("--version names the parent program: %q", out)
		}
		if _, err := os.Stat(filepath.Join(dir, "agent_uuid")); !os.IsNotExist(err) {
			t.Errorf("--version touched DATADIR: %v", err)
		}
	})

	for _, tc := range []struct {
		name string
		args []string
		env  []string
	}{
		{"removed sink.type", []string{"--sink.type=nats"}, nil},
		{"removed ring.mode", []string{"--ring.mode=memory"}, nil},
		{"removed listen.tls", []string{"--listen.tls=127.0.0.1:6514"}, nil},
		{"removed [tls] section", []string{"--tls.cert_file=/x.pem"}, nil},
		{"removed sink.nats.tls", []string{"--sink.nats.tls=true"}, nil},
		{"removed sink.nats.subjects", []string{"--sink.nats.subjects=facility-severity"}, nil},
		{"removed sink.postgres", []string{"--sink.postgres.addr=127.0.0.1:5432"}, nil},
		{"unknown flag", []string{"--listen.typo=1"}, nil},
		{"no transport", []string{"--listen.udp="}, nil},
		{"tls:// in sink.nats.urls", []string{"--sink.nats.urls=tls://127.0.0.1:4222"}, nil},
		{"tls:// in a sink.nats.urls cluster", []string{"--sink.nats.urls=nats://127.0.0.1:4222,tls://127.0.0.1:4223"}, nil},
		{"LOGHUB_EB_URL with tls://", nil, []string{"LOGHUB_EB_URL=tls://127.0.0.1:4222"}},
		{"LOGHUB_EB_URL of another eventbus", nil,
			[]string{"LOGHUB_EB_URL=postgres://u:p@127.0.0.1:1/db?sslmode=disable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			env := append(identityEnv(dir), tc.env...)
			code, out := runArgsToExit(t, bin, dir, env, append(append([]string(nil), localServerArgs...), tc.args...)...)
			if code != ExitConfig {
				t.Fatalf("exit %d, want %d\n%s", code, ExitConfig, out)
			}
			if !strings.Contains(out, "configuration error") {
				t.Errorf("the reason is not reported as a configuration error:\n%s", out)
			}
			if _, err := os.Stat(filepath.Join(dir, "agent_uuid")); !os.IsNotExist(err) {
				t.Errorf("a refused configuration reached the identity library: %v", err)
			}
		})
	}

	// The control: the same building blocks, valid -- an eventbus URL without
	// TLS -- pass the configuration. --dump-config ends the run right after
	// it, before the identity and the sink (SPEC §13.1 step 1), so the control
	// needs no NATS and proves the refusals above are the keys' own.
	t.Run("control", func(t *testing.T) {
		dir := t.TempDir()
		env := append(identityEnv(dir), "LOGHUB_EB_URL=nats://u:p@127.0.0.1:1,nats://127.0.0.1:2")
		code, out := runArgsToExit(t, bin, dir, env, append(append([]string(nil), localServerArgs...), "--dump-config")...)
		if code != ExitOK {
			t.Fatalf("exit %d, want 0\n%s", code, out)
		}
		if !strings.Contains(out, "127.0.0.1:1") || !strings.Contains(out, "127.0.0.1:2") {
			t.Errorf("the dump does not show the addresses the URL filled:\n%s", out)
		}
		if strings.Contains(out, "u:p@") {
			t.Errorf("the dump shows the URL's password:\n%s", out)
		}
	})
}
