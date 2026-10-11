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
)

// The identity exit codes 100..114 of SPEC §13.5 come from go-loghub-ident,
// whose Initialize() calls os.Exit itself and may run only once per process
// (SPEC §13.1). They cannot be observed in-process: the only proof is the real
// binary, built from cmd/go-syslog-server and run as a subprocess with a clean
// environment.

var (
	binaryOnce sync.Once
	binaryPath string
	binaryErr  error
)

// buildBinary builds cmd/go-syslog-server once per test process.
func buildBinary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sls-bin")
		if err != nil {
			binaryErr = err
			return
		}
		binaryPath = filepath.Join(dir, "go-syslog-server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/go-syslog-server")
		cmd.Dir = filepath.Join("..", "..")
		if out, err := cmd.CombinedOutput(); err != nil {
			binaryErr = errors.New(err.Error() + "\n" + string(out))
		}
	})
	if binaryErr != nil {
		t.Fatalf("building cmd/go-syslog-server: %v", binaryErr)
	}
	return binaryPath
}

func TestMain(m *testing.M) {
	code := m.Run()
	if binaryPath != "" {
		os.RemoveAll(filepath.Dir(binaryPath))
	}
	os.Exit(code)
}

// identityEnv is the environment of a subprocess: the identity variables the
// library reads, and nothing inherited -- a DATADIR or a SYSLOG_* of the
// developer's shell must not leak into the run.
func identityEnv(dataDir string, without ...string) []string {
	env := map[string]string{
		"DATADIR":    dataDir,
		"AGENT_UUID": testID.From,
		"AGENT_NAME": testID.AgentName,
		"WORKSPACE":  testID.Space,
		"HOSTNAME":   testID.Hostname,
		"MACHINE_ID": testID.MachineID,
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

// serverArgs keeps the subprocess off every well-known port and every external
// destination.
var serverArgs = []string{
	"--listen.udp=127.0.0.1:0", "--listen.tcp=", "--admin.bind=127.0.0.1:0",
	"--sink.type=none", "--log.format=json",
}

// runToExit runs the binary and returns its exit code and combined output.
func runToExit(t *testing.T, bin, dir string, env []string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, serverArgs...)
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

// runUntilOpen starts the binary, waits for "transport open" in its log, sends
// SIGTERM and returns the exit code and the log.
func runUntilOpen(t *testing.T, bin, dir string, env []string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, serverArgs...)
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
		cwd := t.TempDir()
		code, log := runUntilOpen(t, bin, cwd, identityEnv("data", "AGENT_UUID"))
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
		dir := t.TempDir()
		code, log := runUntilOpen(t, bin, dir, identityEnv(dir))
		if code != 0 {
			t.Fatalf("exit %d, want 0\n%s", code, log)
		}
		if _, err := os.Stat(filepath.Join(dir, "agent_uuid")); !os.IsNotExist(err) {
			t.Errorf("agent_uuid was written although AGENT_UUID came from the environment: %v", err)
		}
		for _, want := range []string{`"from":"` + testID.From + `"`, `"space":"default"`,
			`"agent_name":"` + testID.AgentName + `"`, `"msg":"shutdown complete"`} {
			if !strings.Contains(log, want) {
				t.Errorf("the log lacks %s:\n%s", want, log)
			}
		}
	})
}
