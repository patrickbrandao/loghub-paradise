// Command go-gelf-injector sends GELF messages to a server -- generated, from
// a template, from a file or replayed from a tcpdump capture -- over UDP
// (chunked, compressed), TCP, TLS or HTTP, and reports what happened to each
// one. docs/SPEC.md is the source of truth; everything lives in internal/app.
package main

import (
	"os"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-injector/internal/app"
)

// Set with -ldflags at release time.
var (
	version = "dev"
	commit  = "none"
)

func main() {
	app.SetBuildInfo(version, commit)
	os.Exit(app.Main(os.Args[1:], os.Stdout, os.Stderr))
}
