// Command go-webhook-injector sends HTTP requests to a webhook server --
// generated, from a template, from a file or replayed from a tcpdump capture --
// over HTTP/1.1 or HTTP/2, and reports the response to each one.
// docs/SPEC.md is the source of truth; everything lives in internal/app.
package main

import (
	"os"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-injector/internal/app"
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
