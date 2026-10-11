// Command go-syslog-server is the syslog ingestion server described in
// docs/SPEC.md.
//
// Everything of substance lives in internal/app, so the integration tests build
// the same stack this binary does.
package main

import (
	"os"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server/internal/app"
)

// Set with -ldflags at release time.
var (
	version = "dev"
	commit  = "none"
)

func main() {
	app.SetBuildInfo(version, commit)
	os.Exit(app.Run(os.Args[1:]))
}
