// Command go-relplog-nats-server is the LogHub RELP server described in
// docs/SPEC.md: rsyslog's RELP (omrelp) in over TCP, acknowledged per message,
// one JLH envelope of type=syslog out per message, published to NATS.
//
// Everything of substance lives in internal/app, so the integration tests build
// the same stack this binary does. The one thing that cannot live there is the
// identity: go-loghub-ident's Initialize() runs ONCE per process and exits with
// 112 on a second call, while the tests assemble the app dozens of times in one
// process. So main owns that call and hands the app a function to make it.
package main

import (
	"os"
	// The RFC 3164 zone (syslog.rfc3164_timezone) must not depend on the image
	// shipping a zoneinfo database (SPEC §15.4).
	_ "time/tzdata"

	lhident "github.com/patrickbrandao/go-loghub-ident"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/app"
	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/ident"
)

// Set with -ldflags at release time.
var (
	version = "dev"
	commit  = "none"
)

func main() {
	app.SetBuildInfo(version, commit)
	os.Exit(app.Main(os.Args[1:], initIdentity))
}

// initIdentity is called by the app after the flags that end the program
// (--help, --version, --dump-config) and before any resource is opened
// (go-loghub-ident rule 2, SPEC §13.1).
//
// DATADIR is exported first so the library writes agent_uuid and machine_id in
// the same root the server writes emergency/ (SPEC §16.6). On any failure lhident.Initialize does not return: it writes the
// reason to stderr and exits with 100..114, by design -- there is nothing to
// handle here.
func initIdentity(dataDir string) (ident.Identity, error) {
	if err := os.Setenv("DATADIR", dataDir); err != nil {
		return ident.Identity{}, err
	}
	lhident.Initialize()
	return ident.FromLibrary()
}
