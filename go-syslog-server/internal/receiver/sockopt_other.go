//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package receiver

import (
	"errors"
	"net/netip"
)

// maxSunPath is the Linux value: a platform without the BSD socket options is
// not one this server is shipped for, and the check only has to be plausible.
const maxSunPath = 107

// rcvbufReadbackFactor is irrelevant here: SO_RCVBUF cannot be read back.
const rcvbufReadbackFactor = 1

var errNoSockopt = errors.New("not supported on this platform")

// SO_REUSEPORT has no portable equivalent here, so listen.*_listeners > 1 fails
// loudly at bind time rather than silently collapsing to a single listener.
func setReusePort(fd uintptr) error {
	return errors.New("SO_REUSEPORT is unsupported on this platform; set every listen.*_listeners to 1")
}

// Without PKTINFO the UDP receiver WARNs at boot and falls back to the
// listener's own address (SPEC §4.2).
func setPktinfo(fd uintptr) error { return errNoSockopt }

func parsePktinfo(oob []byte) netip.Addr { return netip.Addr{} }

func getRcvbuf(fd uintptr) (int, error) { return 0, errNoSockopt }
