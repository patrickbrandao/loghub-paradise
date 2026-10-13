//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package receiver

import "errors"

// SO_REUSEPORT has no portable equivalent here, so listen.*_listeners > 1 fails
// loudly at bind time rather than silently collapsing to a single listener.
func setReusePort(fd uintptr) error {
	return errors.New("SO_REUSEPORT is unsupported on this platform; set every listen.*_listeners to 1")
}
