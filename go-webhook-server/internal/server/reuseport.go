//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package server

import "syscall"

// setReusePort enables SO_REUSEPORT so several listeners can share one address
// (SPEC §12.4, server.listeners > 1).
//
// The option value is hard-coded per platform rather than taken from the
// syscall package, because syscall.SO_REUSEPORT is missing on linux/amd64 --
// the primary deployment target -- while present on linux/arm64. The
// alternative was adding golang.org/x/sys as a third production dependency for
// one integer.
//
// Hard-coding is safe here: these are frozen kernel ABI constants. Changing
// them would break every compiled binary on the platform, so they cannot
// change. syscall.SOL_SOCKET is defined everywhere and is used as-is.
func setReusePort(fd uintptr) error {
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
}
