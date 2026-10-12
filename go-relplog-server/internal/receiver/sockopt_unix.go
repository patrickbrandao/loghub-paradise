//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package receiver

import "syscall"

// setReusePort enables SO_REUSEPORT so several listeners can share one address
// (SPEC §4.4, listen.tcp_listeners and listen.tls_listeners > 1), with the
// stdlib syscall package only: no golang.org/x/sys (SPEC §17.3).
//
// The option value is hard-coded per platform rather than taken from the
// syscall package, because syscall.SO_REUSEPORT is missing on linux/amd64 --
// the primary deployment target -- while present on linux/arm64. The
// alternative was adding golang.org/x/sys as a production dependency for one
// integer.
//
// Hard-coding is safe here: these are frozen kernel ABI constants. Changing
// them would break every compiled binary on the platform, so they cannot
// change. syscall.SOL_SOCKET is defined everywhere and is used as-is.
func setReusePort(fd uintptr) error {
	return syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
}

// soReusePort is SO_REUSEPORT: 0xf on Linux, identical across every
// architecture; 0x200 on the BSD family, macOS included. BSD balances by
// connection in a way that is not the load spreading Linux gives, which is
// why listen.*_listeners default to 1 and this is opt-in.
const soReusePort = soReusePortValue
