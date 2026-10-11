//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package server

// SO_REUSEPORT on the BSD family, including macOS.
//
// Note that BSD semantics differ from Linux: BSD balances by connection in a
// way that is not the load-spreading behaviour Linux gives, which is why
// server.listeners defaults to 1 and this is opt-in.
const soReusePort = 0x200
