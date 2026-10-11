//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package receiver

// SO_REUSEPORT on the BSD family, including macOS.
//
// Note that BSD semantics differ from Linux: BSD balances by connection in a
// way that is not the load-spreading behaviour Linux gives, which is why
// listen.udp_listeners and listen.tcp_listeners default to 1 and this is opt-in.
const soReusePort = 0x200
