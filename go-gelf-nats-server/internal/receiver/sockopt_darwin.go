//go:build darwin

package receiver

import "syscall"

const (
	// soReusePort is SO_REUSEPORT on the BSD family, including macOS.
	//
	// Note that BSD semantics differ from Linux: BSD balances by connection in
	// a way that is not the load-spreading behaviour Linux gives, which is why
	// listen.udp_listeners and listen.tcp_listeners default to 1 and this is
	// opt-in.
	soReusePort = 0x200

	// IPv4: IP_RECVDSTADDR is both the option and the control message type,
	// and the message is a bare struct in_addr.
	ipRecvDstOpt = syscall.IP_RECVDSTADDR
	ipDstType    = syscall.IP_RECVDSTADDR
	ipDstOffset  = 0

	// IPv6: the RFC 3542 values of <netinet6/in6.h>. The syscall package for
	// darwin does not export them (only the RFC 2292 IPV6_2292PKTINFO), and
	// golang.org/x/sys is not a dependency (SPEC §17.3). Frozen kernel ABI,
	// like soReusePort; TestUDPDstIPFromPktinfo proves them on every run.
	ipv6RecvPktinfoOpt = 0x3d // IPV6_RECVPKTINFO
	ipv6PktinfoType    = 0x2e // IPV6_PKTINFO

	// maxSunPath is the usable length of sockaddr_un.sun_path: 104 bytes, one
	// of them the terminating NUL (SPEC §4.3).
	maxSunPath = 103

	// rcvbufReadbackFactor: getsockopt(SO_RCVBUF) returns the granted size.
	rcvbufReadbackFactor = 1
)
