//go:build linux

package receiver

import "syscall"

const (
	// soReusePort is SO_REUSEPORT on Linux, identical across every
	// architecture (see setReusePort for why it is not syscall's).
	soReusePort = 0xf

	// IPv4: IP_PKTINFO is both the option and the control message type. The
	// message is struct in_pktinfo { int ipi_ifindex; struct in_addr
	// ipi_spec_dst; struct in_addr ipi_addr; }, and ipi_addr -- the
	// destination in the packet header, the address the emitter dialed -- is
	// at offset 8. ipi_spec_dst is the local address routing would pick, which
	// differs for broadcast and is not what the payload wants.
	ipRecvDstOpt = syscall.IP_PKTINFO
	ipDstType    = syscall.IP_PKTINFO
	ipDstOffset  = 8

	// IPv6 (RFC 3542).
	ipv6RecvPktinfoOpt = syscall.IPV6_RECVPKTINFO
	ipv6PktinfoType    = syscall.IPV6_PKTINFO

	// maxSunPath is the usable length of sockaddr_un.sun_path: 108 bytes, one
	// of them the terminating NUL (SPEC §4.3).
	maxSunPath = 107

	// rcvbufReadbackFactor: getsockopt(SO_RCVBUF) returns twice the granted
	// size on Linux (socket(7)).
	rcvbufReadbackFactor = 2
)
