//go:build freebsd || netbsd || openbsd || dragonfly

package receiver

import "syscall"

const (
	// soReusePort is SO_REUSEPORT on the BSD family. See sockopt_darwin.go
	// for the BSD balancing caveat.
	soReusePort = 0x200

	// IPv4: IP_RECVDSTADDR is both the option and the control message type,
	// and the message is a bare struct in_addr.
	ipRecvDstOpt = syscall.IP_RECVDSTADDR
	ipDstType    = syscall.IP_RECVDSTADDR
	ipDstOffset  = 0

	// IPv6 (RFC 3542).
	ipv6RecvPktinfoOpt = syscall.IPV6_RECVPKTINFO
	ipv6PktinfoType    = syscall.IPV6_PKTINFO

	// maxSunPath is the usable length of sockaddr_un.sun_path (SPEC §4.3).
	maxSunPath = 103

	// rcvbufReadbackFactor: getsockopt(SO_RCVBUF) returns the granted size.
	rcvbufReadbackFactor = 1
)
