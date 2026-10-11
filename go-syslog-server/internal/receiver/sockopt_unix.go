//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package receiver

import (
	"errors"
	"net/netip"
	"syscall"
)

// Socket options of the datagram and stream transports, with the stdlib
// syscall package only: no golang.org/x/net and no golang.org/x/sys
// (SPEC §17.3). The per-platform constants live in sockopt_<os>.go.

// setReusePort enables SO_REUSEPORT so several listeners can share one address
// (SPEC §4.2 and §4.4, listen.*_listeners > 1).
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

// setPktinfo asks for the destination address of every datagram (SPEC §4.2,
// decision 15): IP_PKTINFO and IPV6_RECVPKTINFO on Linux, IP_RECVDSTADDR and
// IPV6_RECVPKTINFO on the BSDs and macOS.
//
// The family is the socket's, read back with getsockname, not the one the
// configured address suggests: Go opens a dual-stack IPv6 socket for ":1514"
// AND for "0.0.0.0:1514", but an IPv4 one when the kernel has IPv6 disabled
// (common in containers). On a dual-stack socket the IPv6 option covers the
// IPv4 datagrams too: they come with an IPv4-mapped destination. The IPv4
// options are refused there by the BSD kernels, so they are not even tried.
func setPktinfo(fd uintptr) error {
	sa, err := syscall.Getsockname(int(fd))
	if err != nil {
		return err
	}
	switch sa.(type) {
	case *syscall.SockaddrInet4:
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipRecvDstOpt, 1)
	case *syscall.SockaddrInet6:
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvPktinfoOpt, 1)
	}
	return errors.New("not an IP socket")
}

// parsePktinfo extracts the destination address from a datagram's control
// messages, or returns the zero Addr. The caller canonicalises it (an
// IPv4-mapped address becomes IPv4, SPEC §4.2).
//
// syscall.ParseSocketControlMessage handles the platform's cmsghdr layout and
// alignment; its one small allocation per datagram is the price of not
// re-deriving that layout by hand for every kernel.
func parsePktinfo(oob []byte) netip.Addr {
	if len(oob) == 0 {
		return netip.Addr{}
	}
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return netip.Addr{}
	}
	for _, m := range msgs {
		switch {
		case m.Header.Level == syscall.IPPROTO_IPV6 && int(m.Header.Type) == ipv6PktinfoType &&
			len(m.Data) >= 16:
			// struct in6_pktinfo { struct in6_addr ipi6_addr; ... }
			return netip.AddrFrom16([16]byte(m.Data[:16]))
		case m.Header.Level == syscall.IPPROTO_IP && int(m.Header.Type) == ipDstType &&
			len(m.Data) >= ipDstOffset+4:
			return netip.AddrFrom4([4]byte(m.Data[ipDstOffset : ipDstOffset+4]))
		}
	}
	return netip.Addr{}
}

// getRcvbuf reads SO_RCVBUF back: the value the kernel actually granted.
func getRcvbuf(fd uintptr) (int, error) {
	return syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
}
