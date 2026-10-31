package transport

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// ProxyHeader writes a PROXY protocol header (haproxy's proxy-protocol.txt)
// at the start of every TCP/TLS connection (SPEC §4.4), as a balancer in
// front of the server would.
type ProxyHeader struct {
	Version int // 1 or 2
	// Src and Dst override the addresses announced; a zero value announces
	// the real one of the connection (local as the client, remote as the
	// address it dialed).
	Src, Dst netip.AddrPort
}

var proxyV2Sig = []byte("\r\n\r\n\x00\r\nQUIT\n")

// Encode returns the header for a connection from local to remote.
func (p *ProxyHeader) Encode(local, remote net.Addr) ([]byte, error) {
	src, dst := p.Src, p.Dst
	if !src.IsValid() {
		src = addrPort(local)
	}
	if !dst.IsValid() {
		dst = addrPort(remote)
	}
	if !src.IsValid() || !dst.IsValid() {
		return nil, fmt.Errorf("PROXY header needs IP addresses, got %v -> %v", local, remote)
	}
	src = netip.AddrPortFrom(src.Addr().Unmap(), src.Port())
	dst = netip.AddrPortFrom(dst.Addr().Unmap(), dst.Port())
	if src.Addr().Is4() != dst.Addr().Is4() {
		// One family per header: announce an IPv4 pair as mapped IPv6.
		src = netip.AddrPortFrom(netip.AddrFrom16(src.Addr().As16()), src.Port())
		dst = netip.AddrPortFrom(netip.AddrFrom16(dst.Addr().As16()), dst.Port())
	}
	v4 := src.Addr().Is4()
	if p.Version == 1 {
		fam := "TCP6"
		if v4 {
			fam = "TCP4"
		}
		s := "PROXY " + fam + " " + src.Addr().String() + " " + dst.Addr().String() + " " +
			strconv.Itoa(int(src.Port())) + " " + strconv.Itoa(int(dst.Port())) + "\r\n"
		return []byte(s), nil
	}
	b := append([]byte(nil), proxyV2Sig...)
	b = append(b, 0x21) // version 2, command PROXY
	if v4 {
		b = append(b, 0x11) // AF_INET, STREAM
		b = binary.BigEndian.AppendUint16(b, 12)
		s4, d4 := src.Addr().As4(), dst.Addr().As4()
		b = append(b, s4[:]...)
		b = append(b, d4[:]...)
	} else {
		b = append(b, 0x21) // AF_INET6, STREAM
		b = binary.BigEndian.AppendUint16(b, 36)
		s6, d6 := src.Addr().As16(), dst.Addr().As16()
		b = append(b, s6[:]...)
		b = append(b, d6[:]...)
	}
	b = binary.BigEndian.AppendUint16(b, src.Port())
	b = binary.BigEndian.AppendUint16(b, dst.Port())
	return b, nil
}

func addrPort(a net.Addr) netip.AddrPort {
	if t, ok := a.(*net.TCPAddr); ok {
		return t.AddrPort()
	}
	ap, _ := netip.ParseAddrPort(a.String())
	return ap
}
