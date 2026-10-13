package receiver

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server/internal/item"
)

// ProxyMode is proxy.protocol (SPEC §4.6).
type ProxyMode uint8

const (
	// ProxyOff reads nothing. A PROXY header that arrives anyway becomes the
	// beginning of a malformed message, which is the honest record of a
	// balancer configured without telling this server.
	ProxyOff ProxyMode = iota
	// ProxyOptional reads a header from a TRUSTED peer when one comes, and
	// goes on without it when none does. From an untrusted peer nothing is
	// read.
	ProxyOptional
	// ProxyRequired closes every connection that does not come from a trusted
	// peer WITH a valid header (reason="proxy").
	ProxyRequired
)

func (m ProxyMode) String() string {
	switch m {
	case ProxyOff:
		return "off"
	case ProxyOptional:
		return "optional"
	case ProxyRequired:
		return "required"
	}
	return "unknown"
}

// ParseProxyMode reads proxy.protocol: off, optional or required.
func ParseProxyMode(s string) (ProxyMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "off", "":
		return ProxyOff, nil
	case "optional":
		return ProxyOptional, nil
	case "required":
		return ProxyRequired, nil
	}
	return 0, fmt.Errorf("receiver: proxy.protocol %q: want off, optional or required", s)
}

// defaultProxyHeaderTimeout is proxy.header_timeout (SPEC §16.5).
const defaultProxyHeaderTimeout = 5 * time.Second

// ProxyConfig is the [proxy] section (SPEC §4.6, §16.5), for the RELP
// listener.
type ProxyConfig struct {
	Mode ProxyMode
	// Trusted are the balancers' CIDRs (proxy.trusted, default
	// 127.0.0.1/32,::1/128), tested against the REAL TCP peer. A list that is
	// too wide lets anyone forge src-ip.
	Trusted []netip.Prefix
	// HeaderTimeout bounds the wait for the header (proxy.header_timeout,
	// 5 s). Zero or negative selects 5 s.
	HeaderTimeout time.Duration
}

// Validate refuses the combination SPEC §13.2 makes exit 1: a mode other than
// off with no trusted network.
func (p ProxyConfig) Validate() error {
	if p.Mode > ProxyRequired {
		return fmt.Errorf("receiver: unknown proxy.protocol %d", p.Mode)
	}
	if p.Mode != ProxyOff && len(p.Trusted) == 0 {
		return errors.New("receiver: proxy.protocol is not off but proxy.trusted is empty")
	}
	for _, t := range p.Trusted {
		if !t.IsValid() {
			return fmt.Errorf("receiver: invalid proxy.trusted prefix %v", t)
		}
	}
	return nil
}

// Trusts reports whether a real TCP peer may send a PROXY header.
func (p ProxyConfig) Trusts(a netip.Addr) bool {
	a = a.Unmap().WithZone("")
	for _, t := range p.Trusted {
		if t.Contains(a) {
			return true
		}
	}
	return false
}

func (p ProxyConfig) headerTimeout() time.Duration {
	if p.HeaderTimeout <= 0 {
		return defaultProxyHeaderTimeout
	}
	return p.HeaderTimeout
}

// ParseCIDRList reads a comma-separated list of CIDRs, the form of
// proxy.trusted. A bare address is accepted as its host
// route (/32 or /128). Blank items are ignored.
func ParseCIDRList(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !strings.Contains(f, "/") {
			a, err := netip.ParseAddr(f)
			if err != nil {
				return nil, fmt.Errorf("receiver: %q is neither a CIDR nor an address", f)
			}
			a = a.Unmap().WithZone("")
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(f)
		if err != nil {
			return nil, fmt.Errorf("receiver: invalid CIDR %q: %v", f, err)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

// ErrProxyHeader means a PROXY header began -- its signature matched -- and is
// not valid, or the connection failed in the middle of it. The connection is
// closed (reason="proxy"): what follows a broken header cannot be trusted to
// be the beginning of a syslog stream.
var ErrProxyHeader = errors.New("receiver: invalid PROXY protocol header")

// ProxyHeader is what a valid PROXY header declared.
type ProxyHeader struct {
	// Version is 1 (text) or 2 (binary).
	Version int
	// Local means "use the real peer": a v2 LOCAL command, a v2 family or
	// protocol that carries no TCP address (UNSPEC, UNIX, DGRAM), or v1
	// UNKNOWN (SPEC §4.6). Src and Dst are then zero.
	Local bool
	// Src is the client, Dst the address it dialed. Src is always a usable
	// address; Dst's address may be the zero Addr when the header declared
	// an unspecified one (it is then omitted, SPEC §4.2).
	Src netip.AddrPort
	Dst netip.AddrPort
}

// The two signatures. v1 is text, v2 a 12-byte binary preamble chosen so that
// it can never begin a valid text protocol (it contains a NUL and "QUIT").
var (
	proxyV1Sig = []byte("PROXY ")
	proxyV2Sig = []byte("\r\n\r\n\x00\r\nQUIT\n")
)

const (
	// proxyV1Max is the longest v1 line, CRLF included (the protocol's own
	// bound: "PROXY TCP6 " + two full IPv6 + two ports + CRLF).
	proxyV1Max = 107
	// proxyV2Max bounds the whole v2 header. The protocol allows 16+65535
	// bytes; nothing a balancer sends for TCP comes close, and above it the
	// connection is closed (SPEC §4.6). It is also the size of the one
	// buffer the header is read into, so the declared length never decides an
	// allocation.
	proxyV2Max    = 4096
	proxyV2Fixed  = 16
	proxyV2Inet   = 12  // 2 x IPv4 + 2 ports
	proxyV2Inet6  = 36  // 2 x IPv6 + 2 ports
	proxyV2UnixSz = 216 // 2 x 108-byte paths
)

// ReadProxyHeader reads a PROXY protocol v1 or v2 header from the start of r
// (SPEC §4.6). Own parser, stdlib only, fuzzed (FuzzProxy, SPEC §18.3).
//
//   - found is false, with a nil error, when the stream does not begin with a
//     signature: nothing has been consumed, and the bytes belong to the
//     RELP stream.
//   - err is io.EOF when the connection ended before its first byte.
//   - err wraps ErrProxyHeader when a signature matched and the header is not
//     valid or was cut short.
//
// It NEVER consumes a byte past the header it declares: what follows is the
// RELP stream, and belongs to the next reader. It never
// allocates by a declared length either: a v2 header is read into a fixed
// 4 KiB array, and a longer declaration is refused before a byte of it is
// read.
func ReadProxyHeader(r *bufio.Reader) (hdr ProxyHeader, found bool, err error) {
	first, err := r.Peek(1)
	if len(first) == 0 {
		return hdr, false, err
	}
	var sig []byte
	switch first[0] {
	case proxyV1Sig[0]:
		sig = proxyV1Sig
	case proxyV2Sig[0]:
		sig = proxyV2Sig
	default:
		return hdr, false, nil
	}
	// Byte by byte, stopping at the first mismatch: a syslog line that merely
	// starts with 'P' or a CR decides at once, instead of waiting for the
	// twelve bytes a full-signature peek would demand.
	for i := 2; i <= len(sig); i++ {
		b, _ := r.Peek(i)
		if len(b) < i || b[i-1] != sig[i-1] {
			return hdr, false, nil
		}
	}
	if sig[0] == proxyV1Sig[0] {
		hdr, err = readProxyV1(r)
	} else {
		hdr, err = readProxyV2(r)
	}
	if err != nil {
		return ProxyHeader{}, true, err
	}
	return hdr, true, nil
}

func proxyErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrProxyHeader}, a...)...)
}

// readProxyV1 reads "PROXY TCP4|TCP6|UNKNOWN ...\r\n", at most 107 bytes.
func readProxyV1(r *bufio.Reader) (ProxyHeader, error) {
	var line [proxyV1Max]byte
	n := 0
	for {
		// The bound is checked BEFORE the read: the 108th byte is not the
		// header's, and consuming it would eat the first byte of whatever
		// follows (FuzzProxy found exactly that).
		if n == len(line) {
			return ProxyHeader{}, proxyErr("v1 longer than %d bytes", proxyV1Max)
		}
		c, err := r.ReadByte()
		if err != nil {
			return ProxyHeader{}, proxyErr("v1 cut short: %v", err)
		}
		line[n] = c
		n++
		if c == '\n' {
			break
		}
	}
	if n < 2 || line[n-2] != '\r' {
		return ProxyHeader{}, proxyErr("v1 line not terminated by CRLF")
	}
	return parseProxyV1(line[:n-2])
}

// parseProxyV1 parses the line without its CRLF. Separators are single
// spaces, as the protocol specifies; anything else is a broken balancer.
func parseProxyV1(b []byte) (ProxyHeader, error) {
	hdr := ProxyHeader{Version: 1}
	rest, ok := bytes.CutPrefix(b, proxyV1Sig)
	if !ok {
		return hdr, proxyErr("v1 without the PROXY keyword")
	}
	proto, rest, _ := bytes.Cut(rest, []byte{' '})
	switch string(proto) {
	case "UNKNOWN":
		// "the receiver must ignore anything presented before the CRLF"
		hdr.Local = true
		return hdr, nil
	case "TCP4", "TCP6":
	default:
		return hdr, proxyErr("v1 protocol %q", proto)
	}
	f := bytes.Split(rest, []byte{' '})
	if len(f) != 4 {
		return hdr, proxyErr("v1 %s with %d fields, want 4", proto, len(f))
	}
	src, err1 := netip.ParseAddr(string(f[0]))
	dst, err2 := netip.ParseAddr(string(f[1]))
	if err1 != nil || err2 != nil {
		return hdr, proxyErr("v1 address is not an IP")
	}
	// TCP4 carries dotted IPv4 and TCP6 IPv6, never the other.
	if string(proto) == "TCP4" && (!src.Is4() || !dst.Is4()) ||
		string(proto) == "TCP6" && (!src.Is6() || !dst.Is6()) {
		return hdr, proxyErr("v1 address does not match %s", proto)
	}
	if src.Zone() != "" || dst.Zone() != "" {
		return hdr, proxyErr("v1 address with a zone")
	}
	sp, ok1 := parsePort(f[2])
	dp, ok2 := parsePort(f[3])
	if !ok1 || !ok2 {
		return hdr, proxyErr("v1 port is not a number in 0..65535")
	}
	return finishProxy(hdr, src, sp, dst, dp)
}

// parsePort reads 1 to 5 decimal digits, 0..65535.
func parsePort(b []byte) (uint16, bool) {
	if len(b) == 0 || len(b) > 5 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if n > 65535 {
		return 0, false
	}
	return uint16(n), true
}

// readProxyV2 reads the 16-byte fixed part and the declared address block.
func readProxyV2(r *bufio.Reader) (ProxyHeader, error) {
	hdr := ProxyHeader{Version: 2}
	var fixed [proxyV2Fixed]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return hdr, proxyErr("v2 cut short: %v", err)
	}
	verCmd, famProto := fixed[12], fixed[13]
	length := int(binary.BigEndian.Uint16(fixed[14:16]))

	if verCmd>>4 != 2 {
		return hdr, proxyErr("v2 version %d", verCmd>>4)
	}
	cmd := verCmd & 0x0f
	if cmd > 1 {
		return hdr, proxyErr("v2 command %d", cmd)
	}
	if proxyV2Fixed+length > proxyV2Max {
		// Refused BEFORE reading the block: the declaration alone is enough,
		// and reading 64 KiB to throw it away is what a hostile peer wants.
		return hdr, proxyErr("v2 header of %d bytes, above %d", proxyV2Fixed+length, proxyV2Max)
	}
	var block [proxyV2Max - proxyV2Fixed]byte
	if _, err := io.ReadFull(r, block[:length]); err != nil {
		return hdr, proxyErr("v2 address block cut short: %v", err)
	}
	return parseProxyV2(cmd, famProto, block[:length])
}

// parseProxyV2 interprets the command, the family/protocol byte and the
// address block. TLVs after the addresses are skipped (SPEC §4.6): they were
// read with the block, and nothing here needs them.
func parseProxyV2(cmd, famProto byte, block []byte) (ProxyHeader, error) {
	hdr := ProxyHeader{Version: 2}

	// The protocol lists exactly these family/protocol values; anything else
	// "must be rejected as invalid by receivers".
	need := 0
	switch famProto {
	case 0x00: // UNSPEC
	case 0x11, 0x12: // TCP4, UDP4
		need = proxyV2Inet
	case 0x21, 0x22: // TCP6, UDP6
		need = proxyV2Inet6
	case 0x31, 0x32: // UNIX stream, UNIX dgram
		need = proxyV2UnixSz
	default:
		if cmd == 0 {
			// LOCAL ignores the family entirely: the balancer is talking
			// about itself (a health check), and the receiver uses the real
			// endpoints.
			hdr.Local = true
			return hdr, nil
		}
		return hdr, proxyErr("v2 family/protocol 0x%02x", famProto)
	}
	if cmd == 0 {
		hdr.Local = true
		return hdr, nil
	}
	if len(block) < need {
		return hdr, proxyErr("v2 address block of %d bytes for family 0x%02x, want %d", len(block), famProto, need)
	}

	var src, dst netip.Addr
	var sp, dp uint16
	switch famProto {
	case 0x11:
		src = netip.AddrFrom4([4]byte(block[0:4]))
		dst = netip.AddrFrom4([4]byte(block[4:8]))
		sp = binary.BigEndian.Uint16(block[8:10])
		dp = binary.BigEndian.Uint16(block[10:12])
	case 0x21:
		src = netip.AddrFrom16([16]byte(block[0:16]))
		dst = netip.AddrFrom16([16]byte(block[16:32]))
		sp = binary.BigEndian.Uint16(block[32:34])
		dp = binary.BigEndian.Uint16(block[34:36])
	default:
		// UNSPEC, UDP and UNIX: no TCP address to adopt. Use the real peer,
		// as if there were no header (SPEC §4.6).
		hdr.Local = true
		return hdr, nil
	}
	return finishProxy(hdr, src, sp, dst, dp)
}

// finishProxy canonicalises the declared addresses. The source must be a
// usable address: the payload requires host.src-ip on TCP, and an
// unspecified one is never recorded (SPEC §4.2), so a header declaring
// 0.0.0.0 as its client has nothing this server could write down.
func finishProxy(hdr ProxyHeader, src netip.Addr, sp uint16, dst netip.Addr, dp uint16) (ProxyHeader, error) {
	s := item.Canonical(src)
	if !s.IsValid() {
		return hdr, proxyErr("source address %v is unspecified", src)
	}
	hdr.Src = netip.AddrPortFrom(s, sp)
	hdr.Dst = netip.AddrPortFrom(item.Canonical(dst), dp)
	return hdr, nil
}
