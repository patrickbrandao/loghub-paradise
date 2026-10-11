//go:build unix

package receiver

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-syslog-server/internal/obs"
)

// rawTLS13Writer writes TLS 1.3 records on a connection whose handshake a
// tls.Client already completed, with the client's application traffic secret
// taken from the KeyLogWriter. It exists for ONE purpose: crypto/tls has no API
// to send a KeyUpdate, and the regression of F2 needs a peer that requests key
// updates and never reads.
type rawTLS13Writer struct {
	conn   net.Conn
	newH   func() hash.Hash
	keyLen int
	secret []byte
	aead   cipher.AEAD
	iv     []byte
	seq    uint64
}

// expandLabel is HKDF-Expand-Label of RFC 8446 §7.1 with an empty context.
func expandLabel(newH func() hash.Hash, secret []byte, label string, n int) []byte {
	full := "tls13 " + label
	info := []byte{byte(n >> 8), byte(n), byte(len(full))}
	info = append(info, full...)
	info = append(info, 0)
	out, err := hkdf.Expand(newH, secret, string(info), n)
	if err != nil {
		panic(err)
	}
	return out
}

func (w *rawTLS13Writer) setSecret(secret []byte) {
	w.secret = secret
	block, err := aes.NewCipher(expandLabel(w.newH, secret, "key", w.keyLen))
	if err != nil {
		panic(err)
	}
	w.aead, _ = cipher.NewGCM(block)
	w.iv = expandLabel(w.newH, secret, "iv", 12)
	w.seq = 0
}

// record encrypts one TLSInnerPlaintext (content || type) as an
// application_data record (RFC 8446 §5.2).
func (w *rawTLS13Writer) record(content []byte, typ byte) []byte {
	inner := append(append([]byte{}, content...), typ)
	hdr := []byte{0x17, 0x03, 0x03, 0, 0}
	binary.BigEndian.PutUint16(hdr[3:], uint16(len(inner)+w.aead.Overhead()))
	nonce := append([]byte{}, w.iv...)
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], w.seq)
	for i := range seq {
		nonce[4+i] ^= seq[i]
	}
	w.seq++
	return w.aead.Seal(hdr, nonce, inner, hdr)
}

// keyUpdate returns the record of a KeyUpdate(update_requested) and rolls the
// sending keys, as RFC 8446 §4.6.3 requires of its sender.
func (w *rawTLS13Writer) keyUpdate() []byte {
	rec := w.record([]byte{24, 0, 0, 1, 1}, 22)
	w.setSecret(expandLabel(w.newH, w.secret, "traffic upd", len(w.secret)))
	return rec
}

// dialKeyUpdater completes a TLS 1.3 handshake and hands back a raw writer, or
// skips when the negotiated suite is not an AES-GCM one this helper speaks.
func dialKeyUpdater(t *testing.T, addr string, p *pki) (net.Conn, *rawTLS13Writer) {
	t.Helper()
	// A tiny receive buffer, set BEFORE connect so the window never grows:
	// with the kernel's autotuning (4 MiB and more on loopback) the answers
	// would pile up for many seconds before the server's write blocked.
	d := net.Dialer{Control: func(network, address string, c syscall.RawConn) error {
		var serr error
		err := c.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		})
		if err != nil {
			return err
		}
		return serr
	}}
	raw, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	var keylog bytes.Buffer
	c := tls.Client(raw, &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS13, KeyLogWriter: &keylog})
	if err := c.Handshake(); err != nil {
		t.Fatal(err)
	}
	w := &rawTLS13Writer{conn: raw}
	switch c.ConnectionState().CipherSuite {
	case tls.TLS_AES_128_GCM_SHA256:
		w.newH, w.keyLen = sha256.New, 16
	case tls.TLS_AES_256_GCM_SHA384:
		w.newH, w.keyLen = sha512.New384, 32
	default:
		t.Skipf("suite %s: this helper only speaks AES-GCM", tls.CipherSuiteName(c.ConnectionState().CipherSuite))
	}
	sc := bufio.NewScanner(&keylog)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 3 && f[0] == "CLIENT_TRAFFIC_SECRET_0" {
			secret, err := hex.DecodeString(f[2])
			if err != nil {
				t.Fatal(err)
			}
			w.setSecret(secret)
			return raw, w
		}
	}
	t.Fatal("no CLIENT_TRAFFIC_SECRET_0 in the key log")
	return nil, nil
}

// floodKeyUpdates sends KeyUpdate(update_requested) and never reads the
// answers, interleaving one octet-counting frame every 15 updates (crypto/tls
// refuses more than 16 non-advancing records in a row). It returns stalled
// once its own writes stop going through -- the server stopped reading, which
// means its reader is parked in the write of a KeyUpdate answer -- or closed
// when the server ended the connection under it.
func floodKeyUpdates(t *testing.T, raw net.Conn, w *rawTLS13Writer) (stalled, closed bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var batch []byte
	for time.Now().Before(deadline) {
		batch = batch[:0]
		for round := 0; round < 64; round++ {
			for k := 0; k < 15; k++ {
				batch = append(batch, w.keyUpdate()...)
			}
			batch = append(batch, w.record([]byte("1 x"), 23)...)
		}
		_ = raw.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := raw.Write(batch); err != nil {
			if isTimeout(err) {
				return true, false
			}
			return false, true
		}
	}
	return false, false
}

// F2 (review 2026-10-05): a TLS 1.3 peer that requests key updates and never
// reads parks the reader in crypto/tls's write of the answer. With only READ
// deadlines that write had none: idle_timeout could not end the connection and
// Stream.Close hung forever. Both deadlines are armed now, Close expires both,
// and a reader that still does not return loses its socket after closeGrace.
func TestTLSKeyUpdateFloodDoesNotWedgeClose(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 1<<20, "debug", nil)
	tr, err := NewTLS(context.Background(), TLSConfig{
		StreamConfig: StreamConfig{Addr: "127.0.0.1:0", IdleTimeout: time.Hour},
		CertFile:     p.serverCert, KeyFile: p.serverKey, ClientAuth: ClientAuthNone,
	}, h.in, h.m, logger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr.Start(ctx)
	drainCtx, stopDrain := context.WithCancel(context.Background())
	defer stopDrain()
	go func() {
		for drainCtx.Err() == nil {
			h.ring.Drain()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	raw, w := dialKeyUpdater(t, tr.Addr(), p)
	stalled, closed := floodKeyUpdates(t, raw, w)
	if closed {
		t.Fatal("the server closed the connection with idle_timeout=1h")
	}
	if !stalled {
		t.Skip("the server kept reading for 20 s: the buffers never filled")
	}

	cancel()
	done := make(chan struct{})
	go func() { tr.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeGrace + 5*time.Second):
		raw.Close() // unwedge, so the test binary can end
		<-done
		t.Fatal("Stream.Close hung on a reader parked in a TLS KeyUpdate write")
	}
	if n := h.m.ConnsActive[obs.TransportTLS].Load(); n != 0 {
		t.Fatalf("conns_active{tls} = %d after Close, want 0", n)
	}
	if h.m.MessagesTotal[obs.TransportTLS].Load() == 0 {
		t.Fatal("the interleaved frames never arrived: the test did not exercise the reader")
	}
}

// ... and idle_timeout alone ends such a connection, now that the write
// deadline is armed with the read one.
func TestTLSKeyUpdateFloodIsBoundedByIdleTimeout(t *testing.T) {
	p := newPKI(t)
	h := newHarness(t, 1<<20, "debug", nil)
	tr := startTLS(t, h, p, TLSConfig{ClientAuth: ClientAuthNone,
		StreamConfig: StreamConfig{IdleTimeout: 300 * time.Millisecond}})
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				h.ring.Drain()
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	raw, w := dialKeyUpdater(t, tr.Addr(), p)
	// Either the flood stalls and the idle deadline then ends the parked
	// write, or the deadline already ended it under the flood (broken pipe).
	if stalled, closed := floodKeyUpdates(t, raw, w); !stalled && !closed {
		t.Skip("the server kept reading for 20 s: the buffers never filled")
	}
	waitFor(t, "the idle timeout to end the wedged connection", func() bool {
		return h.m.ConnsActive[obs.TransportTLS].Load() == 0
	})
}
