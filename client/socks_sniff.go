package client

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// An application behind a local DNS (a phone's VPN app, a TUN) often asks for an IPv6 address: the
// device has IPv6, and DNS answers AAAA whoever asks. Many exits have no IPv6 (an AWS instance by
// default), so the far SOCKS server cannot reach it. With CONNECT answered here the application
// already saw success, so it never falls back to IPv4: the connection just dies (pages without
// images, 2026-10-01).
//
// So for CONNECT to an IPv6 address, read the application's first bytes (it speaks first right after
// the success reply) and, if they name the host (TLS server name, HTTP Host), ask the far server for
// that name instead; it resolves it and reaches whichever family it has. Where the exit has IPv6 the
// name leads to the same service. RABBIT_SOCKS_SNIFF_V6=0 turns this off.
var socksSniffV6 = os.Getenv("RABBIT_SOCKS_SNIFF_V6") != "0"

// sniffWait bounds the wait for the application's first bytes (protocols where the server speaks
// first send nothing; they go on to the address as asked).
const sniffWait = 300 * time.Millisecond

// sniffRequest reads the application's first bytes after a CONNECT to req's IPv6 address. It returns
// the request to send (by name when one was found) and the bytes read, which go after it.
func sniffRequest(conn net.Conn, req []byte) ([]byte, []byte) {
	if !socksSniffV6 || len(req) != 4+16+2 || req[1] != 1 || req[3] != 4 {
		return req, nil
	}
	data := readFirst(conn)
	host := sniffHost(data)
	if host == "" {
		return req, data
	}
	named := append([]byte{5, 1, 0, 3, byte(len(host))}, host...)
	return append(named, req[len(req)-2:]...), data
}

// readFirst reads what the application sends first: a whole TLS record when it starts one, else a
// single read.
func readFirst(conn net.Conn) []byte {
	_ = conn.SetReadDeadline(time.Now().Add(sniffWait))
	defer conn.SetReadDeadline(time.Time{})
	var buf []byte
	chunk := make([]byte, maxTLSRecord)
	for {
		n, err := conn.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if err != nil || !partialTLSRecord(buf) {
			return buf
		}
	}
}

const maxTLSRecord = 5 + 16*1024

// partialTLSRecord: b starts a TLS handshake record that has not fully arrived.
func partialTLSRecord(b []byte) bool {
	if len(b) == 0 || b[0] != 0x16 || len(b) >= maxTLSRecord {
		return false
	}
	return len(b) < 5 || len(b) < 5+(int(b[3])<<8|int(b[4]))
}

// sniffHost returns the host name a TLS ClientHello or an HTTP request names, or "".
func sniffHost(b []byte) string {
	var host string
	if len(b) > 0 && b[0] == 0x16 {
		host = tlsServerName(b)
	} else {
		host = httpHost(b)
	}
	if host == "" || len(host) > 255 || net.ParseIP(host) != nil {
		return ""
	}
	return host
}

var errSniffed = errors.New("sniffed")

// tlsServerName parses a ClientHello with crypto/tls and returns its server name.
func tlsServerName(b []byte) (name string) {
	_ = tls.Server(replayConn{r: bytes.NewReader(b)}, &tls.Config{
		GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
			name = h.ServerName
			return nil, errSniffed
		},
	}).Handshake()
	return name
}

// httpHost returns the Host header of an HTTP/1 request head, without its port.
func httpHost(b []byte) string {
	head := string(b)
	if i := strings.Index(head, "\r\n\r\n"); i >= 0 {
		head = head[:i]
	}
	lines := strings.Split(head, "\r\n")
	if len(lines) < 2 || !strings.Contains(lines[0], " HTTP/1.") {
		return ""
	}
	for _, l := range lines[1:] {
		if k, v, ok := strings.Cut(l, ":"); ok && strings.EqualFold(k, "host") {
			v = strings.TrimSpace(v)
			if h, _, err := net.SplitHostPort(v); err == nil {
				return h
			}
			return v
		}
	}
	return ""
}

// replayConn feeds recorded bytes to crypto/tls and swallows what it writes.
type replayConn struct {
	net.Conn
	r io.Reader
}

func (c replayConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c replayConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c replayConn) Close() error                     { return nil }
func (c replayConn) SetDeadline(time.Time) error      { return nil }
func (c replayConn) SetReadDeadline(time.Time) error  { return nil }
func (c replayConn) SetWriteDeadline(time.Time) error { return nil }
func (c replayConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c replayConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
