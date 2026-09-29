package client

import (
	"bytes"
	"io"
	"net"
	"os"
	"time"
)

// When -dest is a SOCKS5 server at the far end, every new connection first talks SOCKS with it:
// method negotiation (one round trip over the tunnel) and CONNECT (another), before the
// application can send a byte. rabbit itself forwards without waiting, so both can be answered here
// and sent ahead of the application's data:
//
// RABBIT_SOCKS_LOCAL_GREETING=1: answer the method negotiation here ("no auth"); CONNECT still
// waits for the far server's answer.
//
// RABBIT_SOCKS_LOCAL_CONNECT=1 (implies the above): also answer CONNECT here with success right
// away. Many socks servers answer CONNECT with success before they have reached the
// target anyway, so the application learns about an unreachable target the same way as before:
// the connection closes. Other commands (UDP ASSOCIATE, BIND) only get the local greeting.
//
// The greeting and request the far server needs are sent ahead on the application's behalf and
// its answers are dropped here; a refusal closes the connection. Anything that is not a SOCKS5
// greeting offering "no auth" is relayed untouched.
var (
	socksLocalConnect  = os.Getenv("RABBIT_SOCKS_LOCAL_CONNECT") == "1"
	socksLocalGreeting = socksLocalConnect || os.Getenv("RABBIT_SOCKS_LOCAL_GREETING") == "1"
)

var (
	socksGreetingNoAuth = []byte{5, 1, 0}
	socksReplyNoAuth    = []byte{5, 0}
	socksReplyConnected = []byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0} // succeeded, bound 0.0.0.0:0
)

// socksDrop says which answers of the far server to drop before relaying.
type socksDrop struct {
	greeting, connect bool
}

// answerSocks reads the application's first bytes. It returns what to send into the tunnel before
// relaying the rest, and which of the far server's answers to drop.
func answerSocks(conn net.Conn) (prefix []byte, drop socksDrop, err error) {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	head := make([]byte, 2)
	if _, err = io.ReadFull(conn, head); err != nil {
		return nil, drop, err
	}
	if head[0] != 5 || head[1] == 0 {
		return head, drop, nil
	}
	methods := make([]byte, head[1])
	if _, err = io.ReadFull(conn, methods); err != nil {
		return nil, drop, err
	}
	if bytes.IndexByte(methods, 0) < 0 {
		return append(head, methods...), drop, nil
	}
	if _, err = conn.Write(socksReplyNoAuth); err != nil {
		return nil, drop, err
	}
	drop.greeting = true
	prefix = append([]byte(nil), socksGreetingNoAuth...)
	if !socksLocalConnect {
		return prefix, drop, nil
	}
	req, err := readSocksRequest(conn)
	if err != nil {
		return nil, drop, err
	}
	prefix = append(prefix, req...)
	if req[1] == 1 { // CONNECT
		if _, err = conn.Write(socksReplyConnected); err != nil {
			return nil, drop, err
		}
		drop.connect = true
	}
	return prefix, drop, nil
}

// readSocksRequest reads VER CMD RSV ATYP ADDR PORT.
func readSocksRequest(r io.Reader) ([]byte, error) {
	req := make([]byte, 4)
	if _, err := io.ReadFull(r, req); err != nil {
		return nil, err
	}
	rest, err := readSocksAddr(r, req[3])
	return append(req, rest...), err
}

// readSocksAddr reads an address of type atyp followed by a port.
func readSocksAddr(r io.Reader, atyp byte) ([]byte, error) {
	n := 0
	switch atyp {
	case 1:
		n = 4
	case 4:
		n = 16
	case 3:
		l := make([]byte, 1)
		if _, err := io.ReadFull(r, l); err != nil {
			return nil, err
		}
		rest := make([]byte, int(l[0])+2)
		_, err := io.ReadFull(r, rest)
		return append(l, rest...), err
	default:
		return nil, io.ErrUnexpectedEOF
	}
	buf := make([]byte, n+2)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// dropSocksReplies consumes the far server's answers to what was sent on the application's
// behalf; false if it refused.
func dropSocksReplies(r io.Reader, drop socksDrop) bool {
	if drop.greeting {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(r, buf); err != nil || !bytes.Equal(buf, socksReplyNoAuth) {
			return false
		}
	}
	if drop.connect {
		rep := make([]byte, 4)
		if _, err := io.ReadFull(r, rep); err != nil || rep[0] != 5 || rep[1] != 0 {
			return false
		}
		if _, err := readSocksAddr(r, rep[3]); err != nil {
			return false
		}
	}
	return true
}
