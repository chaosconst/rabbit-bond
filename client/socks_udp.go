package client

import (
	"io"
	"net"
	"os"

	"github.com/ihciah/rabbit-tcp/connection_pool"
)

// RABBIT_SOCKS_UDP=1 (with RABBIT_SOCKS_LOCAL_CONNECT): UDP ASSOCIATE is served here (RFC 1928
// section 7). The application gets a UDP port on the address it reached us at; each datagram it
// sends there goes through the tunnels as one datagram block, and the far end sends it out as real
// UDP. The association lasts as long as the application's TCP control connection.
var socksUDP = os.Getenv("RABBIT_SOCKS_UDP") == "1"

func (c *Client) serveUDP(ctrl net.Conn) {
	defer ctrl.Close()
	local := ctrl.LocalAddr().(*net.TCPAddr)
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: local.IP})
	if err != nil {
		_, _ = ctrl.Write([]byte{5, 1, 0, 1, 0, 0, 0, 0, 0, 0}) // general failure
		return
	}
	defer uc.Close()
	_ = uc.SetReadBuffer(connection_pool.DatagramReadBuffer)
	bound := uc.LocalAddr().(*net.UDPAddr)
	reply := append([]byte{5, 0, 0}, encodeUDPAddr(bound)...)
	if _, err := ctrl.Write(reply); err != nil {
		return
	}

	var app *net.UDPAddr // where the application sends from; replies go there
	appCh := make(chan *net.UDPAddr, 1)
	send, end := c.peer.OpenDatagram(func(frame []byte) {
		if app == nil {
			select {
			case a := <-appCh:
				app = a
			default:
				return
			}
		}
		_, _ = uc.WriteToUDP(append([]byte{0, 0, 0}, frame...), app)
	})
	defer end()

	go func() { // the association ends with the control connection
		_, _ = io.Copy(io.Discard, ctrl)
		_ = uc.Close()
	}()
	buf := make([]byte, 64*1024)
	var from0 *net.UDPAddr
	for {
		n, from, err := uc.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if from0 == nil {
			if !from.IP.Equal(ctrl.RemoteAddr().(*net.TCPAddr).IP) {
				continue // only the application that asked may use the port
			}
			from0 = from
			appCh <- from
		} else if !from.IP.Equal(from0.IP) || from.Port != from0.Port {
			continue
		}
		// RSV RSV FRAG | ATYP ADDR PORT DATA; fragments are not supported (dropped, as allowed).
		if n < 4 || buf[2] != 0 || connection_pool.FrameLen(buf[3:n]) < 0 || n-3 < connection_pool.FrameLen(buf[3:n]) {
			continue
		}
		send(append([]byte(nil), buf[3:n]...))
	}
}

func encodeUDPAddr(a *net.UDPAddr) []byte {
	var b []byte
	if ip4 := a.IP.To4(); ip4 != nil {
		b = append([]byte{1}, ip4...)
	} else {
		b = append([]byte{4}, a.IP.To16()...)
	}
	return append(b, byte(a.Port>>8), byte(a.Port))
}
