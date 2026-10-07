package client

import (
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chaosconst/rabbit-bond/connection_pool"
)

// RABBIT_SOCKS_UDP=1 (with RABBIT_SOCKS_LOCAL_CONNECT): UDP ASSOCIATE is served here (RFC 1928
// section 7). The application gets a UDP port on the address it reached us at; each datagram it
// sends there goes through the tunnels as one datagram block, and the far end sends it out as real
// UDP. The association lasts as long as the application's TCP control connection.
var socksUDP = os.Getenv("RABBIT_SOCKS_UDP") == "1"

// RABBIT_LOG_UDP=1 logs each association's opening and end: who asked, the first destination,
// datagrams each way, how long each direction had been quiet, and which side ended it. A UDP flow
// whose association is replaced mid-way leaves the far end from a new port, which servers that
// forbid migration (Cloudflare QUIC, many games) treat as a stranger.
var logUDP = os.Getenv("RABBIT_LOG_UDP") == "1"

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
	opened := time.Now()
	var up, down, lastUp, lastDown int64 // datagrams each way; unix nanos of the latest
	var first string                     // first destination
	ended := make(chan string, 1)        // why the control connection ended
	if logUDP {
		log.Printf("UDP assoc %d open: control from %v\n", bound.Port, ctrl.RemoteAddr())
		defer func() {
			why := "udp socket closed"
			select {
			case why = <-ended:
			default:
			}
			quiet := func(t int64) string {
				if t == 0 {
					return "never"
				}
				return time.Since(time.Unix(0, t)).Round(time.Millisecond).String()
			}
			log.Printf("UDP assoc %d end after %v (%s): first dst %s, up %d (quiet %s), down %d (quiet %s)\n",
				bound.Port, time.Since(opened).Round(time.Millisecond), why, first,
				atomic.LoadInt64(&up), quiet(atomic.LoadInt64(&lastUp)), atomic.LoadInt64(&down), quiet(atomic.LoadInt64(&lastDown)))
		}()
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
		atomic.AddInt64(&down, 1)
		atomic.StoreInt64(&lastDown, time.Now().UnixNano())
	})
	defer end()

	go func() { // the association ends with the control connection
		_, err := io.Copy(io.Discard, ctrl)
		if err == nil {
			ended <- "app closed the control connection"
		} else {
			ended <- "control connection: " + err.Error()
		}
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
		if logUDP {
			if first == "" {
				first = frameDst(buf[3:n])
			}
			atomic.AddInt64(&up, 1)
			atomic.StoreInt64(&lastUp, time.Now().UnixNano())
		}
		send(append([]byte(nil), buf[3:n]...))
	}
}

// frameDst prints a frame's SOCKS5 destination (ATYP ADDR PORT) as host:port.
func frameDst(f []byte) string {
	n := connection_pool.FrameLen(f)
	if n < 0 || len(f) < n {
		return "?"
	}
	var host string
	switch f[0] {
	case 1, 4:
		host = net.IP(f[1 : n-2]).String()
	case 3:
		host = string(f[2 : n-2])
	}
	return net.JoinHostPort(host, strconv.Itoa(int(f[n-2])<<8|int(f[n-1])))
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
