package connection_pool

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ihciah/rabbit-tcp/block"
	"github.com/ihciah/rabbit-tcp/tunnel_pool"
	uatomic "go.uber.org/atomic"
	"sync/atomic"
)

// UDP sessions (SOCKS5 UDP ASSOCIATE). Each datagram travels as one TypeDatagram block whose data
// is a frame: ATYP ADDR PORT DATA in SOCKS5 address format (the destination on the way out, the
// source on the way back). Datagram blocks bypass the ordered connection layer: they are delivered
// as they arrive, duplicates (a head-duplicated copy) dropped by sequence number.

const (
	datagramIdle   = 60 * time.Second // a server session with no traffic this long is closed
	datagramWindow = 1 << 16          // sequence numbers remembered for de-duplication (12 s at 50 Mbit/s)
)

type datagramSession struct {
	id      uint32
	seq     uatomic.Uint32
	deliver func(frame []byte)
	close   func()
	out     chan block.Block
	done    chan struct{}

	mu   sync.Mutex
	seen [datagramWindow / 64]uint64 // bit seq%datagramWindow: delivered, for seq in (top-window, top]
	top  uint32
	any  bool
	last time.Time
}

// fresh reports whether seq has not been delivered yet, and remembers it. A copy of a datagram can
// arrive seconds after the other one when a tunnel queues, so the window is wide.
func (s *datagramSession) fresh(seq uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = time.Now()
	if !s.any {
		s.any, s.top = true, seq
	} else if d := seq - s.top; int32(d) > 0 {
		if d >= datagramWindow {
			s.seen = [datagramWindow / 64]uint64{}
		} else {
			for i := s.top + 1; i != seq; i++ {
				s.clear(i)
			}
		}
		s.clear(seq)
		s.top = seq
	} else if s.top-seq >= datagramWindow {
		atomic.AddUint64(&tunnel_pool.DatagramTooOld, 1)
		return false
	}
	w, bit := (seq%datagramWindow)/64, uint64(1)<<(seq%64)
	if s.seen[w]&bit != 0 {
		return false
	}
	s.seen[w] |= bit
	return true
}

func (s *datagramSession) clear(seq uint32) {
	s.seen[(seq%datagramWindow)/64] &^= uint64(1) << (seq % 64)
}

func newDatagramSession(id uint32, deliver func([]byte)) *datagramSession {
	return &datagramSession{id: id, deliver: deliver, last: time.Now(),
		out: make(chan block.Block, datagramBacklog), done: make(chan struct{})}
}

// Each session queues its outgoing datagrams (datagramBacklog) and one goroutine moves them into
// the send queue, so the socket reader never waits: with Linux's default 208 KB receive buffer
// (net.core.rmem_max caps SetReadBuffer) a reader held up 33 ms at 50 Mbit/s already overflows
// the socket.
const datagramBacklog = 4096

func (cp *ConnectionPool) sendDatagram(s *datagramSession, frame []byte) {
	blk := block.NewDatagramBlock(s.id, s.seq.Inc()-1, frame)
	atomic.AddUint64(&tunnel_pool.DatagramSent, 1)
	select {
	case s.out <- blk:
	default: // backlog full: drop, as a congested UDP path would
		atomic.AddUint64(&tunnel_pool.DatagramQueueDrop, 1)
	}
}

func (cp *ConnectionPool) pumpDatagrams(s *datagramSession) {
	for {
		select {
		case blk := <-s.out:
			select {
			case cp.sendQueue <- blk:
			case <-s.done:
				return
			case <-cp.ctx.Done():
				return
			}
		case <-s.done:
			return
		case <-cp.ctx.Done():
			return
		}
	}
}

// DatagramReadBuffer is the socket receive buffer of UDP sockets carrying sessions.
const DatagramReadBuffer = 4 << 20

// OpenDatagram starts a client UDP session; deliver gets each frame coming back. It returns the
// function sending a frame and the one ending the session.
func (cp *ConnectionPool) OpenDatagram(deliver func(frame []byte)) (send func(frame []byte), end func()) {
	s := newDatagramSession(rand.Uint32(), deliver)
	cp.mappingLock.Lock()
	cp.datagrams[s.id] = s
	cp.mappingLock.Unlock()
	go cp.pumpDatagrams(s)
	return func(frame []byte) { cp.sendDatagram(s, frame) }, func() { cp.removeDatagram(s.id) }
}

func (cp *ConnectionPool) removeDatagram(id uint32) {
	cp.mappingLock.Lock()
	s, ok := cp.datagrams[id]
	delete(cp.datagrams, id)
	cp.closed[id] = time.Now()
	cp.mappingLock.Unlock()
	if ok {
		close(s.done)
		if s.close != nil {
			s.close()
		}
	}
}

// recvDatagram hands a datagram block to its session, opening a server session for a new one.
func (cp *ConnectionPool) recvDatagram(blk block.Block) {
	cp.mappingLock.RLock()
	s, ok := cp.datagrams[blk.ConnectionID]
	at, gone := cp.closed[blk.ConnectionID]
	cp.mappingLock.RUnlock()
	if !ok {
		if !cp.acceptNewConnection || (gone && time.Since(at) < closedMemory) {
			return
		}
		var err error
		if s, err = cp.openServerDatagram(blk.ConnectionID); err != nil {
			cp.logger.Warnf("UDP session %d: %v\n", blk.ConnectionID, err)
			return
		}
	}
	if s.fresh(blk.BlockID) {
		atomic.AddUint64(&tunnel_pool.DatagramRecv, 1)
		s.deliver(blk.BlockData)
	}
}

// openServerDatagram: the far end of a session sends frames out as real UDP from one socket and
// frames what comes back with its source address.
func (cp *ConnectionPool) openServerDatagram(id uint32) (*datagramSession, error) {
	uc, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	_ = uc.SetReadBuffer(DatagramReadBuffer)
	var resolved sync.Map // "host:port" -> *net.UDPAddr
	s := newDatagramSession(id, func(frame []byte) {
		dst, data, err := parseFrame(frame)
		if err != nil {
			return
		}
		if addr, ok := resolved.Load(dst); ok {
			_, _ = uc.WriteToUDP(data, addr.(*net.UDPAddr))
			return
		}
		data = append([]byte(nil), data...)
		go func() { // resolving must not hold up the receive loop that calls deliver
			a, err := net.ResolveUDPAddr("udp", dst)
			if err != nil {
				return
			}
			resolved.Store(dst, a)
			_, _ = uc.WriteToUDP(data, a)
		}()
	})
	s.close = func() { _ = uc.Close() }
	cp.mappingLock.Lock()
	if old, ok := cp.datagrams[id]; ok { // opened concurrently
		cp.mappingLock.Unlock()
		_ = uc.Close()
		return old, nil
	}
	cp.datagrams[id] = s
	cp.mappingLock.Unlock()
	go cp.pumpDatagrams(s)
	go func() {
		buf := make([]byte, 64*1024)
		for {
			_ = uc.SetReadDeadline(time.Now().Add(datagramIdle))
			n, from, err := uc.ReadFromUDP(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					s.mu.Lock()
					idle := time.Since(s.last) >= datagramIdle
					s.mu.Unlock()
					if !idle {
						continue
					}
				}
				cp.removeDatagram(id)
				return
			}
			if n > block.DataSize-19 {
				continue // does not fit in one block
			}
			s.mu.Lock()
			s.last = time.Now()
			s.mu.Unlock()
			cp.sendDatagram(s, append(encodeAddr(from), buf[:n]...))
		}
	}()
	return s, nil
}

// Frames use the SOCKS5 address format: ATYP (1 IPv4, 3 domain, 4 IPv6) ADDR PORT.

func parseFrame(f []byte) (dst string, data []byte, err error) {
	if len(f) < 1 {
		return "", nil, errFrame
	}
	var host string
	i := 1
	switch f[0] {
	case 1:
		if len(f) < 7 {
			return "", nil, errFrame
		}
		host, i = net.IP(f[1:5]).String(), 5
	case 4:
		if len(f) < 19 {
			return "", nil, errFrame
		}
		host, i = net.IP(f[1:17]).String(), 17
	case 3:
		if len(f) < 2 || len(f) < 2+int(f[1])+2 {
			return "", nil, errFrame
		}
		host, i = string(f[2:2+int(f[1])]), 2+int(f[1])
	default:
		return "", nil, errFrame
	}
	port := binary.BigEndian.Uint16(f[i : i+2])
	return net.JoinHostPort(host, strconv.Itoa(int(port))), f[i+2:], nil
}

// FrameLen is the length of the address part at the head of a frame, or -1.
func FrameLen(f []byte) int {
	if len(f) < 1 {
		return -1
	}
	switch f[0] {
	case 1:
		return 7
	case 4:
		return 19
	case 3:
		if len(f) < 2 {
			return -1
		}
		return 4 + int(f[1])
	}
	return -1
}

func encodeAddr(a *net.UDPAddr) []byte {
	var b []byte
	if ip4 := a.IP.To4(); ip4 != nil {
		b = append([]byte{1}, ip4...)
	} else {
		b = append([]byte{4}, a.IP.To16()...)
	}
	return binary.BigEndian.AppendUint16(b, uint16(a.Port))
}

var errFrame = errors.New("bad datagram frame")
