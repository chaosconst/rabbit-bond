package tunnel_pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/chaosconst/rabbit-bond/block"
	"github.com/chaosconst/rabbit-bond/logger"
	"github.com/chaosconst/rabbit-bond/trace"
	"github.com/chaosconst/rabbit-bond/tunnel"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

type Tunnel struct {
	net.Conn
	ctx      context.Context
	cancel   context.CancelFunc
	tunnelID uint32
	peerID   uint32
	logger   *logger.Logger
	flow     *flowState // nil unless FlowControl
	pool     *TunnelPool
	endpoint string // client side: which -rabbit-addr endpoint (network path) this tunnel was dialed to
	line     string // exit line of the path (both sides; see dup.go)
	created  time.Time
	dgramQ   chan block.Block // datagrams of the UDP sessions pinned here (dgram_pin.go); nil if off

	closeNoted int32 // a close reason was recorded (closes.go)
	lastRecv   int64 // unix nanos of the last block read
	copies     int32 // second copies waiting to be written here (dup.go)
}

// Create a new tunnel from a net.Conn and cipher with random tunnelID
func NewActiveTunnel(conn net.Conn, ciph tunnel.Cipher, peerID uint32) (Tunnel, error) {
	tun := newTunnelWithID(conn, ciph, peerID)
	// A path that accepts the connection and then hangs (no signal, a relay dropping packets)
	// must not block the dialer forever.
	conn.SetDeadline(time.Now().Add(HandshakeTimeoutSec * time.Second))
	err := tun.activeExchangePeerID()
	conn.SetDeadline(time.Time{})
	return tun, err
}

func NewPassiveTunnel(conn net.Conn, ciph tunnel.Cipher) (Tunnel, error) {
	tun := newTunnelWithID(conn, ciph, 0)
	conn.SetDeadline(time.Now().Add(HandshakeTimeoutSec * time.Second)) // as NewActiveTunnel
	err := tun.passiveExchangePeerID()
	conn.SetDeadline(time.Time{})
	return tun, err
}

// Create a new tunnel from a net.Conn and cipher with given tunnelID
func newTunnelWithID(conn net.Conn, ciph tunnel.Cipher, peerID uint32) Tunnel {
	tunnelID := rand.Uint32()
	tun := Tunnel{
		Conn:     tunnel.NewEncryptedConn(conn, ciph),
		peerID:   peerID,
		tunnelID: tunnelID,
		logger:   logger.NewLogger(fmt.Sprintf("[Tunnel-%d]", tunnelID)),
		created:  time.Now(),
	}
	if FlowControl {
		tun.flow = newFlowState()
	}
	tun.logger.Infoln("Tunnel created.")
	return tun
}

func (tunnel *Tunnel) activeExchangePeerID() (err error) {
	err = tunnel.sendPeerID(tunnel.peerID)
	if err != nil {
		tunnel.logger.Errorf("Cannot exchange peerID(send failed: %v).\n", err)
		return err
	}
	peerID, err := tunnel.recvPeerID()
	if err != nil {
		tunnel.logger.Errorf("Cannot exchange peerID(recv failed: %v).\n", err)
		return err
	}
	if tunnel.peerID != peerID {
		tunnel.logger.Errorf("Cannot exchange peerID(local: %d, remote: %d).\n", tunnel.peerID, peerID)
		return errors.New("invalid exchanging")
	}
	tunnel.logger.Infoln("PeerID exchange successfully.")
	return
}

func (tunnel *Tunnel) passiveExchangePeerID() (err error) {
	peerID, err := tunnel.recvPeerID()
	if err != nil {
		tunnel.logger.Errorf("Cannot exchange peerID(recv failed: %v).\n", err)
		return err
	}
	err = tunnel.sendPeerID(peerID)
	if err != nil {
		tunnel.logger.Errorf("Cannot exchange peerID(send failed: %v).\n", err)
		return err
	}
	tunnel.peerID = peerID
	tunnel.logger.Infoln("PeerID exchange successfully.")
	return
}

func (tunnel *Tunnel) sendPeerID(peerID uint32) error {
	peerIDBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(peerIDBuffer, peerID)
	_, err := io.CopyN(tunnel.Conn, bytes.NewReader(peerIDBuffer), 4)
	if err != nil {
		tunnel.logger.Errorf("Peer id sent with error:%v.\n", err)
		return err
	}
	tunnel.logger.Infoln("Peer id sent.")
	return nil
}

func (tunnel *Tunnel) recvPeerID() (uint32, error) {
	peerIDBuffer := make([]byte, 4)
	_, err := io.ReadFull(tunnel.Conn, peerIDBuffer)
	if err != nil {
		tunnel.logger.Errorf("Peer id recv with error:%v.\n", err)
		return 0, err
	}
	peerID := binary.LittleEndian.Uint32(peerIDBuffer)
	tunnel.logger.Infoln("Peer id recv.")
	return peerID, nil
}

// Read block from send channel, pack it and send
func (tunnel *Tunnel) OutboundRelay(normalQueue, retryQueue chan block.Block) {
	tunnel.logger.Infoln("Outbound relay started.")
	if tunnel.flow != nil {
		defer tunnel.requeuePending(retryQueue)
		go tunnel.logFlowStats()
		if tunnel.endpoint != "" && flowProbeEvery > 0 { // client side: it is the one that re-dials
			go tunnel.probeLoop()
		}
	}
	for {
		// cancel is of highest priority
		select {
		case <-tunnel.ctx.Done():
			return
		default:
		}
		if tunnel.flow != nil {
			sched, ok := tunnel.waitWindow()
			if !ok {
				return
			}
			if !sched { // not a good choice for shared blocks, but its own datagrams must go
				tunnel.packThenSendDgrams(<-tunnel.dgramQ, retryQueue)
				continue
			}
		}
		// retryQueue is of secondary highest priority
		select {
		case <-tunnel.ctx.Done():
			return
		case blk := <-retryQueue:
			tunnel.packThenSend(blk, retryQueue)
		default:
		}
		// normalQueue is of secondary highest priority
		select {
		case <-tunnel.ctx.Done():
			return
		case blk := <-retryQueue:
			tunnel.packThenSend(blk, retryQueue)
		case blk := <-tunnel.dgramQ:
			tunnel.packThenSendDgrams(blk, retryQueue)
		case blk := <-normalQueue:
			tunnel.packThenSend(blk, retryQueue)
		}
	}
}

func (tunnel *Tunnel) packThenSend(blk block.Block, retryQueue chan block.Block) {
	if tunnel.send(blk, retryQueue) && tunnel.pool != nil && wantDup(blk) {
		if other := tunnel.pool.dupTunnel(tunnel); other != nil {
			other.sendCopy(blk)
		}
	}
}

// RABBIT_FLOW_DGRAM_BATCH > 1: datagrams already waiting in dgramQ go out together with the first
// one, up to that many blocks (and one 16 KB record), in a single write. One write per datagram
// cost one syscall and one TLS record per ~1.2 KB packet on both hops and capped a UDP session at
// ~140 Mbit/s (lab 2026-10-01). A lone datagram still leaves at once, so latency is unchanged.
var flowDgramBatch = envInt("RABBIT_FLOW_DGRAM_BATCH", 1)

const dgramBatchBytes = 16 * 1024

func (tunnel *Tunnel) packThenSendDgrams(first block.Block, retryQueue chan block.Block) {
	if flowDgramBatch <= 1 {
		tunnel.packThenSend(first, retryQueue)
		return
	}
	blks := []block.Block{first}
	size := block.HeaderSize + len(first.BlockData)
drain:
	for len(blks) < flowDgramBatch && size < dgramBatchBytes {
		select {
		case blk := <-tunnel.dgramQ:
			blks = append(blks, blk)
			size += block.HeaderSize + len(blk.BlockData)
		default:
			break drain
		}
	}
	if len(blks) == 1 {
		tunnel.packThenSend(first, retryQueue)
		return
	}
	if !tunnel.sendBatch(blks, retryQueue) || tunnel.pool == nil {
		return
	}
	for _, blk := range blks {
		if wantDup(blk) {
			if other := tunnel.pool.dupTunnel(tunnel); other != nil {
				other.sendCopy(blk)
			}
		}
	}
}

// sendBatch writes blocks with one write; false if the tunnel failed (all of them are re-queued).
func (tunnel *Tunnel) sendBatch(blks []block.Block, retryQueue chan block.Block) bool {
	packed := make([][]byte, len(blks))
	total := 0
	for i := range blks {
		packed[i] = blks[i].Pack()
		total += len(packed[i])
	}
	buf := make([]byte, 0, total)
	for _, p := range packed {
		buf = append(buf, p...)
	}
	if tunnel.flow != nil {
		tunnel.flow.writeMu.Lock()
		defer tunnel.flow.writeMu.Unlock()
	}
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	n, err := tunnel.Conn.Write(buf)
	atomic.AddUint64(&BytesSent, uint64(n))
	if err != nil || n != total {
		tunnel.logger.Warnf("Error when send bytes to tunnel: (n: %d, error: %v).\n", n, err)
		tunnel.closeWhy(fmt.Sprintf("write: %v (%d/%d bytes)", err, n, total))
		for _, blk := range blks {
			atomic.AddUint64(&ResendBytes, blockBytes(blk))
		}
		go func() {
			for _, blk := range blks {
				retryQueue <- blk
			}
		}()
		return false
	}
	tunnel.Conn.SetWriteDeadline(time.Time{})
	if tunnel.flow != nil {
		for i, blk := range blks {
			tunnel.flow.onSent(blk, len(packed[i]))
		}
	}
	return true
}

// send writes one block to the tunnel; false if the tunnel failed (the block is re-queued).
func (tunnel *Tunnel) send(blk block.Block, retryQueue chan block.Block) bool {
	dataToSend := blk.Pack()
	reader := bytes.NewReader(dataToSend)

	if tunnel.flow != nil {
		tunnel.flow.writeMu.Lock()
		defer tunnel.flow.writeMu.Unlock()
	}
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	n, err := io.Copy(tunnel.Conn, reader)
	atomic.AddUint64(&BytesSent, uint64(n))
	if err != nil || n != int64(len(dataToSend)) {
		tunnel.logger.Warnf("Error when send bytes to tunnel: (n: %d, error: %v).\n", n, err)
		// Tunnel down and message has not been fully sent.
		tunnel.closeWhy(fmt.Sprintf("write: %v (%d/%d bytes)", err, n, len(dataToSend)))
		atomic.AddUint64(&ResendBytes, blockBytes(blk))
		go func() {
			retryQueue <- blk
		}()
		// Use new goroutine to avoid channel blocked
		return false
	} else {
		tunnel.Conn.SetWriteDeadline(time.Time{})
		if tunnel.flow != nil {
			tunnel.flow.onSent(blk, len(dataToSend))
		}
		if trace.On && blk.Type <= block.TypeData && blk.BlockID < trace.FirstBlocks {
			trace.Logf("tx cid=%d bid=%d type=%d len=%d tunnel=%d ep=%s", blk.ConnectionID, blk.BlockID, blk.Type, len(blk.BlockData), tunnel.tunnelID, tunnel.endpoint)
		}
		tunnel.logger.Debugf("Copied data to tunnel successfully(n: %d).\n", n)
	}
	return true
}

// Read bytes from connection, parse it to block then put in recv channel
func (tunnel *Tunnel) InboundRelay(output chan<- block.Block) {
	tunnel.logger.Infoln("Inbound relay started.")
	for {
		select {
		case <-tunnel.ctx.Done():
			// Should read all before leave, or packet will be lost
			for {
				// Will never be blocked because the tunnel is closed
				blk, err := block.NewBlockFromReader(tunnel.Conn)
				if err == nil {
					if blk.Type == block.TypeAck || blk.Type == block.TypePing {
						continue
					}
					tunnel.logger.Debugf("Block received from tunnel(type: %d) successfully after close.\n", blk.Type)
					output <- *blk
				} else {
					tunnel.logger.Debugf("Error when receiving block from tunnel after close: %v.\n", err)
					break
				}
			}
			return
		default:
			blk, err := block.NewBlockFromReader(tunnel.Conn)
			if err != nil {
				// Server will never close connection in normal cases
				tunnel.logger.Errorf("Error when receiving block from tunnel: %v.\n", err)
				// Tunnel down and message has not been fully read.
				tunnel.closeWhy("read: " + err.Error())
			} else if atomic.StoreInt64(&tunnel.lastRecv, time.Now().UnixNano()); tunnel.handleFlowBlock(blk) {
				if trace.On && blk.Type <= block.TypeData && blk.BlockID < trace.FirstBlocks {
					trace.Logf("rx cid=%d bid=%d type=%d len=%d tunnel=%d ep=%s", blk.ConnectionID, blk.BlockID, blk.Type, len(blk.BlockData), tunnel.tunnelID, tunnel.endpoint)
				}
				tunnel.logger.Debugf("Block received from tunnel(type: %d)successfully.\n", blk.Type)
				select {
				case output <- *blk:
				default: // the connection layer is behind: don't sit on a held ack meanwhile
					if tunnel.flow != nil && tunnel.flow.unacked > 0 {
						tunnel.flow.unacked = 0
						tunnel.sendAck()
					}
					output <- *blk
				}
			}
		}
	}
}

func (tunnel *Tunnel) GetPeerID() uint32 {
	return tunnel.peerID
}

func (tunnel *Tunnel) closeThenCancel() {
	tunnel.Close()
	tunnel.cancel()
}

// handleFlowBlock consumes acks and acknowledges everything else. It returns
// whether blk should be passed on to the connection layer.
func (tunnel *Tunnel) handleFlowBlock(blk *block.Block) bool {
	if tunnel.flow == nil {
		return true
	}
	if blk.Type == block.TypeAck {
		tunnel.flow.onAck(blk.BlockID)
		tunnel.ackMaybe(false)
		return false
	}
	if blk.Type == block.TypeLine {
		tunnel.ackMaybe(false)
		if tunnel.pool != nil { // dupTunnel reads endpoints under the pool lock
			tunnel.pool.mutex.Lock()
			defer tunnel.pool.mutex.Unlock()
		}
		tunnel.line = string(blk.BlockData)
		return false
	}
	if blk.Type == block.TypePing {
		tunnel.flow.recv += uint64(block.HeaderSize) + uint64(blk.BlockLength)
		tunnel.ackMaybe(true)
		return false
	}
	if blk.Type == block.TypeNack {
		tunnel.flow.recv += uint64(block.HeaderSize) + uint64(blk.BlockLength)
		tunnel.flow.unacked = 0
		tunnel.sendAck()
		if tunnel.pool != nil {
			tunnel.pool.resendForNack(blk.ConnectionID, blk.BlockID)
		}
		return false
	}
	tunnel.flow.recv += uint64(block.HeaderSize) + uint64(blk.BlockLength)
	tunnel.ackMaybe(true)
	return true
}

// RABBIT_FLOW_ACK_BATCH > 1: while more received bytes are already buffered (the next block is
// here), the ack waits for it, up to that many blocks. Acks are cumulative, so one ack at the end
// of a burst says the same as one per block; with small blocks (datagrams) per-block acks were
// one write each and doubled the reverse traffic. When the buffer runs dry the ack goes at once.
var flowAckBatch = envInt("RABBIT_FLOW_ACK_BATCH", 1)

// ackMaybe is called for every block read (data: a block to acknowledge). A held ack must also go
// out when the buffered block it waited for turns out to be the peer's ack: nothing else may
// follow (the peer's window is full, waiting for exactly this ack), and both ends would stall.
func (tunnel *Tunnel) ackMaybe(data bool) {
	if data {
		tunnel.flow.unacked++
	}
	if tunnel.flow.unacked == 0 {
		return
	}
	if flowAckBatch > 1 && tunnel.flow.unacked < flowAckBatch {
		if b, ok := tunnel.Conn.(interface{ Buffered() int }); ok && b.Buffered() > 0 {
			return
		}
	}
	tunnel.flow.unacked = 0
	tunnel.sendAck()
}

func (tunnel *Tunnel) sendAck() {
	ack := block.NewAckBlock(uint32(tunnel.flow.recv))
	tunnel.flow.writeMu.Lock()
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	_, err := tunnel.Conn.Write(ack.Pack())
	tunnel.Conn.SetWriteDeadline(time.Time{})
	tunnel.flow.writeMu.Unlock()
	if err != nil {
		tunnel.logger.Warnf("Error when send ack to tunnel: %v.\n", err)
		tunnel.closeWhy("ack write: " + err.Error())
	}
}

// waitWindow blocks until this tunnel has window and is a good choice for the next
// block (sched), or has window and datagrams of its own pinned sessions waiting.
// Other tunnels' acks change the answer too, hence the short poll.
func (tunnel *Tunnel) waitWindow() (sched, ok bool) {
	timer := time.NewTimer(flowPollEvery)
	defer timer.Stop()
	for {
		if tunnel.flow.canSend() {
			if tunnel.pool.schedulable(tunnel) {
				return true, true
			}
			if len(tunnel.dgramQ) > 0 {
				return false, true
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(flowPollEvery)
		select {
		case <-tunnel.flow.ackCh:
		case <-timer.C:
		case <-tunnel.ctx.Done():
			return false, false
		}
	}
}

// requeuePending hands un-acknowledged blocks of a dead tunnel to the other tunnels;
// the receiver drops duplicates, so re-sending a block that did arrive is harmless.
func (tunnel *Tunnel) requeuePending(retryQueue chan block.Block) {
	blocks := tunnel.flow.takePending()
	if len(blocks) == 0 {
		return
	}
	tunnel.logger.Infof("Re-queue %d un-acknowledged blocks.\n", len(blocks))
	addResend(blocks)
	go func() {
		for _, blk := range blocks {
			retryQueue <- blk
		}
	}()
}

func (tunnel *Tunnel) logFlowStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-tunnel.ctx.Done():
			return
		case <-ticker.C:
			cwnd, inflight, rate, rtt := tunnel.flow.stats()
			tunnel.logger.Infof("flow cwnd=%dKB inflight=%dKB rate=%.1fMbps minRTT=%dms\n",
				cwnd/1024, inflight/1024, rate*8/1e6, rtt.Milliseconds())
		}
	}
}
