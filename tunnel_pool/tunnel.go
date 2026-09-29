package tunnel_pool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ihciah/rabbit-tcp/block"
	"github.com/ihciah/rabbit-tcp/logger"
	"github.com/ihciah/rabbit-tcp/tunnel"
	"io"
	"math/rand"
	"net"
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
	created  time.Time
}

// Create a new tunnel from a net.Conn and cipher with random tunnelID
func NewActiveTunnel(conn net.Conn, ciph tunnel.Cipher, peerID uint32) (Tunnel, error) {
	tun := newTunnelWithID(conn, ciph, peerID)
	return tun, tun.activeExchangePeerID()
}

func NewPassiveTunnel(conn net.Conn, ciph tunnel.Cipher) (Tunnel, error) {
	tun := newTunnelWithID(conn, ciph, 0)
	return tun, tun.passiveExchangePeerID()
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
	}
	for {
		// cancel is of highest priority
		select {
		case <-tunnel.ctx.Done():
			return
		default:
		}
		if tunnel.flow != nil && !tunnel.waitWindow() {
			return
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
		case blk := <-normalQueue:
			tunnel.packThenSend(blk, retryQueue)
		}
	}
}

func (tunnel *Tunnel) packThenSend(blk block.Block, retryQueue chan block.Block) {
	dataToSend := blk.Pack()
	reader := bytes.NewReader(dataToSend)

	if tunnel.flow != nil {
		tunnel.flow.writeMu.Lock()
		defer tunnel.flow.writeMu.Unlock()
	}
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	n, err := io.Copy(tunnel.Conn, reader)
	if err != nil || n != int64(len(dataToSend)) {
		tunnel.logger.Warnf("Error when send bytes to tunnel: (n: %d, error: %v).\n", n, err)
		// Tunnel down and message has not been fully sent.
		tunnel.closeThenCancel()
		go func() {
			retryQueue <- blk
		}()
		// Use new goroutine to avoid channel blocked
	} else {
		tunnel.Conn.SetWriteDeadline(time.Time{})
		if tunnel.flow != nil {
			tunnel.flow.onSent(blk, len(dataToSend))
		}
		tunnel.logger.Debugf("Copied data to tunnel successfully(n: %d).\n", n)
	}
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
					if blk.Type == block.TypeAck {
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
				tunnel.closeThenCancel()
			} else if tunnel.handleFlowBlock(blk) {
				tunnel.logger.Debugf("Block received from tunnel(type: %d)successfully.\n", blk.Type)
				output <- *blk
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
		return false
	}
	if blk.Type == block.TypeNack {
		tunnel.flow.recv += uint64(block.HeaderSize) + uint64(blk.BlockLength)
		tunnel.sendAck()
		if tunnel.pool != nil {
			tunnel.pool.resendForNack(blk.ConnectionID, blk.BlockID)
		}
		return false
	}
	tunnel.flow.recv += uint64(block.HeaderSize) + uint64(blk.BlockLength)
	tunnel.sendAck()
	return true
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
		tunnel.closeThenCancel()
	}
}

// waitWindow blocks until this tunnel has window and is a good choice for the next
// block. Other tunnels' acks change the answer too, hence the short poll.
func (tunnel *Tunnel) waitWindow() bool {
	timer := time.NewTimer(flowPollEvery)
	defer timer.Stop()
	for !tunnel.flow.canSend() || !tunnel.pool.schedulable(tunnel) {
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
			return false
		}
	}
	return true
}

// requeuePending hands un-acknowledged blocks of a dead tunnel to the other tunnels;
// the receiver drops duplicates, so re-sending a block that did arrive is harmless.
func (tunnel *Tunnel) requeuePending(retryQueue chan block.Block) {
	blocks := tunnel.flow.takePending()
	if len(blocks) == 0 {
		return
	}
	tunnel.logger.Infof("Re-queue %d un-acknowledged blocks.\n", len(blocks))
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
