package tunnel_pool

import (
	"context"
	"github.com/ihciah/rabbit-tcp/block"
	"github.com/ihciah/rabbit-tcp/logger"
	"sync"
	"sync/atomic"
	"time"
)

type TunnelPool struct {
	mutex          sync.Mutex
	tunnelMapping  map[uint32]*Tunnel
	peerID         uint32
	manager        Manager
	sendQueue      chan block.Block
	sendRetryQueue chan block.Block
	recvQueue      chan block.Block
	ctx            context.Context
	cancel         context.CancelFunc // currently useless
	logger         *logger.Logger
}

func NewTunnelPool(peerID uint32, manager Manager, peerContext context.Context) *TunnelPool {
	ctx, cancel := context.WithCancel(peerContext)
	tp := &TunnelPool{
		tunnelMapping:  make(map[uint32]*Tunnel),
		peerID:         peerID,
		manager:        manager,
		sendQueue:      make(chan block.Block, SendQueueSize),
		sendRetryQueue: make(chan block.Block, SendQueueSize),
		recvQueue:      make(chan block.Block, RecvQueueSize),
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger.NewLogger("[TunnelPool]"),
	}
	tp.logger.Infof("Tunnel Pool of peer %d created.\n", peerID)
	go manager.DecreaseNotify(tp)
	registerPool(tp)
	if FlowControl && flowReinjectOn {
		go tp.reinjectLoop()
	}
	return tp
}

// schedulable reports whether t should take the next block now: only if it is
// expected to deliver it about as early as the best tunnel (earliest completion
// first). Stuck tunnels are skipped.
func (tp *TunnelPool) schedulable(t *Tunnel) bool {
	if !flowSchedOn {
		return true
	}
	now := time.Now()
	mine, stuck := t.flow.estimate(block.MaxSize, now)
	if stuck {
		return false
	}
	best := mine
	tp.mutex.Lock()
	for _, other := range tp.tunnelMapping {
		if other == t || other.flow == nil {
			continue
		}
		if est, stuck := other.flow.estimate(block.MaxSize, now); !stuck && est < best {
			best = est
		}
	}
	tp.mutex.Unlock()
	return float64(mine) <= float64(best)*flowSchedRatio+float64(flowSchedSlack)
}

// reinjectLoop re-sends blocks stuck on one tunnel through the others. It only acts
// while some tunnel is healthy; if all are stuck the receiver is probably just slow.
func (tp *TunnelPool) reinjectLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-tp.ctx.Done():
			return
		case now := <-ticker.C:
			tp.mutex.Lock()
			tunnels := make([]*Tunnel, 0, len(tp.tunnelMapping))
			for _, t := range tp.tunnelMapping {
				if t.flow != nil {
					tunnels = append(tunnels, t)
				}
			}
			tp.mutex.Unlock()
			healthy := false
			for _, t := range tunnels {
				if _, stuck := t.flow.estimate(0, now); !stuck {
					healthy = true
					break
				}
			}
			if !healthy {
				continue
			}
			for _, t := range tunnels {
				for _, blk := range t.flow.stuckBlocks(now) {
					atomic.AddUint64(&reinjectCnt, uint64(len(blk.Pack())))
					select {
					case tp.sendRetryQueue <- blk:
					default:
						go func(b block.Block) { tp.sendRetryQueue <- b }(blk)
					}
				}
			}
		}
	}
}

// Add a tunnel to tunnelPool and start bi-relay
func (tp *TunnelPool) AddTunnel(tunnel *Tunnel) {
	tp.logger.Infof("Tunnel %d added to Peer %d.\n", tunnel.tunnelID, tp.peerID)
	tp.mutex.Lock()
	defer tp.mutex.Unlock()

	tunnel.pool = tp
	tp.tunnelMapping[tunnel.tunnelID] = tunnel
	tp.manager.Notify(tp)

	tunnel.ctx, tunnel.cancel = context.WithCancel(tp.ctx)
	go func() {
		<-tunnel.ctx.Done()
		tp.RemoveTunnel(tunnel)
	}()

	go tunnel.OutboundRelay(tp.sendQueue, tp.sendRetryQueue)
	go tunnel.InboundRelay(tp.recvQueue)
}

// Remove a tunnel from tunnelPool and stop bi-relay
func (tp *TunnelPool) RemoveTunnel(tunnel *Tunnel) {
	tp.logger.Infof("Tunnel %d to peer %d removed from pool.\n", tunnel.tunnelID, tunnel.peerID)
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	if tunnel, ok := tp.tunnelMapping[tunnel.tunnelID]; ok {
		delete(tp.tunnelMapping, tunnel.tunnelID)
		tp.manager.Notify(tp)
		go tp.manager.DecreaseNotify(tp)
	}
}

func (tp *TunnelPool) GetSendQueue() chan block.Block {
	return tp.sendQueue
}

func (tp *TunnelPool) GetRecvQueue() chan block.Block {
	return tp.recvQueue
}

// NackResent counts blocks re-sent because the receiver reported a hole (for /debug/flow).
var NackResent uint64

// resendForNack re-queues the named block if some tunnel still holds it un-acknowledged.
// A block already acked reached the receiver's rabbit, so the hole will close by itself.
func (tp *TunnelPool) resendForNack(connID, blockID uint32) {
	tp.mutex.Lock()
	tunnels := make([]*Tunnel, 0, len(tp.tunnelMapping))
	for _, t := range tp.tunnelMapping {
		if t.flow != nil {
			tunnels = append(tunnels, t)
		}
	}
	tp.mutex.Unlock()
	for _, t := range tunnels {
		if blk, ok := t.flow.findPending(connID, blockID); ok {
			atomic.AddUint64(&NackResent, 1)
			select {
			case tp.sendRetryQueue <- blk:
			default:
				go func(b block.Block) { tp.sendRetryQueue <- b }(blk)
			}
			return
		}
	}
}
