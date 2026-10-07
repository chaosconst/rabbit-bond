package tunnel_pool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

// RABBIT_FLOW_DGRAM_PIN=1: the datagrams of one UDP session all go through one tunnel instead of
// whichever tunnel takes the next block. Spread over tunnels they arrive out of order (80% of
// packets, hundreds of positions late, lab 2026-09-30), and QUIC or any congestion control that
// counts a packet 3 positions late as lost backs off to a fraction of the path. Pinned, a session
// sees the order of one TCP tunnel, as it would on a plain path. Datagrams still queued on a tunnel
// that dies are lost, as on any UDP path.
//
// A session is pinned to the earliest tunnel when it starts and stays there while that tunnel is
// alive, not stuck and not far behind the best one (DGRAM_REPIN_PCT of the best estimate plus
// DGRAM_REPIN_MS); it is checked at most every dgramCheckEvery. A move reorders once. Small
// datagrams duplicated to another line (DUP_DGRAM) still are: the earlier copy wins.
//
// The estimate is for the datagram's own size, and a measured delivery rate below
// DGRAM_MIN_RATE_KBIT counts as that much. A tunnel carrying little more than a game measures the
// game's own few KB/s, not what the tunnel can do; what it has in flight is the game's own packets
// on their way, and in flight / rate then reads as another round trip of queue (a 16 KB block at
// that rate, seconds). Each session's tunnel looked far behind an idle one, and a game moved about
// once a second each way (production 2026-10-01). A tunnel that is really full measures its real
// rate, above the floor, and still loses the session.
var (
	flowDgramPin      = envInt("RABBIT_FLOW_DGRAM_PIN", 0) == 1
	flowDgramRepinPct = float64(envInt("RABBIT_FLOW_DGRAM_REPIN_PCT", 200)) / 100
	flowDgramRepin    = time.Duration(envInt("RABBIT_FLOW_DGRAM_REPIN_MS", 100)) * time.Millisecond
	flowDgramMinRate  = float64(envInt("RABBIT_FLOW_DGRAM_MIN_RATE_KBIT", 2000)) * 1000 / 8 // bytes/s
)

const (
	dgramCheckEvery = 200 * time.Millisecond
	dgramQueueSize  = 1024
	dgramPinForget  = 2 * time.Minute // a pin not used this long is forgotten
)

// DatagramMoves counts sessions moved to another tunnel, by reason: the tunnel closed, looked
// stuck, or was far behind the earliest one (for /debug/flow).
var DatagramMoves, DatagramMovesGone, DatagramMovesStuck, DatagramMovesSlow uint64

type dgramPin struct {
	tunnel    *Tunnel
	checkedAt time.Time
	usedAt    time.Time
}

type dgramPins struct {
	mu    sync.Mutex
	pins  map[uint32]*dgramPin
	sweep time.Time
}

// DatagramPinned reports whether datagrams go through SendDatagram.
func DatagramPinned() bool { return flowDgramPin && FlowControl }

// SendDatagram queues a datagram block on its session's tunnel, waiting while that queue is full.
// With no tunnel it waits on the shared queue. It gives up when done closes.
func (tp *TunnelPool) SendDatagram(blk block.Block, done <-chan struct{}) {
	for {
		t := tp.datagramTunnel(blk.ConnectionID, block.HeaderSize+len(blk.BlockData))
		if t == nil {
			select {
			case tp.sendQueue <- blk:
			case <-done:
			case <-tp.ctx.Done():
			}
			return
		}
		select {
		case t.dgramQ <- blk:
			return
		default:
		}
		timer := time.NewTimer(dgramCheckEvery)
		select {
		case t.dgramQ <- blk:
			timer.Stop()
			return
		case <-t.ctx.Done():
		case <-timer.C:
		case <-done:
			timer.Stop()
			return
		case <-tp.ctx.Done():
			timer.Stop()
			return
		}
		timer.Stop()
	}
}

// datagramTunnel returns the tunnel of session id for a block of size bytes, pinning or moving it
// when needed.
func (tp *TunnelPool) datagramTunnel(id uint32, size int) *Tunnel {
	now := time.Now()
	tp.dgram.mu.Lock()
	defer tp.dgram.mu.Unlock()
	if tp.dgram.pins == nil {
		tp.dgram.pins = map[uint32]*dgramPin{}
	}
	if now.Sub(tp.dgram.sweep) > dgramPinForget {
		tp.dgram.sweep = now
		for k, p := range tp.dgram.pins {
			if now.Sub(p.usedAt) > dgramPinForget {
				delete(tp.dgram.pins, k)
			}
		}
	}
	p := tp.dgram.pins[id]
	if p != nil && p.tunnel.ctx.Err() == nil && now.Sub(p.checkedAt) < dgramCheckEvery {
		p.usedAt = now
		return p.tunnel
	}
	var cur *Tunnel
	if p != nil && p.tunnel.ctx.Err() == nil {
		cur = p.tunnel
	}
	t, why := tp.pickDatagramTunnel(cur, size, now)
	if t == nil {
		delete(tp.dgram.pins, id)
		return nil
	}
	if p == nil {
		p = &dgramPin{}
		tp.dgram.pins[id] = p
	} else if t != p.tunnel {
		atomic.AddUint64(&DatagramMoves, 1)
		if cur == nil {
			why = &DatagramMovesGone
		}
		atomic.AddUint64(why, 1)
	}
	p.tunnel, p.checkedAt, p.usedAt = t, now, now
	return t
}

// pickDatagramTunnel keeps cur unless it is stuck or far behind the earliest tunnel.
// why is the counter of the reason when it does not keep cur.
func (tp *TunnelPool) pickDatagramTunnel(cur *Tunnel, size int, now time.Time) (t *Tunnel, why *uint64) {
	var best *Tunnel
	var bestEst, curEst time.Duration
	curOK := false
	tp.mutex.Lock()
	for _, c := range tp.tunnelMapping {
		if c.flow == nil || c.dgramQ == nil || c.ctx.Err() != nil {
			continue
		}
		est, _, stuck := c.flow.estimateAtLeast(size, flowDgramMinRate, now)
		if stuck {
			continue
		}
		if c == cur {
			curEst, curOK = est, true
		}
		if best == nil || est < bestEst {
			best, bestEst = c, est
		}
	}
	tp.mutex.Unlock()
	// A new tunnel's queue is estimated at an assumed modest rate until measured, which a session
	// alone can outrun: that would move every session off a fresh tunnel once.
	if !curOK {
		return best, &DatagramMovesStuck
	}
	if !cur.flow.measured() || float64(curEst) <= float64(bestEst)*flowDgramRepinPct+float64(flowDgramRepin) {
		return cur, nil
	}
	return best, &DatagramMovesSlow
}

// ForgetDatagram drops the pin of a closed session.
func (tp *TunnelPool) ForgetDatagram(id uint32) {
	tp.dgram.mu.Lock()
	delete(tp.dgram.pins, id)
	tp.dgram.mu.Unlock()
}
