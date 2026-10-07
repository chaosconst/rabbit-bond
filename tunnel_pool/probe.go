package tunnel_pool

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

// Idle-tunnel probing (client side, flow control). A tunnel whose path died while it carried
// nothing shows no sign of it: there is nothing in flight to go un-acknowledged, so it is never
// "stuck", and the next UDP session pinned to it, or the next block scheduled on it, goes into the
// void. Seen 2026-10-05 on the phone: three idle tunnels dead for 9 minutes, every new UDP session
// on them got no reply (Claude's QUIC hung until it fell back to TCP). An idle tunnel's scheduling
// RTT also stops being refreshed: one block acked 10 minutes late made a tunnel's RTT 623 s, which
// kept it unused and its stall threshold at 8 × 623 s for good.
//
// RABBIT_FLOW_PROBE_S: a tunnel that sent nothing for this long and has nothing in flight sends a
// TypePing, which the far end acknowledges and drops (0 turns probing off). Its ack is also a fresh
// unloaded RTT sample.
// RABBIT_FLOW_DEAD_S: a tunnel with blocks (data or probe) in flight and no ack progress for this
// long is closed; its un-acknowledged blocks go to the other tunnels and the pool dials a
// replacement. The stall detector (a second or so) only stops scheduling on such a tunnel; this
// ends it. Progress, not the age of the oldest block: on a slow path (a weak mobile link) a full
// window can take longer than this to drain while acks keep coming.
//
// The far end must understand TypePing: upgrade servers before clients.
var (
	flowProbeEvery = time.Duration(envInt("RABBIT_FLOW_PROBE_S", 10)) * time.Second
	flowDeadAfter  = time.Duration(envInt("RABBIT_FLOW_DEAD_S", 8)) * time.Second
)

// ProbeSent counts probes, ProbeDead tunnels closed for an ack that never came (for /debug/flow).
var ProbeSent, ProbeDead uint64

var probeCheckEvery = time.Second

func (tunnel *Tunnel) probeLoop() {
	ticker := time.NewTicker(probeCheckEvery)
	defer ticker.Stop()
	for {
		select {
		case <-tunnel.ctx.Done():
			return
		case <-ticker.C:
		}
		idle, waiting := tunnel.flow.probeState(time.Now())
		if flowDeadAfter > 0 && waiting > flowDeadAfter {
			atomic.AddUint64(&ProbeDead, 1)
			tunnel.logger.Warnf("Nothing acknowledged for %.1fs, closing the tunnel.\n", waiting.Seconds())
			tunnel.closeWhy(fmt.Sprintf("dead: nothing acked for %.1fs", waiting.Seconds()))
			return
		}
		if waiting == 0 && idle >= flowProbeEvery {
			tunnel.sendPing()
		}
	}
}

// probeState returns how long nothing has been sent, and how long blocks have been in flight
// without any ack progress (0 if nothing is in flight).
func (f *flowState) probeState(now time.Time) (idle, waiting time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lastSent.IsZero() {
		f.lastSent = now // a fresh tunnel counts as just used
	}
	idle = now.Sub(f.lastSent)
	if len(f.pending) > 0 {
		since := f.pending[0].sentAt
		if f.lastAck.After(since) {
			since = f.lastAck
		}
		waiting = now.Sub(since)
	}
	return
}

func (tunnel *Tunnel) sendPing() {
	ping := block.NewPingBlock()
	data := ping.Pack()
	tunnel.flow.writeMu.Lock()
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	_, err := tunnel.Conn.Write(data)
	tunnel.Conn.SetWriteDeadline(time.Time{})
	if err == nil {
		tunnel.flow.onSent(ping, len(data)) // under writeMu, as for data blocks: acks count bytes in write order
	}
	tunnel.flow.writeMu.Unlock()
	if err != nil {
		tunnel.logger.Warnf("Error when send ping to tunnel: %v.\n", err)
		tunnel.closeWhy("ping write: " + err.Error())
		return
	}
	atomic.AddUint64(&ProbeSent, 1)
}
