package tunnel_pool

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ihciah/rabbit-tcp/block"
)

// FlowControl enables per-tunnel acknowledgement and in-flight limiting.
// It changes the wire protocol (adds block.TypeAck), so both peers must enable it.
//
// Without it a tunnel pulls a new block as soon as the previous one is handed to
// the kernel, so a slow tunnel keeps swallowing blocks into socket buffers and
// every other tunnel ends up waiting for it in the receiver's reorder buffer.
// With it, each tunnel keeps at most cwnd un-acknowledged bytes in flight, where
// cwnd tracks 2 × delivery rate × min RTT of that tunnel. Fast tunnels get acks
// back sooner and naturally carry more blocks. Un-acknowledged blocks are
// re-queued when a tunnel dies, so a broken tunnel no longer leaves holes.
var FlowControl = false

// Experiment knobs (temporary): RABBIT_FLOW_SCHED=0 disables earliest-completion
// gating, RABBIT_FLOW_REINJECT=0 disables re-sending stuck blocks.
var (
	flowSchedOn    = os.Getenv("RABBIT_FLOW_SCHED") != "0"
	flowReinjectOn = os.Getenv("RABBIT_FLOW_REINJECT") != "0"
	flowMinWindow  = uint64(envInt("RABBIT_FLOW_MINWIN_KB", 2048)) * 1024
	flowStallMin   = time.Duration(envInt("RABBIT_FLOW_STALL_MS", 1000)) * time.Millisecond
	flowStallRTTs  = time.Duration(envInt("RABBIT_FLOW_STALL_RTTS", 8))
	flowSchedRatio = float64(envInt("RABBIT_FLOW_SCHED_PCT", 120)) / 100
	flowSchedSlack = time.Duration(envInt("RABBIT_FLOW_SCHED_SLACK_MS", 20)) * time.Millisecond
	// RABBIT_FLOW_RATE=ack falls back to the old ack-interval rate sampling.
	rateSampleByBlock = os.Getenv("RABBIT_FLOW_RATE") != "ack"
	// RABBIT_FLOW_RATE_WIN_MS > 0: the rate is the average over that sliding window instead of
	// the maximum short sample. A per-flow policer (token bucket) lets each flow burst well
	// above its sustained rate; short samples catch the burst and cwnd grows 2-5x too large.
	flowRateAvgWin = time.Duration(envInt("RABBIT_FLOW_RATE_WIN_MS", 0)) * time.Millisecond
	// RABBIT_FLOW_BASERTT_WIN_S > 0: scheduling uses a windowed unloaded RTT (see baseRTT) instead
	// of the lifetime minimum, which stays at the best route ever seen after the route changes.
	flowBaseRTTWin = time.Duration(envInt("RABBIT_FLOW_BASERTT_WIN_S", 0)) * time.Second
	// cwnd = gain × max delivery rate × min RTT. 2 assumes the path itself adds no queueing
	// under load; home Wi-Fi / carrier queues do, so a higher gain may be needed.
	flowGain = float64(envInt("RABBIT_FLOW_GAIN_PCT", 200)) / 100
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

const (
	flowInitWindow = 2048 * 1024
	flowMaxWindow  = 16 * 1024 * 1024
	flowRateWindow = 3 * time.Second // how long a max delivery-rate sample stays valid
	flowMinSample  = 10 * time.Millisecond
	// A second NACKed hole on the same tunnel within this window re-sends all it holds.
	flowNackBulkWindow = time.Second

	// Scheduling: a tunnel may take the next block only if it is expected to deliver
	// it no later than the best tunnel would (within this slack), so slow or
	// congested tunnels become overflow capacity instead of holding up the stream.
	// (flowSchedRatio / flowSchedSlack are vars above: RABBIT_FLOW_SCHED_PCT, RABBIT_FLOW_SCHED_SLACK_MS.)
	flowPollEvery = 5 * time.Millisecond

	// A block un-acknowledged for max(stallRTTs × minRTT, stallMin) is considered
	// stuck (e.g. TCP retransmission on a lossy path) and is re-sent on another tunnel.
	// (flowStallRTTs is a var above, RABBIT_FLOW_STALL_RTTS.)
)

type pendingBlock struct {
	end        uint64 // cumulative sent bytes after this block
	sentAt     time.Time
	ackedAt    uint64 // f.acked when this block was sent (delivery-rate sample base)
	blk        block.Block
	solo       bool // sent to an empty tunnel: its RTT has no queue of this tunnel in it
	reinjected bool
	nacked     bool // already re-sent for a NACK
}

type flowState struct {
	writeMu sync.Mutex // serializes data blocks and acks on the (stream-encrypted) conn

	mu      sync.Mutex
	sent    uint64
	acked   uint64
	pending []pendingBlock
	ackCh   chan struct{} // capacity 1; signaled on every ack

	recv uint64 // bytes received; only touched by InboundRelay

	// minRTT is the lifetime minimum. Samples include queueing, so a windowed minimum
	// would ratchet upwards together with cwnd (self-inflicted bufferbloat).
	minRTT      time.Duration
	maxRate     float64 // bytes per second
	maxRateAt   time.Time
	sampleStart time.Time
	sampleAcked uint64
	ackHist     []ackPoint // flowRateAvgWin: acks within the window (plus one older as base)
	lastNack    time.Time  // last NACKed hole this tunnel held
	// baseRTT is what scheduling uses: the minimum over flowBaseRTTWin of RTTs of blocks sent to an
	// empty tunnel (no own queue), so it follows route changes (e.g. night 50 ms, day 110 ms).
	// cwnd keeps the lifetime minRTT: sized from a windowed value it would ratchet up with its own queue.
	baseRTT     time.Duration
	baseRTTAt   time.Time
	baseRTTCand time.Duration // minimum unloaded sample since baseRTTAt
	cwnd        uint64
}

type ackPoint struct {
	t     time.Time
	acked uint64
}

func newFlowState() *flowState {
	return &flowState{ackCh: make(chan struct{}, 1), cwnd: flowInitWindow}
}

func (f *flowState) inflight() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent - f.acked
}

func (f *flowState) canSend() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent-f.acked < f.cwnd
}

func (f *flowState) onSent(blk block.Block, size int) {
	f.mu.Lock()
	f.sent += uint64(size)
	solo := len(f.pending) == 0
	f.pending = append(f.pending, pendingBlock{end: f.sent, sentAt: time.Now(), ackedAt: f.acked, blk: blk, solo: solo})
	f.mu.Unlock()
}

// onAck handles a cumulative ack. The wire carries the low 32 bits; in-flight
// data is far below 4 GiB, so the wrapped difference recovers the full value.
func (f *flowState) onAck(low uint32) {
	now := time.Now()
	f.mu.Lock()
	acked := f.acked + uint64(low-uint32(f.acked))
	if acked > f.sent {
		acked = f.sent
	}
	// Delivery-rate sample per acked block (as in BBR): bytes acked while that block was in
	// flight, divided by its flight time. Unlike "bytes acked / time between acks" this is
	// not inflated when acks arrive in bursts after a queue drains, which made cwnd (and the
	// standing queue) balloon to ~1 s and set off the stuck-block detector.
	i := 0
	var sample float64
	for ; i < len(f.pending) && f.pending[i].end <= acked; i++ {
		p := f.pending[i]
		rtt := now.Sub(p.sentAt)
		if f.minRTT == 0 || rtt < f.minRTT {
			f.minRTT = rtt
		}
		if p.solo {
			f.noteBaseRTT(rtt, now)
		}
		if rateSampleByBlock && rtt >= flowMinSample {
			if r := float64(acked-p.ackedAt) / rtt.Seconds(); r > sample {
				sample = r
			}
		}
	}
	f.pending = append(f.pending[:0], f.pending[i:]...)
	f.acked = acked

	if flowRateAvgWin > 0 {
		f.ackHist = append(f.ackHist, ackPoint{now, acked})
		k := 0
		for k+1 < len(f.ackHist) && now.Sub(f.ackHist[k+1].t) >= flowRateAvgWin {
			k++
		}
		f.ackHist = f.ackHist[k:]
		// After an idle gap the base is from before it: bytes over the whole gap would read as a
		// near-zero rate and keep the scheduler off this tunnel for a window. Start over instead.
		if now.Sub(f.ackHist[0].t) > 2*flowRateAvgWin {
			f.ackHist = f.ackHist[len(f.ackHist)-1:]
		}
		// Max over flowRateWindow of the window averages: a window where the scheduler fed
		// this tunnel little must not lower the rate, or it gets fed less and spirals down.
		if base := f.ackHist[0]; now.Sub(base.t) >= flowRateAvgWin/2 {
			r := float64(acked-base.acked) / now.Sub(base.t).Seconds()
			if r > f.maxRate || now.Sub(f.maxRateAt) > flowRateWindow {
				f.maxRate, f.maxRateAt = r, now
			}
		}
	} else if rateSampleByBlock {
		if sample > 0 && (sample > f.maxRate || now.Sub(f.maxRateAt) > flowRateWindow) {
			f.maxRate, f.maxRateAt = sample, now
		}
	} else if f.sampleStart.IsZero() {
		f.sampleStart, f.sampleAcked = now, acked
	} else if elapsed := now.Sub(f.sampleStart); elapsed >= f.minRTT && elapsed >= flowMinSample {
		rate := float64(acked-f.sampleAcked) / elapsed.Seconds()
		if rate > f.maxRate || now.Sub(f.maxRateAt) > flowRateWindow {
			f.maxRate, f.maxRateAt = rate, now
		}
		f.sampleStart, f.sampleAcked = now, acked
	}
	if f.maxRate > 0 && f.minRTT > 0 {
		w := uint64(flowGain * f.maxRate * f.minRTT.Seconds())
		if w < flowMinWindow {
			w = flowMinWindow
		} else if w > flowMaxWindow {
			w = flowMaxWindow
		}
		f.cwnd = w
	}
	f.mu.Unlock()

	select {
	case f.ackCh <- struct{}{}:
	default:
	}
}

// takePending removes and returns all un-acknowledged blocks (tunnel is dead).
func (f *flowState) takePending() []block.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	blocks := make([]block.Block, 0, len(f.pending))
	for _, p := range f.pending {
		blocks = append(blocks, p.blk)
	}
	f.pending = nil
	return blocks
}

func (f *flowState) stats() (cwnd, inflight uint64, rate float64, rtt time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cwnd, f.sent - f.acked, f.maxRate, f.minRTT
}

// noteBaseRTT takes an unloaded RTT sample: a new minimum is used at once; otherwise, once the
// current value is flowBaseRTTWin old, it is replaced by the minimum seen since.
func (f *flowState) noteBaseRTT(rtt time.Duration, now time.Time) {
	if f.baseRTTCand == 0 || rtt < f.baseRTTCand {
		f.baseRTTCand = rtt
	}
	if f.baseRTT == 0 || rtt < f.baseRTT || (flowBaseRTTWin > 0 && now.Sub(f.baseRTTAt) > flowBaseRTTWin) {
		f.baseRTT, f.baseRTTAt, f.baseRTTCand = f.baseRTTCand, now, 0
	}
}

// schedRTTLocked is the RTT scheduling and stall detection use.
func (f *flowState) schedRTTLocked() time.Duration {
	if flowBaseRTTWin > 0 && f.baseRTT > 0 {
		return f.baseRTT
	}
	return f.minRTT
}

func (f *flowState) stallAfterLocked() time.Duration {
	d := flowStallRTTs * f.schedRTTLocked()
	if d < flowStallMin {
		d = flowStallMin
	}
	return d
}

// estimate returns the expected time for a block of size bytes sent now to be
// delivered, and whether the tunnel currently looks stuck.
func (f *flowState) estimate(size int, now time.Time) (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) > 0 && now.Sub(f.pending[0].sentAt) > f.stallAfterLocked() {
		return 0, true
	}
	est := f.schedRTTLocked()
	// A stale rate sample (tunnel idle or starved) is ignored, so the tunnel gets
	// picked again and re-measured instead of being excluded forever.
	if f.maxRate > 0 && now.Sub(f.maxRateAt) <= flowRateWindow {
		est += time.Duration(float64(f.sent-f.acked+uint64(size)) / f.maxRate * float64(time.Second))
	}
	return est, false
}

// stuckBlocks returns pending blocks that have waited too long and marks them,
// so each is re-sent at most once from here.
func (f *flowState) stuckBlocks(now time.Time) []block.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []block.Block
	limit := f.stallAfterLocked()
	for i := range f.pending {
		if now.Sub(f.pending[i].sentAt) <= limit {
			break
		}
		if !f.pending[i].reinjected {
			f.pending[i].reinjected = true
			out = append(out, f.pending[i].blk)
		}
	}
	return out
}

// nackPending returns the blocks to re-send for a NACKed hole of connID that this tunnel holds
// (nil if it does not hold it). The first hole a tunnel causes costs only that block: usually
// the tunnel is just briefly slow and what follows arrives right behind it. A second hole within
// flowNackBulkWindow means it is stalled; then everything it holds for the connection is re-sent,
// since re-sending one block per NACK would stall the stream NackAfter per block.
// Blocks already re-sent for a NACK are skipped, except the hole itself.
func (f *flowState) nackPending(connID, blockID uint32) []block.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []block.Block
	now := time.Now()
	bulk := now.Sub(f.lastNack) < flowNackBulkWindow
	for i := range f.pending {
		p := &f.pending[i]
		b := p.blk
		if b.ConnectionID != connID || b.Type != block.TypeData || b.BlockID < blockID {
			continue
		}
		if b.BlockID == blockID || (bulk && len(out) > 0 && !p.nacked) {
			p.nacked = true
			out = append(out, b)
		}
	}
	if len(out) > 0 {
		f.lastNack = now
	}
	return out
}
