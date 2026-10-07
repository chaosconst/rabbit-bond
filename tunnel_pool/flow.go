package tunnel_pool

import (
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
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
	// RABBIT_FLOW_SCHED_MIN_RATE_KBIT: the delivery rate scheduling and head duplication assume at
	// least. A tunnel that carried only a few small blocks (a TLS handshake, an idle pool) measures
	// its rate from them, e.g. 0.01-0.1 Mbit/s, and its estimate for a full block becomes seconds of
	// "queue": the paths just used look slowest and an idle slow one looks fastest. Bulk transfers
	// measure real rates far above the floor and are not affected.
	flowSchedMinRate = float64(envInt("RABBIT_FLOW_SCHED_MIN_RATE_KBIT", 8000)) * 1000 / 8 // bytes/s
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
	// RABBIT_FLOW_LATE_MS > 0: a block acked more than this after its expected delivery is "late"
	// (on a TCP tunnel that is almost always a loss recovered by retransmission timeout, which the
	// receiver cannot NACK when nothing follows the lost block). Each tunnel keeps its late rate p
	// and mean lateness; the expected delivery gets p × lateness added, so a tunnel that loses more
	// is chosen less. The rate decays toward 0 with flowLateHalfLife so a tunnel is not shunned for
	// one bad minute forever.
	flowLateAfter = time.Duration(envInt("RABBIT_FLOW_LATE_MS", 0)) * time.Millisecond
	// RABBIT_FLOW_SCHED_QUEUE=1: the scheduling allowance (SCHED_PCT) applies to the best tunnel's
	// queue only, not to its RTT. With the allowance on the whole estimate, an idle pool (no queues)
	// let every tunnel within 3x the best RTT take a block, so small requests went to any tunnel;
	// now they go to the earliest one, while under load (queues) the allowance is as wide as before.
	flowSchedQueue = os.Getenv("RABBIT_FLOW_SCHED_QUEUE") == "1"
	// RABBIT_FLOW_EXTRA=1 (with LATE_MS): also add the tunnel's typical delay beyond its expected
	// delivery for blocks that are not late. The scheduling RTT is the tunnel's lowest in 30 s, so a
	// path that is steadily 150 ms slower than that (a congested return path in the evening) still
	// looked as early as a clean one and kept getting the small blocks.
	flowExtra = os.Getenv("RABBIT_FLOW_EXTRA") == "1"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

const (
	flowInitWindow = 512 * 1024
	flowFreshRate  = 10e6 / 8 // bytes/s assumed for a tunnel not measured yet
	flowMaxWindow  = 16 * 1024 * 1024
	flowRateWindow = 3 * time.Second // how long a max delivery-rate sample stays valid
	flowMinSample  = 10 * time.Millisecond
	// A second NACKed hole on the same tunnel within this window re-sends all it holds.
	flowNackBulkWindow = time.Second
	flowLateHalfLife   = time.Minute
	flowLateGain       = 1.0 / 32 // late rate: moving average over ~32 blocks

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
	nacked     bool          // already re-sent for a NACK
	expect     time.Duration // expected delivery when sent (flowLateAfter)
}

type flowState struct {
	writeMu sync.Mutex // serializes data blocks and acks on the (stream-encrypted) conn

	mu      sync.Mutex
	sent    uint64
	acked   uint64
	pending []pendingBlock
	// ackAhead is an ack for bytes onSent has not counted yet. A block is counted once its write
	// returns, and the peer's ack can be read before that: clamped to what was counted, the ack
	// was lost. With nothing more sent on the tunnel no later ack came, its last block looked
	// stuck for good and the scheduler never used the tunnel again (production 2026-10-01: a
	// 14-byte disconnect block, 1.5 h).
	ackAhead uint64
	ackCh    chan struct{} // capacity 1; signaled on every ack
	lastSent time.Time     // when the last block was counted by onSent (probe.go)
	lastAck  time.Time     // when an ack last acknowledged something new (probe.go)

	recv    uint64 // bytes received; only touched by InboundRelay
	unacked int    // blocks received but not acked yet (flowAckBatch); only touched by InboundRelay

	// minRTT is the lifetime minimum. Samples include queueing, so a windowed minimum
	// would ratchet upwards together with cwnd (self-inflicted bufferbloat).
	minRTT      time.Duration
	maxRate     float64 // bytes per second
	maxRateAt   time.Time
	sampleStart time.Time
	sampleAcked uint64
	ackHist     []ackPoint // flowRateAvgWin: acks within the window (plus one older as base)
	lastNack    time.Time  // last NACKed hole this tunnel held
	nacks       uint64     // NACKed holes this tunnel held (for /debug/flow)
	// baseRTT is what scheduling uses: the minimum over flowBaseRTTWin of RTTs of blocks sent to an
	// empty tunnel (no own queue), so it follows route changes (e.g. night 50 ms, day 110 ms).
	// cwnd keeps the lifetime minRTT: sized from a windowed value it would ratchet up with its own queue.
	baseRTT     time.Duration
	baseRTTAt   time.Time
	baseRTTCand time.Duration // minimum unloaded sample since baseRTTAt
	cwnd        uint64
	lateP       float64 // late-block rate, as of lateAt (decays with flowLateHalfLife)
	lateAt      time.Time
	lateBy      time.Duration // mean lateness of late blocks (beyond their expected delivery)
	lates       uint64        // late blocks (for /debug/flow)
	extra       time.Duration // mean delay beyond expected delivery of blocks that are not late, as of lateAt
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
	now := time.Now()
	var expect time.Duration
	if flowLateAfter > 0 {
		expect = f.schedRTTLocked() + f.queueLocked(size, 0, now)
	}
	f.sent += uint64(size)
	f.lastSent = now
	solo := len(f.pending) == 0
	f.pending = append(f.pending, pendingBlock{end: f.sent, sentAt: now, ackedAt: f.acked, blk: blk, solo: solo, expect: expect})
	acked := false
	if f.ackAhead > f.acked {
		// Acked before it was counted: drop it from pending without an RTT or rate sample, which
		// would read as zero.
		a := f.ackAhead
		if a > f.sent {
			a = f.sent
		}
		i := 0
		for ; i < len(f.pending) && f.pending[i].end <= a; i++ {
			f.pending[i] = pendingBlock{}
		}
		f.pending = f.pending[i:]
		f.acked = a
		acked = true
	}
	f.mu.Unlock()
	if acked {
		select {
		case f.ackCh <- struct{}{}:
		default:
		}
	}
}

// onAck handles a cumulative ack. The wire carries the low 32 bits; in-flight
// data is far below 4 GiB, so the wrapped difference recovers the full value.
func (f *flowState) onAck(low uint32) {
	now := time.Now()
	f.mu.Lock()
	acked := f.acked + uint64(low-uint32(f.acked))
	if acked > f.sent {
		f.ackAhead = acked
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
		if flowLateAfter > 0 && p.expect > 0 {
			f.noteLate(rtt-p.expect, now)
		}
		if rateSampleByBlock && rtt >= flowMinSample {
			if r := float64(acked-p.ackedAt) / rtt.Seconds(); r > sample {
				sample = r
			}
		}
	}
	// Drop the acked head by re-slicing: shifting the rest down on every ack was O(pending) per
	// ack, quadratic with many small blocks (datagrams: thousands in flight per tunnel), and ate
	// a quarter of the server's CPU in the tunnel's single inbound goroutine, which then acked
	// late and capped UDP at ~140 Mbit/s (lab 2026-10-01). Cleared so the blocks can be freed;
	// append reallocates when the capacity runs out, copying only what is still pending.
	for j := 0; j < i; j++ {
		f.pending[j] = pendingBlock{}
	}
	f.pending = f.pending[i:]
	if acked > f.acked {
		f.lastAck = now
	}
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
		if p.blk.Type != block.TypePing { // a probe means nothing to another tunnel
			blocks = append(blocks, p.blk)
		}
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

// noteLate takes one delivered block's lateness (actual minus expected delivery).
func (f *flowState) noteLate(by time.Duration, now time.Time) {
	p, e := f.latePLocked(now), f.extraLocked(now)
	if by > flowLateAfter {
		p += (1 - p) * flowLateGain
		if f.lateBy == 0 {
			f.lateBy = by
		} else {
			f.lateBy += (by - f.lateBy) / 4
		}
		f.lates++
	} else {
		p -= p * flowLateGain
		if by < 0 {
			by = 0
		}
		e += (by - e) / 8
	}
	f.lateP, f.extra, f.lateAt = p, e, now
}

func (f *flowState) latePLocked(now time.Time) float64 {
	if f.lateP == 0 {
		return 0
	}
	return f.lateP * math.Exp2(-float64(now.Sub(f.lateAt))/float64(flowLateHalfLife))
}

func (f *flowState) extraLocked(now time.Time) time.Duration {
	if f.extra == 0 {
		return 0
	}
	return time.Duration(float64(f.extra) * math.Exp2(-float64(now.Sub(f.lateAt))/float64(flowLateHalfLife)))
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
	est, _, stuck := f.estimateParts(size, now)
	return est, stuck
}

// estimateParts also returns the queueing part of the estimate.
func (f *flowState) estimateParts(size int, now time.Time) (est, queue time.Duration, stuck bool) {
	return f.estimateAtLeast(size, 0, now)
}

// estimateAtLeast is estimateParts with the measured delivery rate taken as at least minRate.
func (f *flowState) estimateAtLeast(size int, minRate float64, now time.Time) (est, queue time.Duration, stuck bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) > 0 && now.Sub(f.pending[0].sentAt) > f.stallAfterLocked() {
		return 0, 0, true
	}
	queue = f.queueLocked(size, minRate, now)
	est = f.schedRTTLocked() + queue
	if flowLateAfter > 0 {
		est += time.Duration(f.latePLocked(now) * float64(f.lateBy))
		if flowExtra {
			est += f.extraLocked(now)
		}
	}
	return est, queue, false
}

// measured reports whether the tunnel has a delivery rate sample yet.
func (f *flowState) measured() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxRate > 0
}

// queueLocked is the time to deliver what is in flight plus size bytes at the measured rate.
// A measured rate below minRate counts as minRate.
func (f *flowState) queueLocked(size int, minRate float64, now time.Time) time.Duration {
	// A stale rate sample (tunnel idle or starved) is ignored, so the tunnel gets
	// picked again and re-measured instead of being excluded forever.
	if f.maxRate > 0 && now.Sub(f.maxRateAt) <= flowRateWindow {
		rate := f.maxRate
		if rate < minRate {
			rate = minRate
		}
		return time.Duration(float64(f.sent-f.acked+uint64(size)) / rate * float64(time.Second))
	} else if f.maxRate == 0 {
		// A new tunnel has no rate yet and is in TCP slow start: counted as queue-free it looked
		// like the fastest path and got a whole window at once, which it needs seconds to deliver
		// (holes, stalls). Assume a modest rate until it is measured.
		return time.Duration(float64(f.sent-f.acked+uint64(size)) / flowFreshRate * float64(time.Second))
	}
	return 0
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
		if !f.pending[i].reinjected && f.pending[i].blk.Type != block.TypePing {
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
		f.nacks++
	}
	return out
}
