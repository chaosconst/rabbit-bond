package tunnel_pool

import (
	"testing"
	"time"
)

// idleTunnel: a measured tunnel with nothing in flight.
func idleTunnel(id uint32, rtt time.Duration, now time.Time) *Tunnel {
	f := newFlowState()
	f.minRTT, f.baseRTT, f.baseRTTAt = rtt, rtt, now
	f.maxRate, f.maxRateAt = 50e6/8, now
	return &Tunnel{tunnelID: id, flow: f}
}

func withLate(t *testing.T, after time.Duration, queue bool) {
	oldAfter, oldQueue, oldRatio := flowLateAfter, flowSchedQueue, flowSchedRatio
	flowLateAfter, flowSchedQueue, flowSchedRatio = after, queue, 3
	t.Cleanup(func() { flowLateAfter, flowSchedQueue, flowSchedRatio = oldAfter, oldQueue, oldRatio })
}

func TestLateTunnelIsEstimatedLater(t *testing.T) {
	withLate(t, 300*time.Millisecond, true)
	now := time.Now()
	lossy, clean := idleTunnel(1, 100*time.Millisecond, now), idleTunnel(2, 100*time.Millisecond, now)
	for i := 0; i < 200; i++ { // 1 block in 10 is 1.5 s late on the lossy tunnel
		by := 5 * time.Millisecond
		if i%10 == 0 {
			by = 1500 * time.Millisecond
		}
		lossy.flow.noteLate(by, now)
		clean.flow.noteLate(5*time.Millisecond, now)
	}
	el, _ := lossy.flow.estimate(0, now)
	ec, _ := clean.flow.estimate(0, now)
	t.Logf("lossy %v (p %.3f, late by %v), clean %v", el, lossy.flow.lateP, lossy.flow.lateBy, ec)
	if d := el - ec; d < 80*time.Millisecond || d > 250*time.Millisecond {
		t.Fatalf("penalty %v, want about 10%% x 1.5 s", d)
	}
	// Two half-lives later a quarter of it is left.
	el2, _ := lossy.flow.estimate(0, now.Add(2*flowLateHalfLife))
	if d := el2 - ec; d > (el-ec)/4+time.Millisecond {
		t.Fatalf("penalty did not decay: %v", d)
	}
}

func TestIdlePoolSendsToEarliestTunnel(t *testing.T) {
	now := time.Now()
	fast, slow := idleTunnel(1, 65*time.Millisecond, now), idleTunnel(2, 105*time.Millisecond, now)
	tp := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{1: fast, 2: slow}}

	withLate(t, 300*time.Millisecond, false)
	if !tp.schedulable(slow) {
		t.Fatalf("old allowance (x3 on the whole estimate) should let the slow tunnel take a block")
	}
	flowSchedQueue = true
	if tp.schedulable(slow) || !tp.schedulable(fast) {
		t.Fatalf("queue-only allowance: an idle pool must send to the earliest tunnel only")
	}
	// The fast tunnel turns lossy: now the slow one is the earliest.
	for i := 0; i < 100; i++ {
		by := time.Duration(0)
		if i%5 == 0 {
			by = 2 * time.Second
		}
		fast.flow.noteLate(by, now)
	}
	if !tp.schedulable(slow) || tp.schedulable(fast) {
		t.Fatalf("a lossy tunnel should lose the small blocks to a clean one")
	}
}

func TestLoadedPoolKeepsAllowance(t *testing.T) {
	withLate(t, 300*time.Millisecond, true)
	now := time.Now()
	fast, slow := idleTunnel(1, 65*time.Millisecond, now), idleTunnel(2, 105*time.Millisecond, now)
	tp := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{1: fast, 2: slow}}
	// Both carry a queue (bulk transfer): 1 MB in flight at 50 Mbit/s = 160 ms.
	fast.flow.sent, slow.flow.sent = 1<<20, 1<<20
	if !tp.schedulable(slow) {
		t.Fatalf("under load the slower tunnel must still take blocks (overflow capacity)")
	}
}

func TestSteadilySlowPathLosesSmallBlocks(t *testing.T) {
	withLate(t, 300*time.Millisecond, true)
	now := time.Now()
	home, sh2 := idleTunnel(1, 60*time.Millisecond, now), idleTunnel(2, 70*time.Millisecond, now)
	tp := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{1: home, 2: sh2}}
	for i := 0; i < 40; i++ { // home's return path is 150 ms slower than its best, never 300 ms late
		home.flow.noteLate(150*time.Millisecond, now)
		sh2.flow.noteLate(10*time.Millisecond, now)
	}
	if !tp.schedulable(home) {
		t.Fatalf("without EXTRA the lowest-RTT tunnel keeps the small blocks")
	}
	flowExtra = true
	defer func() { flowExtra = false }()
	if tp.schedulable(home) || !tp.schedulable(sh2) {
		t.Fatalf("extra %v vs %v: the steadily slower path should lose small blocks", home.flow.extra, sh2.flow.extra)
	}
	// Idle for 4 half-lives: home's extra has mostly decayed and it is tried again.
	later := now.Add(4 * flowLateHalfLife)
	if e, _ := home.flow.estimate(0, later); e > 60*time.Millisecond+15*time.Millisecond {
		t.Fatalf("extra did not decay: %v", e)
	}
}
