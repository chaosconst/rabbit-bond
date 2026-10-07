package tunnel_pool

import (
	"testing"
	"time"
)

// A tunnel carrying only a game measures the game's own rate: its few packets in flight must not
// read as queue. A tunnel that is really full keeps its measured rate.
func TestDatagramQueueRateFloor(t *testing.T) {
	now := time.Now()
	game := &flowState{maxRate: 5e3, maxRateAt: now, sent: 400}
	if q := game.queueLocked(300, flowDgramMinRate, now); q > 5*time.Millisecond {
		t.Errorf("game alone: queue %v, want ~3 ms at the 2 Mbit/s floor", q)
	}
	if q := game.queueLocked(300, 0, now); q < 100*time.Millisecond {
		t.Errorf("without the floor the game's own rate gives %v; expected the old overestimate", q)
	}
	full := &flowState{maxRate: 20e6 / 8, maxRateAt: now, sent: 512 * 1024}
	if q := full.queueLocked(300, flowDgramMinRate, now); q < 200*time.Millisecond {
		t.Errorf("full tunnel: queue %v, want ~210 ms at its measured 20 Mbit/s", q)
	}
	slow := &flowState{maxRate: 0.5e6 / 8, maxRateAt: now, sent: 50 * 1024}
	if q := slow.queueLocked(300, flowDgramMinRate, now); q < 150*time.Millisecond {
		t.Errorf("0.5 Mbit/s tunnel holding 50 KB: queue %v, want ~200 ms at the floor", q)
	}
}
