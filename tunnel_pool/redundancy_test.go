package tunnel_pool

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/chaosconst/rabbit-bond/block"
)

func TestRedundancyPct(t *testing.T) {
	atomic.StoreUint64(&BytesSent, 0)
	atomic.StoreUint64(&DupBytes, 0)
	atomic.StoreUint64(&ResendBytes, 0)
	if redundancyPct() != 0 {
		t.Fatal("no bytes sent should read 0%")
	}
	blk := block.Block{BlockData: make([]byte, 987)} // 1000 bytes on the wire
	atomic.AddUint64(&BytesSent, 10000)
	atomic.AddUint64(&DupBytes, blockBytes(blk))
	addResend([]block.Block{blk, blk})
	if got := redundancyPct(); math.Abs(got-30) > 1e-9 {
		t.Fatalf("got %.3f%%, want 30%%", got)
	}
}
