package tunnel_pool

import (
	"context"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

// A tunnel whose writes hang (a phone on a flaky 4G link) must not collect a goroutine per second
// copy: at most dupMaxPending wait, the rest are skipped, and failed copies are dropped, not re-sent.
func TestCopiesToAHungTunnelAreBounded(t *testing.T) {
	near, far := net.Pipe() // nobody reads far: every write blocks
	ctx, cancel := context.WithCancel(context.Background())
	hung := &Tunnel{Conn: near, ctx: ctx, cancel: cancel, flow: newFlowState()}
	skipped0, sent0 := atomic.LoadUint64(&DupSkipped), atomic.LoadUint64(&DupSent)
	g0 := runtime.NumGoroutine()

	const n = 5000
	for i := 0; i < n; i++ {
		hung.sendCopy(block.Block{Type: block.TypeDatagram, ConnectionID: 7, BlockID: uint32(i), BlockData: make([]byte, 100)})
	}
	time.Sleep(50 * time.Millisecond)
	if p := atomic.LoadInt32(&hung.copies); p > dupMaxPending {
		t.Fatalf("%d copies waiting on a hung tunnel, want at most %d", p, dupMaxPending)
	}
	if g := runtime.NumGoroutine() - g0; g > dupMaxPending+2 {
		t.Fatalf("%d goroutines for copies to one hung tunnel", g)
	}
	sent, skipped := atomic.LoadUint64(&DupSent)-sent0, atomic.LoadUint64(&DupSkipped)-skipped0
	if sent+skipped != n || sent > dupMaxPending {
		t.Fatalf("sent %d skipped %d of %d", sent, skipped, n)
	}

	far.Close() // the link dies: waiting copies fail, close the tunnel and are dropped
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&hung.copies) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p := atomic.LoadInt32(&hung.copies); p != 0 {
		t.Fatalf("%d copies still waiting after the tunnel died", p)
	}
	if ctx.Err() == nil {
		t.Fatal("a failed copy should close the tunnel")
	}
}
