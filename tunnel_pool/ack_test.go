package tunnel_pool

import (
	"net"
	"testing"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

// ackConn counts writes (acks) and reports a settable number of buffered bytes.
type ackConn struct {
	net.Conn
	writes   int
	buffered int
}

func (c *ackConn) Write(b []byte) (int, error)        { c.writes++; return len(b), nil }
func (c *ackConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *ackConn) Buffered() int                      { return c.buffered }

func TestHeldAckFlushedByPeerAck(t *testing.T) {
	defer func(v int) { flowAckBatch = v }(flowAckBatch)
	flowAckBatch = 16
	conn := &ackConn{buffered: 100}
	tun := &Tunnel{Conn: conn, flow: newFlowState()}

	tun.ackMaybe(true) // the next block is already buffered: hold the ack
	if conn.writes != 0 {
		t.Fatalf("ack sent while more was buffered")
	}
	// The buffered block was the peer's ack and nothing follows: the held ack must go now,
	// or the peer (window full) and this end wait on each other forever.
	conn.buffered = 0
	tun.ackMaybe(false)
	if conn.writes != 1 {
		t.Fatalf("held ack not flushed when the buffer ran dry (writes=%d)", conn.writes)
	}
	tun.ackMaybe(false) // nothing held: no extra ack
	if conn.writes != 1 {
		t.Fatalf("spurious ack (writes=%d)", conn.writes)
	}
}

func TestAckBatchCapsHeldBlocks(t *testing.T) {
	defer func(v int) { flowAckBatch = v }(flowAckBatch)
	flowAckBatch = 4
	conn := &ackConn{buffered: 100}
	tun := &Tunnel{Conn: conn, flow: newFlowState()}
	for i := 0; i < 8; i++ {
		tun.ackMaybe(true)
	}
	if conn.writes != 2 {
		t.Fatalf("want one ack per 4 blocks while buffered, got %d for 8", conn.writes)
	}
}

func TestAckPerBlockByDefault(t *testing.T) {
	conn := &ackConn{buffered: 100}
	tun := &Tunnel{Conn: conn, flow: newFlowState()}
	for i := 0; i < 3; i++ {
		tun.ackMaybe(true)
	}
	if conn.writes != 3 {
		t.Fatalf("default must ack every block, got %d for 3", conn.writes)
	}
}

// The peer's ack can be read before the write that sent the block returns and onSent counts it.
// That ack must still count: lost, the block stays in flight, and a tunnel with nothing more to
// send looks stuck forever.
func TestAckBeforeOnSent(t *testing.T) {
	f := newFlowState()
	blk := block.NewDisconnectBlock(1, 1, 0)
	size := len(blk.Pack())
	f.onSent(blk, 100) // earlier traffic
	f.onAck(100)
	f.onAck(uint32(100 + size)) // the ack of blk arrives first
	f.onSent(blk, size)
	if _, inflight, _, _ := f.stats(); inflight != 0 {
		t.Fatalf("%d bytes in flight after their ack, want 0", inflight)
	}
	if _, stuck := f.estimate(0, time.Now().Add(time.Minute)); stuck {
		t.Fatal("tunnel looks stuck with everything acked")
	}
}
