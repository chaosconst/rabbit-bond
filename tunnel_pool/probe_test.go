package tunnel_pool

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
	"github.com/chaosconst/rabbit-bond/logger"
)

func setProbeTimes(t *testing.T, every, dead, check time.Duration) {
	e, d, c := flowProbeEvery, flowDeadAfter, probeCheckEvery
	flowProbeEvery, flowDeadAfter, probeCheckEvery = every, dead, check
	t.Cleanup(func() { flowProbeEvery, flowDeadAfter, probeCheckEvery = e, d, c })
}

func probeTunnel(conn net.Conn) *Tunnel {
	tun := &Tunnel{Conn: conn, flow: newFlowState(), logger: logger.NewLogger("[probe-test]"), endpoint: "test", created: time.Now()}
	tun.ctx, tun.cancel = context.WithCancel(context.Background())
	return tun
}

// An idle tunnel sends probes; the far end acknowledges them and passes nothing on.
func TestProbeAckedAndDropped(t *testing.T) {
	setProbeTimes(t, 50*time.Millisecond, 2*time.Second, 10*time.Millisecond)
	a, b := net.Pipe()
	client, server := probeTunnel(a), probeTunnel(b)
	defer client.cancel()
	defer server.cancel()
	clientOut, serverOut := make(chan block.Block, 8), make(chan block.Block, 8)
	go client.InboundRelay(clientOut)
	go server.InboundRelay(serverOut)
	before := atomic.LoadUint64(&ProbeSent)
	done := make(chan struct{})
	go func() { client.probeLoop(); close(done) }()
	defer func() { client.cancel(); <-done }() // before setProbeTimes restores the knobs

	time.Sleep(400 * time.Millisecond)
	if n := atomic.LoadUint64(&ProbeSent) - before; n < 2 {
		t.Fatalf("idle tunnel sent %d probes in 400 ms with a 50 ms interval", n)
	}
	if client.ctx.Err() != nil {
		t.Fatal("a tunnel whose probes are acked was closed")
	}
	select {
	case blk := <-serverOut:
		t.Fatalf("probe passed on to the connection layer (type %#x)", blk.Type)
	default:
	}
	// Probes are acked: nothing stays in flight between them for long.
	time.Sleep(20 * time.Millisecond)
	if _, waiting := client.flow.probeState(time.Now()); waiting > 200*time.Millisecond {
		t.Fatalf("probe un-acked for %v", waiting)
	}
}

// blackHole accepts every write and never answers: a path that died without a reset.
type blackHole struct{ net.Conn }

func (blackHole) Write(b []byte) (int, error)      { return len(b), nil }
func (blackHole) SetWriteDeadline(time.Time) error { return nil }
func (blackHole) Close() error                     { return nil }

func TestProbeClosesDeadIdleTunnel(t *testing.T) {
	setProbeTimes(t, 50*time.Millisecond, 300*time.Millisecond, 10*time.Millisecond)
	tun := probeTunnel(blackHole{})
	done := make(chan struct{})
	go func() { tun.probeLoop(); close(done) }()
	defer func() { tun.cancel(); <-done }()
	select {
	case <-tun.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("dead idle tunnel not closed")
	}
	found := false
	for _, ev := range recentCloses() {
		if ev.Endpoint == "test" && strings.HasPrefix(ev.Why, "dead: ") {
			found = true
		}
	}
	if !found {
		t.Fatal("close reason not recorded")
	}
}

// A dead tunnel's un-acked probe is not handed to the other tunnels.
func TestProbeNotRequeued(t *testing.T) {
	f := newFlowState()
	f.onSent(block.NewPingBlock(), block.HeaderSize)
	data := block.NewDisconnectBlock(1, 1, 0)
	f.onSent(data, len(data.Pack()))
	got := f.takePending()
	if len(got) != 1 || got[0].Type != block.TypeDisconnect {
		t.Fatalf("takePending = %v, want only the data block", got)
	}
}

// A slow tunnel whose oldest block has waited long but whose acks keep coming is not dead.
func TestProbeSlowTunnelNotDead(t *testing.T) {
	f := newFlowState()
	now := time.Now()
	for i := 0; i < 3; i++ {
		blk := block.NewDisconnectBlock(1, uint32(i), 0)
		f.onSent(blk, 100)
	}
	for i := range f.pending {
		f.pending[i].sentAt = now.Add(-20 * time.Second) // queued behind a slow link
	}
	f.onAck(100) // the first one just got through
	if _, waiting := f.probeState(time.Now()); waiting > time.Second {
		t.Fatalf("slow but progressing tunnel reads as %v without progress", waiting)
	}
}
