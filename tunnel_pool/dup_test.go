package tunnel_pool

import (
	"testing"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

func TestDupGoesToAnotherPath(t *testing.T) {
	now := time.Now()
	h1, h2, s := idleTunnel(1, 60*time.Millisecond, now), idleTunnel(2, 50*time.Millisecond, now), idleTunnel(3, 70*time.Millisecond, now)
	h1.line, h2.line, s.line = "cu", "cu", "sh2"
	tp := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{1: h1, 2: h2, 3: s}}
	if got := tp.dupTunnel(h1); got != s {
		t.Fatalf("second copy from cu should go to sh2 even though another cu tunnel is earlier, got %v", got.line)
	}
	delete(tp.tunnelMapping, 3)
	if got := tp.dupTunnel(h1); got != nil {
		t.Fatalf("only home tunnels left: no second copy, got %v", got.tunnelID)
	}
	h1.line, h2.line = "", "" // paths unknown (old peer): any other tunnel
	if got := tp.dupTunnel(h1); got != h2 {
		t.Fatalf("unknown paths: expected the other tunnel")
	}
}

func TestWantDup(t *testing.T) {
	old := flowDupFirst
	defer func() { flowDupFirst = old }()
	flowDupFirst = 4
	for _, c := range []struct {
		blk  block.Block
		want bool
	}{
		{block.Block{Type: block.TypeConnect, BlockID: 0}, true},
		{block.Block{Type: block.TypeData, BlockID: 3}, true},
		{block.Block{Type: block.TypeData, BlockID: 4}, false},
		{block.Block{Type: block.TypeNack, BlockID: 0}, false},
	} {
		if wantDup(c.blk) != c.want {
			t.Fatalf("%+v: want %v", c.blk, c.want)
		}
	}
	flowDupFirst = 0
	if wantDup(block.Block{Type: block.TypeData}) {
		t.Fatal("off by default")
	}
}

func TestLinesGroupEndpoints(t *testing.T) {
	old := flowLines
	defer func() { flowLines = old }()
	flowLines = parseLines("127.0.0.1:12455=cu, 127.0.0.1:12456=cu,127.0.0.1:12457=sh2")
	if lineOf("127.0.0.1:12455") != "cu" || lineOf("127.0.0.1:12456") != "cu" || lineOf("127.0.0.1:12457") != "sh2" {
		t.Fatalf("lines %v", flowLines)
	}
	if lineOf("127.0.0.1:9999") != "127.0.0.1:9999" {
		t.Fatal("an endpoint not listed is its own line")
	}
}

func TestWantDupDatagram(t *testing.T) {
	old := flowDupDgram
	defer func() { flowDupDgram = old }()
	flowDupDgram = 1400
	if !wantDup(block.Block{Type: block.TypeDatagram, BlockID: 99999, BlockData: make([]byte, 100)}) {
		t.Fatal("a small datagram is always doubled")
	}
	if wantDup(block.Block{Type: block.TypeDatagram, BlockData: make([]byte, 1500)}) {
		t.Fatal("a large datagram is not")
	}
}

func TestDupDatagramOnlyInSmallSessions(t *testing.T) {
	defer func(d, a int) { flowDupDgram, flowDupAvg = d, a }(flowDupDgram, flowDupAvg)
	flowDupDgram, flowDupAvg = 600, 400
	dg := func(id uint32, n int) block.Block {
		return block.Block{Type: block.TypeDatagram, ConnectionID: id, BlockData: make([]byte, n)}
	}
	// A game: small packets throughout, all duplicated.
	for i := 0; i < 50; i++ {
		if !wantDup(dg(1, 120)) {
			t.Fatalf("game packet %d not duplicated", i)
		}
	}
	// A bulk QUIC download: a 1200-byte Initial first, full packets, now and then a small one.
	wantDup(dg(2, 1250))
	for i := 0; i < 200; i++ {
		n := 1250
		if i%20 == 0 {
			n = 300
		}
		if wantDup(dg(2, n)) {
			t.Fatalf("packet %d (%d bytes) of a bulk session duplicated", i, n)
		}
	}
	// Large packets are never duplicated, whatever the session.
	if wantDup(dg(1, 900)) {
		t.Fatalf("900-byte packet duplicated")
	}
}
