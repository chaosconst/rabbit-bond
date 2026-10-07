package tunnel_pool

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chaosconst/rabbit-bond/block"
)

// RABBIT_FLOW_DUP_FIRST=N (both ends): the first N blocks of every connection (SOCKS request,
// TLS handshake, the first request and response) are also sent on a tunnel of another network
// path; the receiver keeps whichever copy arrives first and drops the other. A new connection
// then waits for the faster of two paths at that moment instead of the one the scheduler guessed,
// and a lone lost small block no longer waits for a TCP retransmission timeout, since nothing
// behind it would reveal the hole. Only small, latency-bound blocks are doubled.
//
// The second copy goes to another *line*: endpoints that leave the country through the same
// exit (home broadband and a phone of the same carrier) jam together, so they are one line.
// RABBIT_FLOW_LINES="127.0.0.1:12455=cu,127.0.0.1:12456=cu,127.0.0.1:12457=sh2" names them on the
// client; an endpoint not listed is a line of its own. A client tunnel tells the server its line
// in a TypeLine block right after it joins the pool (not counted in flow accounting). An old
// server does not know TypeLine, so upgrade the server first.
//
// RABBIT_FLOW_DUP_DGRAM=N: every UDP datagram block of at most N bytes is duplicated the same way
// (games, voice, DNS: small packets where the earlier copy is all that matters).
//
// Only in sessions whose datagrams average at most RABBIT_FLOW_DUP_DGRAM_AVG bytes (default 400; 0
// duplicates every small datagram, as before 2026-10-01). Without it, in a bulk QUIC download the
// occasional small packet (the tail of a write, an ack) is duplicated too, and its copy on the
// faster line overtakes the packets before it on the session's own tunnel: the
// receiver acks around a gap, and a loss-based sender (quic-go, Cubic) takes the reordering as loss
// and backs off; at the ~10% of packets that overtook, a quic-go download fell from 130-170 to
// 1.5 Mbit/s (lab 2026-10-01; Google's sender did not mind). Game and voice sessions are small
// throughout and keep their copies.
var (
	flowDupFirst = uint32(envInt("RABBIT_FLOW_DUP_FIRST", 0))
	flowDupDgram = envInt("RABBIT_FLOW_DUP_DGRAM", 0)
	flowDupAvg   = envInt("RABBIT_FLOW_DUP_DGRAM_AVG", 400)
	flowLines    = parseLines(os.Getenv("RABBIT_FLOW_LINES"))
)

func parseLines(s string) map[string]string {
	m := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		if i := strings.LastIndex(kv, "="); i > 0 {
			m[strings.TrimSpace(kv[:i])] = strings.TrimSpace(kv[i+1:])
		}
	}
	return m
}

// lineOf names the line of a client endpoint.
func lineOf(endpoint string) string {
	if l, ok := flowLines[endpoint]; ok {
		return l
	}
	return endpoint
}

// DupSent counts duplicated blocks (for /debug/flow).
var DupSent uint64

// UDP session counters, kept by connection_pool (for /debug/flow).
var DatagramSent, DatagramRecv, DatagramQueueDrop, DatagramTooOld uint64

// Reorder hold (RABBIT_FLOW_DGRAM_HOLD_MS) counters: datagrams given up on when a hold timed out or
// the hold was full, and datagrams that arrived after their gap was skipped (for /debug/flow).
var DatagramHoldSkipTimer, DatagramHoldSkipFull, DatagramHoldLate uint64

func (tunnel *Tunnel) sendLine() {
	blk := block.NewLineBlock(tunnel.line)
	tunnel.flow.writeMu.Lock()
	defer tunnel.flow.writeMu.Unlock()
	tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
	if _, err := tunnel.Conn.Write(blk.Pack()); err != nil {
		tunnel.logger.Warnf("Error when send line to tunnel: %v.\n", err)
	}
	tunnel.Conn.SetWriteDeadline(time.Time{})
}

// A second copy is optional: the first one has already gone out. So it never waits in line and is
// never re-sent. At most dupMaxPending copies wait for one tunnel's write lock; more are skipped,
// and a copy that fails to write closes the tunnel and is dropped. Until 2026-10-06 every copy took
// its own goroutine and, on failure, the retry queue: a phone tunnel on a flaky 4G link looks idle,
// hence earliest, and is picked for copies while its writes hang for the 8 s timeout. A production
// server, doubling ~2000 small datagrams a second, grew to 1.3 million goroutines and 9 GB, swapped
// for 20 minutes and was killed by the kernel.
const dupMaxPending = 64

// DupSkipped counts second copies not sent because the chosen tunnel was still busy with earlier ones.
var DupSkipped uint64

func (tunnel *Tunnel) sendCopy(blk block.Block) {
	if atomic.AddInt32(&tunnel.copies, 1) > dupMaxPending {
		atomic.AddInt32(&tunnel.copies, -1)
		atomic.AddUint64(&DupSkipped, 1)
		return
	}
	atomic.AddUint64(&DupSent, 1)
	atomic.AddUint64(&DupBytes, blockBytes(blk))
	go func() {
		defer atomic.AddInt32(&tunnel.copies, -1)
		data := blk.Pack()
		tunnel.flow.writeMu.Lock()
		defer tunnel.flow.writeMu.Unlock()
		tunnel.Conn.SetWriteDeadline(time.Now().Add(TunnelBlockTimeoutSec * time.Second))
		n, err := tunnel.Conn.Write(data)
		atomic.AddUint64(&BytesSent, uint64(n))
		if err != nil || n != len(data) {
			tunnel.closeWhy(fmt.Sprintf("write copy: %v (%d/%d bytes)", err, n, len(data)))
			return
		}
		tunnel.Conn.SetWriteDeadline(time.Time{})
		tunnel.flow.onSent(blk, len(data))
	}()
}

func wantDup(blk block.Block) bool {
	if blk.Type == block.TypeDatagram {
		if flowDupAvg > 0 {
			return dgramSizes.small(blk.ConnectionID, len(blk.BlockData))
		}
		return len(blk.BlockData) <= flowDupDgram
	}
	return flowDupFirst > 0 && blk.Type <= block.TypeData && blk.BlockID < flowDupFirst
}

// dupTunnel picks where to send the second copy of a block sent on from: the earliest tunnel of
// another path, or of any other tunnel if paths are unknown.
func (tp *TunnelPool) dupTunnel(from *Tunnel) *Tunnel {
	now := time.Now()
	var best, bestAny *Tunnel
	var bestEst, bestAnyEst time.Duration
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	for _, t := range tp.tunnelMapping {
		if t == from || t.flow == nil {
			continue
		}
		est, _, stuck := t.flow.estimateAtLeast(block.MaxSize, flowSchedMinRate, now)
		if stuck {
			continue
		}
		if bestAny == nil || est < bestAnyEst {
			bestAny, bestAnyEst = t, est
		}
		if t.line != "" && t.line != from.line && (best == nil || est < bestEst) {
			best, bestEst = t, est
		}
	}
	if best != nil {
		return best
	}
	if from.line == "" {
		return bestAny
	}
	return nil // every other tunnel is on the same path: a second copy would not help
}

// dgramSizes keeps each UDP session's average datagram size (flowDupAvg).
var dgramSizes = sizeAverages{m: map[uint32]*sizeAverage{}}

type sizeAverage struct {
	avg    float64
	usedAt time.Time
}

type sizeAverages struct {
	mu    sync.Mutex
	m     map[uint32]*sizeAverage
	sweep time.Time
}

const sizeAvgGain = 1.0 / 16

// small takes the next datagram of a session: whether to duplicate it, i.e. it is at most
// flowDupDgram bytes and the session's datagrams average at most flowDupAvg (this one included).
func (a *sizeAverages) small(id uint32, n int) bool {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.m[id]
	if !ok {
		s = &sizeAverage{avg: float64(n)}
		a.m[id] = s
	} else {
		s.avg += (float64(n) - s.avg) * sizeAvgGain
	}
	s.usedAt = now
	if now.Sub(a.sweep) > time.Minute {
		a.sweep = now
		for k, v := range a.m {
			if now.Sub(v.usedAt) > 2*time.Minute {
				delete(a.m, k)
			}
		}
	}
	return n <= flowDupDgram && s.avg <= float64(flowDupAvg)
}
