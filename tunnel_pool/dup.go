package tunnel_pool

import (
	"os"
	"strings"
	"time"

	"github.com/ihciah/rabbit-tcp/block"
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
var (
	flowDupFirst = uint32(envInt("RABBIT_FLOW_DUP_FIRST", 0))
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

func wantDup(blk block.Block) bool {
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
		est, stuck := t.flow.estimate(block.MaxSize, now)
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
