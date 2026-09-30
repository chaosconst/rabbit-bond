package tunnel_pool

import (
	"sync/atomic"

	"github.com/chaosconst/rabbit-bond/block"
)

// Redundancy accounting for /debug/flow: the share of the bytes this end writes to its
// tunnels that are extra copies. Extra = duplicated blocks (head blocks, small datagrams)
// plus re-sent blocks (NACKed holes, stuck blocks re-injected, blocks of a failed write or
// a dead tunnel re-queued). A re-queued block counts when it is queued, so a copy dropped
// later (its connection closed meanwhile) still counts: the share is an upper bound.
var BytesSent, DupBytes, ResendBytes uint64

func blockBytes(blk block.Block) uint64 {
	return uint64(block.HeaderSize + len(blk.BlockData))
}

func addResend(blks []block.Block) {
	var n uint64
	for _, blk := range blks {
		n += blockBytes(blk)
	}
	atomic.AddUint64(&ResendBytes, n)
}

// redundancyPct returns (DupBytes + ResendBytes) / BytesSent in percent.
func redundancyPct() float64 {
	sent := atomic.LoadUint64(&BytesSent)
	if sent == 0 {
		return 0
	}
	extra := atomic.LoadUint64(&DupBytes) + atomic.LoadUint64(&ResendBytes)
	return float64(extra) * 100 / float64(sent)
}
