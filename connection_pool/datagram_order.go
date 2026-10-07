package connection_pool

import (
	"github.com/chaosconst/rabbit-bond/tunnel_pool"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// RABBIT_FLOW_DGRAM_HOLD_MS > 0: the receiving end hands a session's datagrams on in the order they
// were sent, holding one that arrives after a gap until the gap is filled or it has waited this
// long. Sequence numbers are given where datagrams enter the tunnels, so a gap is almost always a
// datagram still on its way on another tunnel (a loss on the real path leaves no gap), and
// waiting turns reordering, which QUIC and TCP-like senders count as loss, into a little delay.
// A gap that does not fill (a tunnel died with datagrams queued) costs one hold.
var dgramHold = time.Duration(envMs("RABBIT_FLOW_DGRAM_HOLD_MS")) * time.Millisecond

const dgramHoldMax = 4096 // datagrams held per session at most; beyond that the oldest gap is skipped

func envMs(name string) int {
	n, _ := strconv.Atoi(os.Getenv(name))
	return n
}

type datagramOrder struct {
	mu      sync.Mutex
	deliver func([]byte)
	next    uint32
	started bool
	held    map[uint32][]byte
	timer   *time.Timer
}

func newDatagramOrder(deliver func([]byte)) *datagramOrder {
	return &datagramOrder{deliver: deliver, held: map[uint32][]byte{}}
}

// push takes a fresh (not duplicate) datagram and delivers what is now in order.
func (o *datagramOrder) push(seq uint32, frame []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.started {
		o.started, o.next = true, seq
	}
	switch d := int32(seq - o.next); {
	case d < 0: // its gap was already skipped: late anyway, hand it on now
		atomic.AddUint64(&tunnel_pool.DatagramHoldLate, 1)
		o.deliver(frame)
	case d == 0:
		o.deliver(frame)
		o.next++
		o.drain()
	default:
		if len(o.held) >= dgramHoldMax {
			o.skip(&tunnel_pool.DatagramHoldSkipFull)
		}
		o.held[seq] = frame
	}
	if len(o.held) == 0 {
		if o.timer != nil {
			o.timer.Stop()
			o.timer = nil
		}
	} else if o.timer == nil {
		o.timer = time.AfterFunc(dgramHold, o.expire)
	}
}

func (o *datagramOrder) drain() {
	for {
		f, ok := o.held[o.next]
		if !ok {
			return
		}
		delete(o.held, o.next)
		o.deliver(f)
		o.next++
	}
}

// skip gives up on the gap before the earliest held datagram.
func (o *datagramOrder) skip(counter *uint64) {
	first := true
	var low uint32
	for seq := range o.held {
		if first || int32(seq-low) < 0 {
			low, first = seq, false
		}
	}
	if !first {
		atomic.AddUint64(counter, uint64(low-o.next))
		o.next = low
		o.drain()
	}
}

func (o *datagramOrder) expire() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.timer = nil
	if len(o.held) == 0 {
		return
	}
	o.skip(&tunnel_pool.DatagramHoldSkipTimer)
	if len(o.held) > 0 {
		o.timer = time.AfterFunc(dgramHold, o.expire)
	}
}
