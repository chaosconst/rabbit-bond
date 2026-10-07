package tunnel_pool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Why tunnels close, for /debug/flow. All tunnels of a pool were seen closing within the same second,
// a few times an hour, with nothing in the logs (production 2026-10-01): which side saw the error
// first, and what error (EOF: the far end closed; reset; timeout: nothing arrived), says where the
// cut is. Each tunnel records its first reason only.

type closeEvent struct {
	At       float64 `json:"t"`
	Tunnel   uint32  `json:"tunnel"`
	Peer     uint32  `json:"peer"`
	Endpoint string  `json:"endpoint,omitempty"`
	Line     string  `json:"line,omitempty"`
	Why      string  `json:"why"`
	AgeS     float64 `json:"age_s"`
	IdleMs   int64   `json:"idle_ms"` // since the last block arrived
}

const closeKeep = 256

var closeLog struct {
	mu     sync.Mutex
	events []closeEvent
}

// closeWhy closes the tunnel, recording why if it is the first reason given.
func (tunnel *Tunnel) closeWhy(why string) {
	tunnel.noteClose(why)
	tunnel.closeThenCancel()
}

func (tunnel *Tunnel) noteClose(why string) {
	if !atomic.CompareAndSwapInt32(&tunnel.closeNoted, 0, 1) {
		return
	}
	now := time.Now()
	ev := closeEvent{
		At:       float64(now.UnixNano()) / 1e9,
		Tunnel:   tunnel.tunnelID,
		Peer:     tunnel.peerID,
		Endpoint: tunnel.endpoint,
		Line:     tunnel.line,
		Why:      why,
		AgeS:     now.Sub(tunnel.created).Seconds(),
	}
	if last := atomic.LoadInt64(&tunnel.lastRecv); last > 0 {
		ev.IdleMs = now.Sub(time.Unix(0, last)).Milliseconds()
	}
	closeLog.mu.Lock()
	closeLog.events = append(closeLog.events, ev)
	if len(closeLog.events) > closeKeep {
		closeLog.events = closeLog.events[len(closeLog.events)-closeKeep:]
	}
	closeLog.mu.Unlock()
}

func recentCloses() []closeEvent {
	closeLog.mu.Lock()
	defer closeLog.mu.Unlock()
	return append([]closeEvent(nil), closeLog.events...)
}
