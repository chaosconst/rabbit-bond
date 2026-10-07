package tunnel_pool

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseReasonRecordedOnce(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	tun := &Tunnel{Conn: a, ctx: ctx, cancel: cancel, tunnelID: 7, endpoint: "127.0.0.1:12455", line: "cu", created: time.Now().Add(-90 * time.Second)}
	atomic.StoreInt64(&tun.lastRecv, time.Now().Add(-1500*time.Millisecond).UnixNano())
	before := len(recentCloses())
	tun.closeWhy("read: EOF")
	tun.closeWhy("write: broken pipe")
	tun.noteClose("cancelled")
	evs := recentCloses()
	if len(evs) != before+1 {
		t.Fatalf("%d events recorded, want 1", len(evs)-before)
	}
	ev := evs[len(evs)-1]
	if ev.Why != "read: EOF" || ev.Tunnel != 7 || ev.Line != "cu" || ev.AgeS < 89 || ev.IdleMs < 1400 {
		t.Fatalf("event %+v", ev)
	}
	if ctx.Err() == nil {
		t.Fatal("closeWhy must still close and cancel")
	}
}
