package tunnel_pool

import (
	"testing"
	"time"
)

func TestPickEndpointSkipsFailedEndpoint(t *testing.T) {
	cm := &ClientManager{endpoint: "home*2,g5*1,sh2*1", tunnelNum: 16}
	tp := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{1: {endpoint: "home"}, 2: {endpoint: "home"}}}
	if ep := cm.pickEndpoint(tp); ep != "g5" {
		t.Fatalf("picked %q, want the most under-filled g5", ep)
	}
	cm.failedAt = map[string]time.Time{"g5": time.Now()} // 5G unplugged
	if ep := cm.pickEndpoint(tp); ep != "sh2" {
		t.Fatalf("picked %q, want sh2 while g5 is backing off", ep)
	}
	cm.failedAt["sh2"], cm.failedAt["home"] = time.Now(), time.Now()
	if ep := cm.pickEndpoint(tp); ep != "" {
		t.Fatalf("picked %q, want none while all back off", ep)
	}
	cm.failedAt = map[string]time.Time{"g5": time.Now().Add(-dialBackoff)}
	if ep := cm.pickEndpoint(tp); ep != "g5" {
		t.Fatalf("picked %q, want g5 again after the backoff", ep)
	}
	// Shares are caps: with 5G down, home stops at 8 and sh2 at 4 instead of taking 5G's share.
	cm.failedAt = map[string]time.Time{"g5": time.Now()}
	full := &TunnelPool{tunnelMapping: map[uint32]*Tunnel{}}
	for i := 0; i < 12; i++ {
		ep := "home"
		if i >= 8 {
			ep = "sh2"
		}
		full.tunnelMapping[uint32(i)] = &Tunnel{endpoint: ep}
	}
	if ep := cm.pickEndpoint(full); ep != "" {
		t.Fatalf("picked %q with home 8/8 and sh2 4/4 while 5G is down, want none", ep)
	}
}
