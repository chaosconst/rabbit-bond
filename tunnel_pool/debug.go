package tunnel_pool

import (
	"encoding/json"

	"github.com/ihciah/rabbit-tcp/connection"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Live pools for /debug/flow (served on the pprof listener when RABBIT_PPROF is set).
var (
	livePools   = map[*TunnelPool]struct{}{}
	livePoolsMu sync.Mutex
	reinjectCnt uint64
)

func registerPool(tp *TunnelPool) {
	livePoolsMu.Lock()
	livePools[tp] = struct{}{}
	livePoolsMu.Unlock()
	go func() {
		<-tp.ctx.Done()
		livePoolsMu.Lock()
		delete(livePools, tp)
		livePoolsMu.Unlock()
	}()
}

type tunnelStat struct {
	ID       uint32  `json:"id"`
	Endpoint string  `json:"endpoint"`
	Line     string  `json:"line"`
	Recv     uint64  `json:"recv"`
	Sent     uint64  `json:"sent"`
	Acked    uint64  `json:"acked"`
	Cwnd     uint64  `json:"cwnd"`
	Inflight uint64  `json:"inflight"`
	RateMbps float64 `json:"rate_mbps"`
	MinRTTms float64 `json:"min_rtt_ms"`
	SchedRTT float64 `json:"sched_rtt_ms"`
	Nacks    uint64  `json:"nacks"`
	LateP    float64 `json:"late_p"`
	LateByMs float64 `json:"late_by_ms"`
	Lates    uint64  `json:"lates"`
	ExtraMs  float64 `json:"extra_ms"`
	Stuck    bool    `json:"stuck"`
}

func init() {
	http.HandleFunc("/debug/flow", func(w http.ResponseWriter, r *http.Request) {
		out := map[string]interface{}{"t": float64(time.Now().UnixNano()) / 1e9}
		var peers []map[string]interface{}
		livePoolsMu.Lock()
		for tp := range livePools {
			tp.mutex.Lock()
			var ts []tunnelStat
			for _, t := range tp.tunnelMapping {
				if t.flow == nil {
					continue
				}
				cwnd, inflight, rate, rtt := t.flow.stats()
				t.flow.mu.Lock()
				sent, acked, srtt, nacks := t.flow.sent, t.flow.acked, t.flow.schedRTTLocked(), t.flow.nacks
				lateP, lateBy, lates, extra := t.flow.latePLocked(time.Now()), t.flow.lateBy, t.flow.lates, t.flow.extraLocked(time.Now())
				t.flow.mu.Unlock()
				_, stuck := t.flow.estimate(0, time.Now())
				ts = append(ts, tunnelStat{t.tunnelID, t.endpoint, t.line, t.flow.recv, sent, acked, cwnd, inflight,
					rate * 8 / 1e6, float64(rtt.Microseconds()) / 1000, float64(srtt.Microseconds()) / 1000, nacks,
					lateP, float64(lateBy.Microseconds()) / 1000, lates, float64(extra.Microseconds()) / 1000, stuck})
			}
			tp.mutex.Unlock()
			peers = append(peers, map[string]interface{}{"peer": tp.peerID, "tunnels": ts})
		}
		livePoolsMu.Unlock()
		out["peers"] = peers
		if a := debugAuto(); a != nil {
			out["auto"] = a
		}
		out["reinjected"] = atomic.LoadUint64(&reinjectCnt)
		out["nack_resent"] = atomic.LoadUint64(&NackResent)
		out["dup_sent"] = atomic.LoadUint64(&DupSent)
		out["dgram_sent"] = atomic.LoadUint64(&DatagramSent)
		out["dgram_recv"] = atomic.LoadUint64(&DatagramRecv)
		out["dgram_queue_drop"] = atomic.LoadUint64(&DatagramQueueDrop)
		out["dgram_too_old"] = atomic.LoadUint64(&DatagramTooOld)
		out["nack_sent"] = connection.NackSent.Load()
		json.NewEncoder(w).Encode(out)
	})
}
