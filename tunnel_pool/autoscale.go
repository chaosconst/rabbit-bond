package tunnel_pool

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Automatic tunnel count per endpoint (client side, RABBIT_AUTO_TUNNELS=1); -rabbit-addr then
// only lists the endpoints and weights / -tunnelN are ignored.
//
// Several tunnels per network path are needed because a single TCP flow on a long, lossy or
// per-flow-policed path cannot fill it; how many depends on the path and the hour. So each
// endpoint climbs: while the pool is busy, one endpoint at a time doubles its tunnels and keeps
// them only if each added tunnel brought at least half an average tunnel's rate to the *total*
// (a path that only takes load over from another path has not added anything). The first step
// that does not pay is undone and remembered as the endpoint's plateau. While busy, every
// autoReprobe the plateau is re-tested with a 25% step, so it follows the path. After autoIdle
// without load an endpoint shrinks to autoMin; when load returns it goes straight back to its
// plateau instead of climbing again.
var (
	autoTunnels = os.Getenv("RABBIT_AUTO_TUNNELS") == "1"
	autoInit    = envInt("RABBIT_AUTO_INIT", 4)
	autoMin     = envInt("RABBIT_AUTO_MIN", 2)
	autoMax     = envInt("RABBIT_AUTO_MAX", 32)
	autoBusy    = float64(envInt("RABBIT_AUTO_BUSY_MBPS", 50)) * 1e6 / 8 // bytes/s
)

const (
	autoTick    = 500 * time.Millisecond
	autoWindow  = 3 * time.Second // rate averaging window
	autoSettle  = time.Second     // after the new tunnels are up, before measuring
	autoIdle    = time.Minute
	autoReprobe = time.Minute
)

type autoPath struct {
	target, plateau int
	lastBusy        time.Time
	settledAt       time.Time
}

type autoProbe struct {
	ep         string
	before     int     // tunnels before the step
	rateBefore float64 // total rate before the step (bytes/s)
	upAt       time.Time
	doubling   bool
}

type autoScaler struct {
	mu      sync.Mutex
	paths   map[string]*autoPath
	order   []string
	probe   *autoProbe
	seen    map[uint32]uint64 // tunnel -> recv already counted
	total   uint64            // cumulative bytes received over all tunnels, ever
	history []autoPoint
	logf    func(string, ...interface{})
}

type autoPoint struct {
	t     time.Time
	total uint64
}

func endpointAddrs(spec string) []string {
	var out []string
	for _, p := range strings.Split(spec, ",") {
		out = append(out, strings.SplitN(p, "*", 2)[0])
	}
	return out
}

// autoState is the live scaler, for /debug/flow.
var (
	autoStateMu sync.Mutex
	autoState   *autoScaler
	autoLog     []string // last decisions
)

func newAutoScaler(spec string, logf func(string, ...interface{})) *autoScaler {
	a := &autoScaler{paths: map[string]*autoPath{}, seen: map[uint32]uint64{}}
	a.logf = func(f string, v ...interface{}) {
		autoStateMu.Lock()
		autoLog = append(autoLog, time.Now().Format("15:04:05 ")+fmt.Sprintf(f, v...))
		if len(autoLog) > 20 {
			autoLog = autoLog[len(autoLog)-20:]
		}
		autoStateMu.Unlock()
		logf(f, v...)
	}
	autoStateMu.Lock()
	autoState = a
	autoStateMu.Unlock()
	for _, ep := range endpointAddrs(spec) {
		a.paths[ep] = &autoPath{target: autoInit, lastBusy: time.Now()} // no idle shrink right after start
		a.order = append(a.order, ep)
	}
	return a
}

// want returns how many more tunnels each endpoint needs.
func (a *autoScaler) want(count map[string]int) map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]int{}
	for ep, p := range a.paths {
		if d := p.target - count[ep]; d > 0 {
			out[ep] = d
		}
	}
	return out
}

// rate is the total receive rate over the last autoWindow (bytes/s), ok=false if not enough history.
func (a *autoScaler) rate(now time.Time) (float64, bool) {
	h := a.history
	if len(h) < 2 {
		return 0, false
	}
	last := h[len(h)-1]
	for i := len(h) - 1; i >= 0; i-- {
		if last.t.Sub(h[i].t) >= autoWindow {
			return float64(last.total-h[i].total) / last.t.Sub(h[i].t).Seconds(), true
		}
	}
	return 0, false
}

// step runs one control round. It returns endpoint -> tunnels to close.
func (a *autoScaler) step(now time.Time, tunnels []*Tunnel) map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	count := map[string]int{}
	live := map[uint32]bool{}
	for _, t := range tunnels {
		count[t.endpoint]++
		live[t.tunnelID] = true
		r := t.flow.recv
		a.total += r - a.seen[t.tunnelID]
		a.seen[t.tunnelID] = r
	}
	for id := range a.seen {
		if !live[id] {
			delete(a.seen, id)
		}
	}
	a.history = append(a.history, autoPoint{now, a.total})
	for len(a.history) > 2 && now.Sub(a.history[1].t) > 2*autoWindow {
		a.history = a.history[1:]
	}
	rate, ok := a.rate(now)
	busy := ok && rate >= autoBusy
	for _, p := range a.paths {
		if busy {
			p.lastBusy = now
		}
	}

	if pr := a.probe; pr != nil {
		p := a.paths[pr.ep]
		switch {
		case !busy:
			// Load ended mid-probe: nothing learned.
			a.logf("auto %s: load ended, probe %d->%d dropped\n", pr.ep, pr.before, p.target)
			p.target, a.probe = pr.before, nil
		case count[pr.ep] < p.target:
			// still dialing
		case pr.upAt.IsZero():
			pr.upAt = now
		case now.Sub(pr.upAt) >= autoSettle+autoWindow:
			added := p.target - pr.before
			need := 0.5 * float64(added) * pr.rateBefore / float64(imax(1, a.tunnelCount(count)-added))
			gain := rate - pr.rateBefore
			if gain >= need {
				a.logf("auto %s: %d->%d tunnels, total %.0f->%.0f Mbit/s, kept\n", pr.ep, pr.before, p.target, pr.rateBefore*8/1e6, rate*8/1e6)
				if pr.doubling && p.target < autoMax {
					a.probe = &autoProbe{ep: pr.ep, before: p.target, rateBefore: rate, doubling: true}
					p.target = imin(2*p.target, autoMax)
				} else {
					p.plateau, p.settledAt, a.probe = p.target, now, nil
				}
			} else {
				a.logf("auto %s: %d->%d tunnels, total %.0f->%.0f Mbit/s, undone (plateau %d)\n", pr.ep, pr.before, p.target, pr.rateBefore*8/1e6, rate*8/1e6, pr.before)
				p.target, p.plateau, p.settledAt, a.probe = pr.before, pr.before, now, nil
			}
		}
	} else if busy {
		// Return to known plateaus first, then probe one endpoint at a time.
		for _, ep := range a.order {
			if p := a.paths[ep]; p.plateau > 0 && p.target < p.plateau {
				p.target = p.plateau
			}
		}
		for _, ep := range a.order {
			p := a.paths[ep]
			if count[ep] < p.target {
				break // wait until all are up before measuring anything
			}
			if p.plateau == 0 && p.target < autoMax {
				a.probe = &autoProbe{ep: ep, before: p.target, rateBefore: rate, doubling: true}
				p.target = imin(2*p.target, autoMax)
				break
			}
			if p.plateau > 0 && now.Sub(p.settledAt) >= autoReprobe && p.target < autoMax {
				a.probe = &autoProbe{ep: ep, before: p.target, rateBefore: rate}
				p.target = imin(p.target+imax(1, p.target/4), autoMax)
				p.settledAt = now
				break
			}
		}
	}

	close := map[string]int{}
	for ep, p := range a.paths {
		if !busy && now.Sub(p.lastBusy) >= autoIdle && p.target > autoMin && a.probe == nil {
			p.target = autoMin
		}
		if d := count[ep] - p.target; d > 0 {
			close[ep] = d
		}
	}
	return close
}

func (a *autoScaler) tunnelCount(count map[string]int) int {
	n := 0
	for _, c := range count {
		n += c
	}
	if n == 0 {
		return 1
	}
	return n
}

// run drives the pool: dials what is missing (through cm.DecreaseNotify) and closes extras.
func (a *autoScaler) run(cm *ClientManager, pool *TunnelPool) {
	t := time.NewTicker(autoTick)
	defer t.Stop()
	for {
		select {
		case <-pool.ctx.Done():
			return
		case now := <-t.C:
			pool.mutex.Lock()
			var ts []*Tunnel
			for _, tun := range pool.tunnelMapping {
				if tun.flow != nil {
					ts = append(ts, tun)
				}
			}
			pool.mutex.Unlock()
			for ep, n := range a.step(now, ts) {
				// Close the newest tunnels of that endpoint; their unacked blocks are re-queued.
				var mine []*Tunnel
				for _, tun := range ts {
					if tun.endpoint == ep {
						mine = append(mine, tun)
					}
				}
				sort.Slice(mine, func(i, j int) bool { return mine[i].created.After(mine[j].created) })
				for i := 0; i < n && i < len(mine); i++ {
					mine[i].closeThenCancel()
				}
			}
			go cm.DecreaseNotify(pool)
		}
	}
}

func imin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func imax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// debugAuto reports targets, plateaus and recent decisions (nil when auto scaling is off).
func debugAuto() interface{} {
	autoStateMu.Lock()
	a, log := autoState, append([]string(nil), autoLog...)
	autoStateMu.Unlock()
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	paths := map[string]interface{}{}
	for ep, p := range a.paths {
		paths[ep] = map[string]int{"target": p.target, "plateau": p.plateau}
	}
	return map[string]interface{}{"paths": paths, "log": log}
}
