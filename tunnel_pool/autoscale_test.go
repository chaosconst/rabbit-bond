package tunnel_pool

import (
	"fmt"
	"testing"
	"time"
)

// simPath: each tunnel carries perTunnel Mbit/s until the path's capacity (like a per-flow
// policed line); beyond knee tunnels the path loses a little (more flows, more contention).
type simPath struct {
	perTunnel, capacity float64
	knee                int
}

func (p simPath) rate(n int) float64 {
	r := p.perTunnel * float64(n)
	if r > p.capacity {
		r = p.capacity
	}
	if n > p.knee {
		r -= float64(n-p.knee) * 2
	}
	return r
}

type sim struct {
	t       *testing.T
	a       *autoScaler
	paths   map[string]simPath
	tunnels []*Tunnel
	now     time.Time
	nextID  uint32
}

func (s *sim) counts() map[string]int {
	c := map[string]int{}
	for _, t := range s.tunnels {
		c[t.endpoint]++
	}
	return c
}

// run advances d with the given demand (Mbit/s, 0 = idle); dialing is instant.
func (s *sim) run(d time.Duration, demand float64) {
	for end := s.now.Add(d); s.now.Before(end); s.now = s.now.Add(autoTick) {
		for ep, n := range s.a.want(s.counts()) {
			for i := 0; i < n; i++ {
				s.nextID++
				s.tunnels = append(s.tunnels, &Tunnel{tunnelID: s.nextID, endpoint: ep, flow: newFlowState(), created: s.now})
			}
		}
		c := s.counts()
		total := 0.0
		rates := map[string]float64{}
		for ep, p := range s.paths {
			rates[ep] = p.rate(c[ep])
			total += rates[ep]
		}
		scale := 1.0
		if total > demand {
			scale = demand / total
		}
		for _, t := range s.tunnels {
			share := rates[t.endpoint] * scale / float64(c[t.endpoint])
			t.flow.recv += uint64(share * 1e6 / 8 * autoTick.Seconds())
		}
		for ep, n := range s.a.step(s.now, s.tunnels) {
			var keep []*Tunnel
			for i := len(s.tunnels) - 1; i >= 0; i-- {
				if s.tunnels[i].endpoint == ep && n > 0 {
					n--
					continue
				}
				keep = append([]*Tunnel{s.tunnels[i]}, keep...)
			}
			s.tunnels = keep
		}
	}
}

func TestAutoScaleClimbsToPlateauShrinksAndReturns(t *testing.T) {
	s := &sim{t: t, now: time.Unix(0, 0),
		paths: map[string]simPath{"home": {35, 520, 20}, "g5": {30, 260, 12}},
		a:     newAutoScaler("home,g5", func(f string, v ...interface{}) { t.Logf(f, v...) })}
	s.run(90*time.Second, 10000)
	c := s.counts()
	t.Logf("after load: %v", c)
	if c["home"] < 14 || c["home"] > 32 || c["g5"] < 8 || c["g5"] > 16 {
		t.Fatalf("did not climb to the plateau: %v", c)
	}
	s.run(2*time.Minute, 0)
	if c := s.counts(); c["home"] != autoMin || c["g5"] != autoMin {
		t.Fatalf("did not shrink when idle: %v", c)
	}
	s.run(2*time.Second, 10000)
	c2 := s.counts()
	t.Logf("load again: %v", c2)
	if c2["home"] != s.a.paths["home"].plateau || c2["g5"] != s.a.paths["g5"].plateau {
		t.Fatalf("did not return to the plateau: %v", c2)
	}
}

func TestAutoScaleAppLimitedStaysSmall(t *testing.T) {
	s := &sim{t: t, now: time.Unix(0, 0),
		paths: map[string]simPath{"home": {35, 520, 20}, "g5": {30, 260, 12}},
		a:     newAutoScaler("home,g5", func(string, ...interface{}) {})}
	s.run(60*time.Second, 120) // e.g. a video: 120 Mbit/s, more tunnels bring nothing
	if c := s.counts(); c["home"] > 8 || c["g5"] > 8 {
		t.Fatalf("grew without gain: %v", c)
	}
}

func TestAutoScaleDoesNotProbeDuringRampUp(t *testing.T) {
	var logs []string
	s := &sim{t: t, now: time.Unix(0, 0),
		paths: map[string]simPath{"home": {35, 520, 20}, "g5": {30, 260, 12}},
		a:     newAutoScaler("home,g5", func(f string, v ...interface{}) { logs = append(logs, fmt.Sprintf(f, v...)) })}
	s.run(10*time.Second, 0)    // idle, then a transfer starts
	s.run(8*time.Second, 10000) // the first windows still contain the idle part
	for _, l := range logs {
		t.Fatalf("decided before the load was steady: %s", l)
	}
}

func TestAutoScaleNeverClosesWhileBusy(t *testing.T) {
	s := &sim{t: t, now: time.Unix(0, 0),
		paths: map[string]simPath{"home": {35, 520, 20}, "g5": {30, 260, 12}},
		a:     newAutoScaler("home,g5", func(string, ...interface{}) {})}
	max := 0
	for i := 0; i < 180; i++ { // 90 s of load, tracking whether a count ever went down
		before := s.counts()
		s.run(autoTick, 10000)
		after := s.counts()
		for ep := range before {
			if after[ep] < before[ep] {
				t.Fatalf("closed %s tunnels under load: %d -> %d", ep, before[ep], after[ep])
			}
		}
		if n := after["home"] + after["g5"]; n > max {
			max = n
		}
	}
	s.run(3*time.Second, 0) // load gone: surplus from undone steps is closed now
	c := s.counts()
	if c["home"] != s.a.paths["home"].target || c["g5"] != s.a.paths["g5"].target {
		t.Fatalf("surplus not closed after load: %v targets %d/%d", c, s.a.paths["home"].target, s.a.paths["g5"].target)
	}
	t.Logf("peak %d tunnels under load, after load %v", max, c)
}
