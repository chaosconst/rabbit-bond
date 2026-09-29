package tunnel_pool

import (
	"context"
	"github.com/ihciah/rabbit-tcp/logger"
	"github.com/ihciah/rabbit-tcp/tunnel"
	"go.uber.org/atomic"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Manager interface {
	Notify(pool *TunnelPool)         // When TunnelPool size changed, Notify should be called
	DecreaseNotify(pool *TunnelPool) // When TunnelPool size decreased, DecreaseNotify should be called
}

type ClientManager struct {
	decreaseNotifyLock sync.Mutex // Only one decrease notify can run at the same time
	tunnelNum          int
	endpoint           string
	peerID             uint32
	cipher             tunnel.Cipher
	logger             *logger.Logger
	auto               *autoScaler // RABBIT_AUTO_TUNNELS=1
	autoOnce           sync.Once
	failedAt           map[string]time.Time // endpoint -> last failed dial (fixed mode; under decreaseNotifyLock)
}

func NewClientManager(tunnelNum int, endpoint string, peerID uint32, cipher tunnel.Cipher) ClientManager {
	return ClientManager{
		tunnelNum: tunnelNum,
		endpoint:  endpoint,
		cipher:    cipher,
		peerID:    peerID,
		logger:    logger.NewLogger("[ClientManager]"),
	}
}

// Keep tunnelPool size above tunnelNum (or, with RABBIT_AUTO_TUNNELS, each endpoint at its target)
func (cm *ClientManager) DecreaseNotify(pool *TunnelPool) {
	if autoTunnels && FlowControl {
		cm.autoOnce.Do(func() {
			cm.auto = newAutoScaler(cm.endpoint, cm.logger.Infof)
			go cm.auto.run(cm, pool)
		})
		cm.autoDial(pool)
		return
	}
	cm.decreaseNotifyLock.Lock()
	defer cm.decreaseNotifyLock.Unlock()
	tunnelCount := len(pool.tunnelMapping)

	for tunnelToCreate := cm.tunnelNum - tunnelCount; tunnelToCreate > 0; {
		select {
		case <-pool.ctx.Done():
			// Have to return if pool cancel is called.
			return
		default:
		}

		cm.logger.Infof("Need %d new tunnels now.\n", tunnelToCreate)
		endpoint := cm.pickEndpoint(pool)
		if endpoint == "" { // every endpoint failed just now
			time.Sleep(ErrorWaitSec * time.Second)
			continue
		}
		conn, err := net.Dial("tcp", endpoint)
		if err != nil {
			cm.logger.Errorf("Error when dial to %s: %v.\n", endpoint, err)
			cm.dialFailed(endpoint)
			continue
		}
		tun, err := NewActiveTunnel(conn, cm.cipher, cm.peerID)
		if err != nil {
			cm.logger.Errorf("Error when create active tunnel: %v\n", err)
			cm.dialFailed(endpoint)
			continue
		}
		tun.endpoint = endpoint
		pool.AddTunnel(&tun)
		tunnelToCreate--
		cm.logger.Infof("Successfully dialed to %s. TunnelToCreate: %d\n", endpoint, tunnelToCreate)
	}
}

// autoDial opens what the auto scaler asks for. Each endpoint dials on its own, so a path that
// hangs (no signal, a relay dropping packets) never holds up refilling the others.
func (cm *ClientManager) autoDial(pool *TunnelPool) {
	count := map[string]int{}
	pool.mutex.Lock()
	for _, t := range pool.tunnelMapping {
		count[t.endpoint]++
	}
	pool.mutex.Unlock()
	for ep, n := range cm.auto.want(count) {
		if !cm.auto.startDialing(ep) {
			continue // this endpoint is already dialing
		}
		go func(ep string, n int) {
			defer cm.auto.doneDialing(ep)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					cm.dialOne(pool, ep)
				}()
			}
			wg.Wait()
		}(ep, n)
	}
}

func (cm *ClientManager) dialOne(pool *TunnelPool, endpoint string) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		cm.logger.Errorf("Error when dial to %s: %v.\n", endpoint, err)
		time.Sleep(ErrorWaitSec * time.Second)
		return
	}
	tun, err := NewActiveTunnel(conn, cm.cipher, cm.peerID)
	if err != nil {
		cm.logger.Errorf("Error when create active tunnel: %v\n", err)
		time.Sleep(ErrorWaitSec * time.Second)
		return
	}
	tun.endpoint = endpoint
	pool.AddTunnel(&tun)
}

func (cm *ClientManager) Notify(pool *TunnelPool) {}

// pickEndpoint spreads tunnels over several endpoints (one per network path).
// -rabbit-addr accepts "host:port*weight,host:port*weight"; a new tunnel goes to the
// endpoint with the lowest (tunnels+1)/weight, so a path that lost tunnels is refilled first.
// dialFailed takes an endpoint out of pickEndpoint for dialBackoff. Without it a dead endpoint
// (5G unplugged) stays the most under-filled one and is picked again and again, so the pool never
// refills the live paths (seen: 2 of 16 tunnels after a restart with 5G off).
func (cm *ClientManager) dialFailed(endpoint string) {
	if cm.failedAt == nil {
		cm.failedAt = map[string]time.Time{}
	}
	cm.failedAt[endpoint] = time.Now()
	if len(strings.Split(cm.endpoint, ",")) == 1 {
		time.Sleep(ErrorWaitSec * time.Second)
	}
}

const dialBackoff = 10 * time.Second

// pickEndpoint returns the endpoint furthest below its weighted share, skipping endpoints whose
// dial failed within dialBackoff; "" if all of them did.
func (cm *ClientManager) pickEndpoint(pool *TunnelPool) string {
	parts := strings.Split(cm.endpoint, ",")
	if len(parts) == 1 {
		return strings.SplitN(parts[0], "*", 2)[0]
	}
	count := map[string]int{}
	pool.mutex.Lock()
	for _, t := range pool.tunnelMapping {
		count[t.endpoint]++
	}
	pool.mutex.Unlock()
	addrs, weights, sum := make([]string, len(parts)), make([]float64, len(parts)), 0.0
	for i, p := range parts {
		addrs[i], weights[i] = p, 1.0
		if j := strings.Index(p, "*"); j >= 0 {
			addrs[i] = p[:j]
			if v, err := strconv.ParseFloat(p[j+1:], 64); err == nil && v > 0 {
				weights[i] = v
			}
		}
		sum += weights[i]
	}
	best, bestScore := "", 0.0
	for i, addr := range addrs {
		w := weights[i]
		// Each endpoint is capped at its share of -tunnelN (8/4/4): a dead path's share is not
		// handed to the others, it is re-dialed after dialBackoff and filled when it comes back.
		if float64(count[addr]) >= math.Round(float64(cm.tunnelNum)*w/sum) {
			continue
		}
		if time.Since(cm.failedAt[addr]) < dialBackoff {
			continue
		}
		if score := float64(count[addr]+1) / w; best == "" || score < bestScore {
			best, bestScore = addr, score
		}
	}
	return best
}

type ServerManager struct {
	notifyLock          sync.Mutex // Only one notify can run in the same time
	removePeerFunc      context.CancelFunc
	cancelCountDownFunc context.CancelFunc
	triggered           atomic.Bool
	logger              *logger.Logger
}

func NewServerManager(removePeerFunc context.CancelFunc) ServerManager {
	return ServerManager{
		logger:         logger.NewLogger("[ServerManager]"),
		removePeerFunc: removePeerFunc,
	}
}

// If tunnelPool size is zero for more than EmptyPoolDestroySec, delete it
func (sm *ServerManager) Notify(pool *TunnelPool) {
	tunnelCount := len(pool.tunnelMapping)

	if tunnelCount == 0 && sm.triggered.CAS(false, true) {
		var destroyAfterCtx context.Context
		destroyAfterCtx, sm.cancelCountDownFunc = context.WithCancel(context.Background())
		go func(*ServerManager) {
			select {
			case <-destroyAfterCtx.Done():
				sm.logger.Debugln("ServerManager notify canceled.")
			case <-time.After(EmptyPoolDestroySec * time.Second):
				sm.logger.Infoln("ServerManager will be destroyed.")
				sm.removePeerFunc()
			}
		}(sm)
	}

	if tunnelCount != 0 && sm.triggered.CAS(true, false) {
		sm.cancelCountDownFunc()
	}
}

func (sm *ServerManager) DecreaseNotify(pool *TunnelPool) {}
