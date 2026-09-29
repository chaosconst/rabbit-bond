package tunnel_pool

const (
	ErrorWaitSec          = 3  // If a tunnel cannot be dialed, will wait for this period and retry infinitely
	TunnelBlockTimeoutSec = 8  // If a tunnel cannot send a block within the limit, will treat it a dead tunnel
	HandshakeTimeoutSec   = 10 // A new tunnel must finish the peerID exchange within this
	EmptyPoolDestroySec   = 60 // The pool will be destroyed(server side) if no tunnel dialed in
	SendQueueSize         = 48 // SendQueue channel cap
	RecvQueueSize         = 48 // RecvQueue channel cap
)
