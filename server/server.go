package server

import (
	"github.com/chaosconst/rabbit-bond/logger"
	"github.com/chaosconst/rabbit-bond/peer"
	"github.com/chaosconst/rabbit-bond/tunnel"
	"net"
)

type Server struct {
	peerGroup peer.PeerGroup
	logger    *logger.Logger
}

func NewServer(cipher tunnel.Cipher) Server {
	return Server{
		peerGroup: peer.NewPeerGroup(cipher),
		logger:    logger.NewLogger("[Server]"),
	}
}

func (s *Server) Serve(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Errorf("Error when accept connection: %v.\n", err)
			continue
		}
		// Each in its own goroutine: the handshake waits for the client, and one connection that
		// never sends it (a path that died while dialing) held up every later tunnel of every client.
		go func(conn net.Conn) {
			if err := s.peerGroup.AddTunnelFromConn(conn); err != nil {
				s.logger.Errorf("Error when add tunnel to tunnel pool: %v.\n", err)
			}
		}(conn)
	}
}
