package main

import (
	"flag"
	"fmt"
	"github.com/ihciah/rabbit-tcp/client"
	"github.com/ihciah/rabbit-tcp/connection"
	"github.com/ihciah/rabbit-tcp/logger"
	"github.com/ihciah/rabbit-tcp/server"
	"github.com/ihciah/rabbit-tcp/tunnel"
	"github.com/ihciah/rabbit-tcp/tunnel_pool"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var Version = "No version information"

var flowControl bool

const (
	ClientMode = iota
	ServerMode
	DefaultPassword = "PASSWORD"
)

func parseFlags() (pass bool, mode int, password string, addr string, listen string, dest string, tunnelN int, verbose int) {
	var modeString string
	var printVersion bool
	flag.StringVar(&modeString, "mode", "c", "running mode(s or c)")
	flag.StringVar(&password, "password", DefaultPassword, "password")
	flag.StringVar(&addr, "rabbit-addr", ":443", "listen(server mode) or remote(client mode) address used by rabbit-tcp")
	flag.StringVar(&listen, "listen", "", "[Client Only] listen address, eg: 127.0.0.1:2333")
	flag.StringVar(&dest, "dest", "", "[Client Only] destination address, eg: shadowsocks server address")
	flag.IntVar(&tunnelN, "tunnelN", 4, "[Client Only] number of tunnels to use in rabbit-tcp")
	flag.IntVar(&verbose, "verbose", 2, "verbose level(0~5)")
	flag.BoolVar(&printVersion, "version", false, "show version")
	flag.BoolVar(&flowControl, "flow", false, "per-tunnel ack and in-flight limiting for tunnels over paths of different speed (both peers must enable it)")
	flag.Parse()

	pass = true

	// version
	if printVersion {
		log.Println("Rabbit TCP (https://github.com/ihciah/rabbit-tcp/)")
		log.Printf("Version: %s.\n", Version)
		pass = false
		return
	}

	// mode
	modeString = strings.ToLower(modeString)
	if modeString == "c" || modeString == "client" {
		mode = ClientMode
	} else if modeString == "s" || modeString == "server" {
		mode = ServerMode
	} else {
		log.Printf("Unsupported mode %s.\n", modeString)
		pass = false
		return
	}

	// password
	if password == "" {
		log.Println("Password must be specified.")
		pass = false
		return
	}
	if password == DefaultPassword {
		log.Println("Password must be changed instead of default password.")
		pass = false
		return
	}

	// listen, dest, tunnelN
	if mode == ClientMode {
		if listen == "" {
			log.Println("Listen address must be specified in client mode.")
			pass = false
		}
		if dest == "" {
			log.Println("Destination address must be specified in client mode.")
			pass = false
		}
		if tunnelN == 0 {
			log.Println("Tunnel number must be positive.")
			pass = false
		}
	}
	return
}

func main() {
	pass, mode, password, addr, listen, dest, tunnelN, verbose := parseFlags()
	if !pass {
		return
	}
	// Tunnels usually already ride inside TLS, so this layer mostly authenticates.
	// RABBIT_CIPHER=AES-128-GCM uses the CPU's AES instructions; both peers must agree.
	cipherName := "CHACHA20-IETF-POLY1305"
	if v := os.Getenv("RABBIT_CIPHER"); v != "" {
		cipherName = v
	}
	cipher, err := tunnel.NewAEADCipher(cipherName, nil, password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cipher %s: %v\n", cipherName, err)
		os.Exit(2)
	}
	logger.LEVEL = verbose
	tunnel_pool.FlowControl = flowControl
	if flowControl {
		// RABBIT_NACK_MS=0 disables NACKs (peer without NACK support); default 200 ms.
		ms := 200
		if v, err := strconv.Atoi(os.Getenv("RABBIT_NACK_MS")); err == nil {
			ms = v
		}
		connection.NackAfter = time.Duration(ms) * time.Millisecond
	}
	if mode == ClientMode {
		c := client.NewClient(tunnelN, addr, cipher)
		c.ServeForward(listen, dest)
	} else {
		s := server.NewServer(cipher)
		s.Serve(addr)
	}
}
