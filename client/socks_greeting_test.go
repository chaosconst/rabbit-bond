package client

import (
	"bytes"
	"io"
	"net"
	"testing"
)

// talk runs answerSocks against an application that sends sent and then reads want bytes.
func talk(t *testing.T, sent []byte, want int) (prefix []byte, drop socksDrop, reply []byte) {
	// A real TCP pair: like the application's socket it buffers (net.Pipe would deadlock on the
	// application writing greeting + request at once while the reply is written).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	app, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	rabbit, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	defer rabbit.Close()
	got := make(chan []byte, 1)
	go func() {
		app.Write(sent)
		buf := make([]byte, want)
		n, _ := io.ReadFull(app, buf)
		got <- buf[:n]
	}()
	prefix, drop, err = answerSocks(rabbit)
	if err != nil {
		t.Fatal(err)
	}
	if want > 0 {
		reply = <-got
	}
	return
}

func setMode(t *testing.T, greeting, connect bool) {
	g, c := socksLocalGreeting, socksLocalConnect
	socksLocalGreeting, socksLocalConnect = greeting, connect
	t.Cleanup(func() { socksLocalGreeting, socksLocalConnect = g, c })
}

var connectReq = []byte{5, 1, 0, 3, 16, 'c', 'l', 'o', 'u', 'd', '.', 'i', 'n', 'f', 'e', 'r', 'o', '.', 'n', 'e', 't', 1, 187}

func TestSocksGreetingAnsweredLocally(t *testing.T) {
	setMode(t, true, false)
	prefix, drop, reply := talk(t, []byte{5, 2, 2, 0}, 2) // offers user/pass and no-auth
	if !bytes.Equal(reply, []byte{5, 0}) || !bytes.Equal(prefix, []byte{5, 1, 0}) || drop != (socksDrop{greeting: true}) {
		t.Fatalf("reply %v prefix %v drop %v", reply, prefix, drop)
	}
	if !dropSocksReplies(bytes.NewReader([]byte{5, 0}), drop) || dropSocksReplies(bytes.NewReader([]byte{5, 0xff}), drop) {
		t.Fatal("greeting answer handling")
	}
}

func TestSocksConnectAnsweredLocally(t *testing.T) {
	setMode(t, true, true)
	sent := append([]byte{5, 1, 0}, connectReq...)
	prefix, drop, reply := talk(t, sent, 2+len(socksReplyConnected))
	if !bytes.Equal(reply, append([]byte{5, 0}, socksReplyConnected...)) {
		t.Fatalf("reply %v", reply)
	}
	if !bytes.Equal(prefix, sent) || drop != (socksDrop{greeting: true, connect: true}) {
		t.Fatalf("prefix %v drop %v: the far server must get greeting + request", prefix, drop)
	}
	// Far answers: greeting, then CONNECT success with an IPv4 or a domain bound address, then data.
	for _, far := range [][]byte{
		{5, 0, 5, 0, 0, 1, 1, 2, 3, 4, 0, 80, 'H', 'i'},
		{5, 0, 5, 0, 0, 3, 3, 'a', 'b', 'c', 0, 80, 'H', 'i'},
	} {
		r := bytes.NewReader(far)
		if !dropSocksReplies(r, drop) {
			t.Fatalf("%v: rejected", far)
		}
		if rest, _ := io.ReadAll(r); string(rest) != "Hi" {
			t.Fatalf("%v: data after the answers %q", far, rest)
		}
	}
	if dropSocksReplies(bytes.NewReader([]byte{5, 0, 5, 4, 0, 1, 0, 0, 0, 0, 0, 0}), drop) {
		t.Fatal("a refused CONNECT must close the connection")
	}
}

func TestSocksUDPAssociateOnlyGreeting(t *testing.T) {
	setMode(t, true, true)
	req := []byte{5, 3, 0, 1, 0, 0, 0, 0, 0, 0}
	prefix, drop, _ := talk(t, append([]byte{5, 1, 0}, req...), 2)
	if drop != (socksDrop{greeting: true}) || !bytes.Equal(prefix, append([]byte{5, 1, 0}, req...)) {
		t.Fatalf("prefix %v drop %v", prefix, drop)
	}
}

func TestSocksPassthrough(t *testing.T) {
	setMode(t, true, true)
	for _, sent := range [][]byte{{5, 1, 2}, {'G', 'E'}} { // auth required; not SOCKS5
		prefix, drop, _ := talk(t, sent, 0)
		if !bytes.Equal(prefix, sent) || drop != (socksDrop{}) {
			t.Fatalf("%v: prefix %v drop %v, want relayed untouched", sent, prefix, drop)
		}
	}
}
