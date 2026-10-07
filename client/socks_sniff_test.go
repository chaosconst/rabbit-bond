package client

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

// recordConn keeps what crypto/tls writes; reads end at once.
type recordConn struct {
	replayConn
	w *bytes.Buffer
}

func (c recordConn) Write(p []byte) (int, error) { return c.w.Write(p) }

// clientHello returns the ClientHello Go's TLS client sends for name.
func clientHello(t *testing.T, name string) []byte {
	var w bytes.Buffer
	_ = tls.Client(recordConn{replayConn{r: bytes.NewReader(nil)}, &w}, &tls.Config{ServerName: name}).Handshake()
	if w.Len() == 0 || w.Bytes()[0] != 0x16 {
		t.Fatalf("no ClientHello recorded")
	}
	return w.Bytes()
}

var v6Req = append([]byte{5, 1, 0, 4}, append(net.ParseIP("2600:9000:21b5:5c00:7:59dd:7880:93a1").To16(), 1, 187)...)

func TestSniffHost(t *testing.T) {
	for _, c := range []struct {
		in   []byte
		want string
	}{
		{clientHello(t, "images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com"), "images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com"},
		{[]byte("GET /a HTTP/1.1\r\nUser-Agent: x\r\nHost: example.com:8080\r\n\r\n"), "example.com"},
		{[]byte("GET / HTTP/1.1\r\nhost: example.org\r\n\r\n"), "example.org"},
		{[]byte("GET / HTTP/1.1\r\nHost: [2001:db8::1]:80\r\n\r\n"), ""}, // an address names nothing new
		{[]byte("SSH-2.0-OpenSSH_9.6\r\n"), ""},
		{nil, ""},
	} {
		if got := sniffHost(c.in); got != c.want {
			t.Errorf("%.30q: got %q want %q", c.in, got, c.want)
		}
	}
}

func TestSocksConnectV6AskedByName(t *testing.T) {
	setMode(t, true, true)
	hello := clientHello(t, "img.example.com")
	sent := append(append([]byte{5, 1, 0}, v6Req...), hello...)
	prefix, drop, _ := talk(t, sent, 2+len(socksReplyConnected))
	named := append([]byte{5, 1, 0, 3, 15}, "img.example.com"...)
	want := append(append(append([]byte{5, 1, 0}, named...), 1, 187), hello...)
	if !bytes.Equal(prefix, want) || drop != (socksDrop{greeting: true, connect: true}) {
		t.Fatalf("prefix %v\nwant   %v", prefix, want)
	}
}

func TestSocksConnectV6ClientHelloInPieces(t *testing.T) {
	setMode(t, true, true)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	app, _ := net.Dial("tcp", ln.Addr().String())
	rabbit, _ := ln.Accept()
	defer app.Close()
	defer rabbit.Close()
	hello := clientHello(t, "split.example.com")
	go func() {
		app.Write(append([]byte{5, 1, 0}, v6Req...))
		io.ReadFull(app, make([]byte, 2+len(socksReplyConnected)))
		app.Write(hello[:3])
		time.Sleep(20 * time.Millisecond)
		app.Write(hello[3:40])
		time.Sleep(20 * time.Millisecond)
		app.Write(hello[40:])
	}()
	prefix, _, _, err := answerSocks(rabbit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(prefix, []byte("split.example.com")) || !bytes.HasSuffix(prefix, hello) {
		t.Fatalf("prefix %q", prefix)
	}
}

func TestSocksConnectUntouched(t *testing.T) {
	setMode(t, true, true)
	v4 := []byte{5, 1, 0, 1, 1, 2, 3, 4, 1, 187}
	hello := clientHello(t, "a.example.com")
	for _, c := range []struct {
		name       string
		sent, want []byte
	}{
		{"IPv4 is left alone", append(append([]byte{5, 1, 0}, v4...), hello...), append([]byte{5, 1, 0}, v4...)},
		{"IPv6, server speaks first", append([]byte{5, 1, 0}, v6Req...), append([]byte{5, 1, 0}, v6Req...)},
		{"IPv6, no name in the data", append(append([]byte{5, 1, 0}, v6Req...), "SSH-2.0-x\r\n"...), append(append([]byte{5, 1, 0}, v6Req...), "SSH-2.0-x\r\n"...)},
	} {
		t0 := time.Now()
		prefix, _, _ := talk(t, c.sent, 2+len(socksReplyConnected))
		if !bytes.Equal(prefix, c.want) {
			t.Errorf("%s: prefix %v\nwant %v", c.name, prefix, c.want)
		}
		if d := time.Since(t0); d > sniffWait+200*time.Millisecond {
			t.Errorf("%s: took %v", c.name, d)
		}
	}
	socksSniffV6 = false
	defer func() { socksSniffV6 = true }()
	prefix, _, _ := talk(t, append(append([]byte{5, 1, 0}, v6Req...), hello...), 2+len(socksReplyConnected))
	if !bytes.Equal(prefix, append([]byte{5, 1, 0}, v6Req...)) {
		t.Errorf("RABBIT_SOCKS_SNIFF_V6=0: prefix %v", prefix)
	}
}
