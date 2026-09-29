package connection_pool

import (
	"bytes"
	"net"
	"testing"
)

func TestDatagramDedup(t *testing.T) {
	s := newDatagramSession(1, nil)
	for _, c := range []struct {
		seq  uint32
		want bool
	}{{0, true}, {1, true}, {0, false}, {3, true}, {2, true}, {3, false}, {1, false}} {
		if s.fresh(c.seq) != c.want {
			t.Fatalf("seq %d: want fresh=%v", c.seq, c.want)
		}
	}
	s.fresh(50000) // a tunnel lagging 50000 datagrams behind: its copies still count
	if !s.fresh(2 + 4096) {
		t.Fatal("an unseen datagram within the window must be delivered")
	}
	s.fresh(100000)
	if s.fresh(2) {
		t.Fatal("a copy older than the window is dropped")
	}
	if !s.fresh(99999) || s.fresh(99999) {
		t.Fatal("window bookkeeping after a jump")
	}
	for i := uint32(100001); i < 300000; i++ {
		if !s.fresh(i) {
			t.Fatalf("fresh seq %d rejected", i)
		}
	}
	if s.fresh(299990) {
		t.Fatal("duplicate after wrap of the bitmap")
	}
}

func TestFrameRoundTrip(t *testing.T) {
	for _, a := range []*net.UDPAddr{{IP: net.ParseIP("8.8.8.8"), Port: 53}, {IP: net.ParseIP("2001:db8::1"), Port: 443}} {
		f := append(encodeAddr(a), 'h', 'i')
		dst, data, err := parseFrame(f)
		if err != nil || dst != a.String() || string(data) != "hi" || FrameLen(f) != len(f)-2 {
			t.Fatalf("%v: dst %q data %q err %v len %d", a, dst, data, err, FrameLen(f))
		}
	}
	f := append([]byte{3, 3, 'a', 'b', 'c', 0, 53}, 'x')
	if dst, data, err := parseFrame(f); err != nil || dst != "abc:53" || !bytes.Equal(data, []byte("x")) {
		t.Fatalf("domain frame: %q %q %v", dst, data, err)
	}
	for _, bad := range [][]byte{{}, {1, 1, 2}, {3, 9, 'a'}, {9, 0, 0}} {
		if _, _, err := parseFrame(bad); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}
