package connection_pool

import (
	"testing"
	"time"
)

func TestDatagramOrder(t *testing.T) {
	dgramHold = 50 * time.Millisecond
	got := make(chan byte, 16)
	o := newDatagramOrder(func(f []byte) { got <- f[0] })
	for _, seq := range []uint32{10, 12, 13, 11, 15} { // 14 never comes
		o.push(seq, []byte{byte(seq)})
	}
	var order []byte
	for len(order) < 4 {
		order = append(order, <-got)
	}
	if string(order) != string([]byte{10, 11, 12, 13}) {
		t.Fatalf("in order: %v", order)
	}
	select {
	case b := <-got:
		t.Fatalf("15 delivered before the hold: %d", b)
	case <-time.After(20 * time.Millisecond):
	}
	if b := <-got; b != 15 {
		t.Fatalf("after hold: %d", b)
	}
	o.push(14, []byte{14}) // late after its gap was skipped
	if b := <-got; b != 14 {
		t.Fatalf("late: %d", b)
	}
}
