package geminirealtime

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type expiryFreedNormalConn struct {
	net.Conn
	calls                                             atomic.Int32
	first, second, third, releaseFirst, releaseSecond chan struct{}
}

func (c *expiryFreedNormalConn) Close() error {
	switch c.calls.Add(1) {
	case 1:
		close(c.first)
		<-c.releaseFirst
		return errors.New("normal close failed before physical disconnect")
	case 2:
		close(c.second)
		<-c.releaseSecond
		return c.Conn.Close()
	default:
		close(c.third)
		return c.Conn.Close()
	}
}

func TestExpiryRetriesNormalSlotCompletedAfterInterrupt(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "released_before_expiry_control"
		if late {
			name = "released_after_expiry"
		}
		t.Run(name, func(t *testing.T) {
			client, peer := net.Pipe()
			defer peer.Close()
			body := &expiryFreedNormalConn{Conn: client, first: make(chan struct{}), second: make(chan struct{}), third: make(chan struct{}), releaseFirst: make(chan struct{}), releaseSecond: make(chan struct{})}
			var firstOnce, secondOnce sync.Once
			releaseFirst := func() { firstOnce.Do(func() { close(body.releaseFirst) }) }
			releaseSecond := func() { secondOnce.Do(func() { close(body.releaseSecond) }) }
			defer releaseFirst()
			defer releaseSecond()
			s := newPhysicalSocket(body, time.Now().Add(250*time.Millisecond))
			expired := make(chan struct{})
			s.bindExpiry(func() { s.Interrupt(); close(expired) })
			go s.Close()
			select {
			case <-body.first:
			case <-time.After(time.Second):
				t.Fatal("normal attempt did not start")
			}
			s.Interrupt()
			select {
			case <-body.second:
			case <-time.After(time.Second):
				t.Fatal("interrupt attempt did not start")
			}
			if !late {
				releaseFirst()
			}
			select {
			case <-expired:
			case <-time.After(time.Second):
				t.Fatal("expiry did not run")
			}
			if late {
				releaseFirst()
			}
			select {
			case <-body.third:
			case <-time.After(400 * time.Millisecond):
				t.Fatalf("expiry did not retry the freed normal close slot while interrupt return remained held; attempts=%d", body.calls.Load())
			}
		})
	}
}
