//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package gateway

import (
	"errors"
	"net"
)

func ListenNativeUnix(string) (net.Listener, error) {
	return nil, errors.New("gateway: native socket locking is unsupported on this platform")
}
