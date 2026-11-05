package gateway

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/unix"
)

// Darwin's /dev/fd entries cannot be traversed as directory aliases. Bind a
// short relative name with a thread-local cwd instead. No process cwd changes,
// and the socket stays in the same private staging directory for atomic link.
func listenNativeStagedSocket(staging string, directory *os.File) (net.Listener, error) {
	type result struct {
		listener *net.UnixListener
		err      error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		if err := unix.PthreadFchdir(int(directory.Fd())); err != nil {
			runtime.UnlockOSThread()
			done <- result{err: err}
			return
		}
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: "s", Net: "unix"})
		if listener != nil {
			listener.SetUnlinkOnClose(false)
		}
		if resetErr := unix.PthreadFchdir(-1); resetErr != nil {
			if listener != nil {
				_ = listener.Close()
			}
			// Exiting this dedicated goroutine while still locked makes Go
			// retire the OS thread, so a failed reset cannot leak its cwd.
			done <- result{err: errors.New("gateway: native socket thread cwd reset failed")}
			return
		}
		runtime.UnlockOSThread()
		done <- result{listener: listener, err: err}
	}()
	bound := <-done
	if bound.err != nil {
		return nil, bound.err
	}
	if err := setSocketMode(filepath.Join(staging, "s")); err != nil {
		_ = bound.listener.Close()
		return nil, err
	}
	return bound.listener, nil
}
