//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package gateway

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// ListenNativeUnix retains a process lock through listener cleanup. Only a
// same-owner stale socket inside an owner-only directory can be reclaimed.
// The lock file remains in place so concurrent restarts share one lock inode.
func ListenNativeUnix(socketPath string) (net.Listener, error) {
	refuse := func() (net.Listener, error) {
		return nil, errors.New("gateway: native socket ownership or listener is unavailable")
	}
	if !filepath.IsAbs(socketPath) {
		return refuse()
	}
	dir, err := os.Lstat(filepath.Dir(socketPath))
	if err != nil || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return refuse()
	}
	owner, ok := dir.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		return refuse()
	}
	fd, err := syscall.Open(socketPath+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return refuse()
	}
	lock := os.NewFile(uintptr(fd), "native-socket-lock")
	keep := false
	defer func() {
		if !keep {
			_ = lock.Close()
		}
	}()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return refuse()
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) || stat.Nlink != 1 || syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return refuse()
	}
	entry, err := os.Lstat(socketPath)
	if err == nil {
		stat, ok := entry.Sys().(*syscall.Stat_t)
		if !ok || entry.Mode()&os.ModeSocket == 0 || stat.Uid != uint32(os.Getuid()) {
			return refuse()
		}
		conn, dialErr := net.DialTimeout("unix", socketPath, time.Second)
		if dialErr == nil {
			_ = conn.Close()
			return refuse()
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return refuse()
		}
		current, statErr := os.Lstat(socketPath)
		if statErr != nil || !os.SameFile(entry, current) {
			return refuse()
		}
		if os.Remove(socketPath) != nil {
			return refuse()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return refuse()
	}
	listener, err := ListenUnix(socketPath)
	if err != nil {
		return nil, err
	}
	keep = true
	return &nativeUnixListener{Listener: listener, lock: lock}, nil
}

type nativeUnixListener struct {
	net.Listener
	lock *os.File
	once sync.Once
	err  error
}

func (l *nativeUnixListener) Close() error {
	l.once.Do(func() { l.err = l.Listener.Close(); _ = l.lock.Close() })
	return l.err
}
