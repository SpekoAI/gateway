//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ListenNativeUnix retains a process lock through listener cleanup. Only a
// socket recorded by the previous native owner can be reclaimed. ECONNREFUSED is
// insufficient evidence: another process may be between bind and listen.
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
		var prior nativeSocketOwner
		decoder := json.NewDecoder(io.LimitReader(lock, 4096))
		decoder.DisallowUnknownFields()
		// This record was written only after binding under this same retained
		// flock. A native owner releases the lock after closing its listener;
		// process death releases both. No PID liveness probe is needed (PIDs
		// may be reused, including PID 1 across container restarts).
		if decoder.Decode(&prior) != nil || prior.Version != 1 || prior.Device != uint64(stat.Dev) || prior.Inode != uint64(stat.Ino) || prior.ModifiedAt != entry.ModTime().UnixNano() {
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
	entry, err = os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		return refuse()
	}
	socketStat, ok := entry.Sys().(*syscall.Stat_t)
	if !ok || lock.Truncate(0) != nil {
		_ = listener.Close()
		return refuse()
	}
	if _, err = lock.Seek(0, io.SeekStart); err == nil {
		err = json.NewEncoder(lock).Encode(nativeSocketOwner{Version: 1, Device: uint64(socketStat.Dev), Inode: uint64(socketStat.Ino), ModifiedAt: entry.ModTime().UnixNano()})
	}
	if err != nil || lock.Sync() != nil {
		_ = listener.Close()
		return refuse()
	}
	keep = true
	return &nativeUnixListener{Listener: listener, lock: lock}, nil
}

type nativeSocketOwner struct {
	Version    int    `json:"version"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	ModifiedAt int64  `json:"modified_at"`
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
