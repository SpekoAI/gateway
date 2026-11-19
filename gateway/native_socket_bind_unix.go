//go:build linux || freebsd || openbsd || netbsd || dragonfly

package gateway

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// Verify a directory-descriptor alias before using it to keep sockaddr short.
// The caller retains this directory descriptor through socket publication.
func listenNativeStagedSocket(staging string, directory *os.File) (net.Listener, error) {
	entry, err := directory.Stat()
	if err == nil {
		for _, root := range []string{"/proc/self/fd", "/dev/fd"} {
			alias := filepath.Join(root, strconv.FormatUint(uint64(directory.Fd()), 10))
			if actual, err := os.Stat(alias + "/."); err == nil && os.SameFile(entry, actual) {
				return ListenUnix(filepath.Join(alias, "s"))
			}
		}
	}
	return ListenUnix(filepath.Join(staging, "s"))
}
