//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package gateway_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/SpekoAI/gateway/gateway"
)

func TestNativeSocketReclaimsStaleEntryAndRetainsOneListenerOwner(t *testing.T) {
	directory, err := os.MkdirTemp("", "native-socket-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "runtime.sock")
	old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	old.SetUnlinkOnClose(false)
	_ = old.Close()
	first, err := gateway.ListenNativeUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if unexpected, err := gateway.ListenNativeUnix(path); err == nil {
		_ = unexpected.Close()
		t.Fatal("a second owner replaced a live native socket")
	}
	probe, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal("the first native listener was removed")
	}
	_ = probe.Close()
	_ = first.Close()
	second, err := gateway.ListenNativeUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = first.Close() // Repeated cleanup must not unlink the next owner's socket.
	probe, err = net.Dial("unix", path)
	if err != nil {
		t.Fatal("repeated cleanup removed the next listener")
	}
	_ = probe.Close()
}

func TestNativeSocketRefusesLiveUncoordinatedListenerAndUnrelatedEntries(t *testing.T) {
	for _, kind := range []string{"live", "file", "socket-symlink", "lock-symlink"} {
		t.Run(kind, func(t *testing.T) {
			directory, err := os.MkdirTemp("", "native-socket-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(directory)
			path := filepath.Join(directory, "runtime.sock")
			switch kind {
			case "live":
				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			case "file":
				if err := os.WriteFile(path, []byte("owned-data"), 0600); err != nil {
					t.Fatal(err)
				}
			default:
				target := filepath.Join(directory, "data")
				if err := os.WriteFile(target, []byte("owned-data"), 0600); err != nil {
					t.Fatal(err)
				}
				link := path
				if kind == "lock-symlink" {
					link += ".lock"
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			if listener, err := gateway.ListenNativeUnix(path); err == nil {
				_ = listener.Close()
				t.Fatal("unrelated or live entry was replaced")
			}
			if kind != "lock-symlink" {
				if _, err := os.Lstat(path); err != nil {
					t.Fatal("existing entry was removed")
				}
			}
		})
	}
}
