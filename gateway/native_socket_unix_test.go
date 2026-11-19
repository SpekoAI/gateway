//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package gateway_test

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/gateway"
)

func TestNativeSocketRetainsOneListenerOwner(t *testing.T) {
	directory, err := os.MkdirTemp("", "native-socket-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "runtime.sock")
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
	for _, kind := range []string{"live", "bound", "stale-unrecorded", "file", "socket-symlink", "lock-symlink"} {
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
			case "bound":
				fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer syscall.Close(fd)
				if err = syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := syscall.Listen(fd, 1); err != nil {
						t.Fatal(err)
					}
					probe, err := net.Dial("unix", path)
					if err != nil {
						t.Fatal("bound owner's socket was removed before listen", err)
					}
					_ = probe.Close()
				}()
			case "stale-unrecorded":
				old, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				old.SetUnlinkOnClose(false)
				_ = old.Close()
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

func TestNativeSocketRecoversOnlyItsRecordedDeadOwner(t *testing.T) {
	if path := os.Getenv("SPEKO_NATIVE_SOCKET_TEST_CHILD"); path != "" {
		listener, err := gateway.ListenNativeUnix(path)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		if err = os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Second)
		}
	}
	directory, err := os.MkdirTemp("", "native-socket-crash-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "runtime.sock")
	child := exec.Command(os.Args[0], "-test.run=^TestNativeSocketRecoversOnlyItsRecordedDeadOwner$")
	child.Env = append(os.Environ(), "SPEKO_NATIVE_SOCKET_TEST_CHILD="+path)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err = os.Stat(path + ".ready"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child listener did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	listener, err := gateway.ListenNativeUnix(path)
	if err != nil {
		t.Fatal("recorded dead native socket was not recovered", err)
	}
	defer listener.Close()
	probe, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = probe.Close()
}
