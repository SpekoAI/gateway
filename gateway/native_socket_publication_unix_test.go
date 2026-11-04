//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package gateway

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNativeSocketPreservesLongValidStablePaths(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux sockaddr_un boundary")
	}
	for _, length := range []int{100, 107} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			base, err := os.MkdirTemp("", "native-long-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(base)
			parent := filepath.Join(base, strings.Repeat("d", length-len(base)-3))
			if err = os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(parent, "s")
			if len(path) != length {
				t.Fatal("incorrect long socket fixture", len(path))
			}
			direct, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal("stable path should bind without staging", err)
			}
			_ = direct.Close()
			listener, err := ListenNativeUnix(path)
			if err != nil {
				t.Fatal("native staging broke a valid stable path", err)
			}
			defer listener.Close()
			connection, err := net.Dial("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			_ = connection.Close()
			if listener.Addr().String() != path {
				t.Fatal("listener exposed its private binding path")
			}
		})
	}
}

func TestNativeSocketCrashBeforePublicationNeverBlocksStablePath(t *testing.T) {
	if path := os.Getenv("SPEKO_NATIVE_SOCKET_PUBLICATION_CHILD"); path != "" {
		_, err := listenNativeUnix(path, func(lock *os.File, owner nativeSocketOwner) error {
			if os.Getenv("SPEKO_NATIVE_SOCKET_PUBLICATION_PHASE") == "truncated" {
				if err := lock.Truncate(0); err != nil {
					return err
				}
			} else if err := persistNativeSocketOwner(lock, owner); err != nil {
				return err
			}
			if err := os.WriteFile(path+".ready", []byte("bound"), 0600); err != nil {
				return err
			}
			for {
				time.Sleep(time.Second)
			}
		})
		t.Fatal("child did not stop at ownership persistence", err)
	}
	for _, phase := range []string{"truncated", "recorded"} {
		t.Run(phase, func(t *testing.T) {
			directory, err := os.MkdirTemp("", "native-publication-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(directory)
			path := filepath.Join(directory, "runtime.sock")
			child := exec.Command(os.Args[0], "-test.run=^TestNativeSocketCrashBeforePublicationNeverBlocksStablePath$")
			child.Env = append(os.Environ(), "SPEKO_NATIVE_SOCKET_PUBLICATION_CHILD="+path, "SPEKO_NATIVE_SOCKET_PUBLICATION_PHASE="+phase)
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
					t.Fatal("child did not reach bound socket persistence")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if _, err = os.Lstat(path); !os.IsNotExist(err) {
				t.Fatal("stable socket exposed before durable ownership", err)
			}
			if err = child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Wait()
			listener, err := ListenNativeUnix(path)
			if err != nil {
				t.Fatal("crash before publication blocked restart", err)
			}
			defer listener.Close()
			connection, err := net.Dial("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			_ = connection.Close()
		})
	}
}

func TestNativeSocketPublicationDoesNotReplaceConcurrentUncoordinatedBind(t *testing.T) {
	directory, err := os.MkdirTemp("", "native-publication-race-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "runtime.sock")
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	listener, err := listenNativeUnix(path, func(lock *os.File, owner nativeSocketOwner) error {
		if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
			return err
		}
		return persistNativeSocketOwner(lock, owner)
	})
	if err == nil {
		_ = listener.Close()
		t.Fatal("publication replaced an uncoordinated bind")
	}
	if err = syscall.Listen(fd, 1); err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal("uncoordinated owner lost its path", err)
	}
	_ = connection.Close()
}
