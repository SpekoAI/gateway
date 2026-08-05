//go:build unix

package gateway

import "os"

func setSocketPermissions(socketPath string) error {
	return os.Chmod(socketPath, 0o600)
}
