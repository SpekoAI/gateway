//go:build !unix

package gateway

func setSocketPermissions(string) error { return nil }
