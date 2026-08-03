// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build unix

package platform

import (
	"errors"
	"syscall"
)

// IsSalvageableReadError reports whether err unambiguously describes a local
// failure to read data from a file. Callers must still verify that writes to
// the same storage are healthy before discarding data.
func IsSalvageableReadError(err error) bool {
	return errors.Is(err, syscall.EIO)
}
