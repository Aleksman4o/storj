// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build windows

package platform

import (
	"errors"

	"golang.org/x/sys/windows"
)

// IsSalvageableReadError reports whether err is eligible for classification as
// a local file read failure. Callers must still confirm the error with a direct
// retry and verify that writes to the same storage are healthy before discarding
// data.
func IsSalvageableReadError(err error) bool {
	return errors.Is(err, windows.ERROR_CRC) ||
		errors.Is(err, windows.ERROR_SECTOR_NOT_FOUND) ||
		errors.Is(err, windows.ERROR_READ_FAULT)
}
