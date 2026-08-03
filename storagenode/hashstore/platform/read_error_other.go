// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build !unix && !windows

package platform

// IsSalvageableReadError reports whether err unambiguously describes a local
// failure to read data from a file. Unknown platforms fail closed.
func IsSalvageableReadError(error) bool {
	return false
}
