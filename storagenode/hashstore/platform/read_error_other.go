// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build !unix && !windows

package platform

// IsSalvageableReadError reports whether err is eligible for classification as
// a local file read failure. Unknown platforms fail closed.
func IsSalvageableReadError(error) bool {
	return false
}
