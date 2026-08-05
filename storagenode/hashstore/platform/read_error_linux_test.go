// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build linux

package platform

import (
	"os"
	"syscall"
	"testing"
)

func TestIsSalvageableReadErrorENODATA(t *testing.T) {
	if !IsSalvageableReadError(&os.PathError{Op: "read", Path: "log", Err: syscall.ENODATA}) {
		t.Fatal("wrapped ENODATA must be salvageable on Linux")
	}
}
