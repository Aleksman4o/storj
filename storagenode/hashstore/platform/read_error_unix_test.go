// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build unix

package platform

import (
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
)

func TestIsSalvageableReadError(t *testing.T) {
	if !IsSalvageableReadError(&os.PathError{Op: "read", Path: "log", Err: syscall.EIO}) {
		t.Fatal("wrapped EIO must be salvageable")
	}
	if IsSalvageableReadError(io.ErrUnexpectedEOF) {
		t.Fatal("unexpected EOF must be confirmed using the file size")
	}
	if IsSalvageableReadError(errors.New("read failed")) {
		t.Fatal("unknown read errors must fail closed")
	}
}
