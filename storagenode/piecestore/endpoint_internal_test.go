// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/drpc/drpcctx"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcwire"
)

func TestReportedFreeDisk(t *testing.T) {
	tests := []struct {
		name      string
		target    memory.Size
		available memory.Size
		expected  memory.Size
		notify    bool
	}{
		{name: "disabled", available: 20 * memory.GB, expected: 20 * memory.GB},
		{name: "target below report threshold", target: 3 * memory.GB, available: 20 * memory.GB, expected: 20 * memory.GB},
		{name: "target at report threshold", target: 5 * memory.GB, available: 20 * memory.GB, expected: 20 * memory.GB},
		{name: "above target", target: 20 * memory.GB, available: 100 * memory.GB, expected: 85 * memory.GB},
		{name: "at target", target: 20 * memory.GB, available: 20 * memory.GB, expected: 5 * memory.GB},
		{name: "below target", target: 20 * memory.GB, available: 20*memory.GB - 1, expected: 5*memory.GB - 1, notify: true},
		{name: "clamped at zero", target: 20 * memory.GB, available: 10 * memory.GB, expected: 0, notify: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{
				ReportCapacityThreshold: 5 * memory.GB,
				TargetFreeSpace:         test.target,
			}
			require.Equal(t, test.expected.Int64(), config.reportedFreeDisk(test.available.Int64()))
			require.Equal(t, test.notify, config.shouldNotifyLowDisk(test.available.Int64()))
		})
	}
}

func TestIsCongested(t *testing.T) {
	const congestionThreshold = 0.8

	tests := []struct {
		name                  string
		maxConcurrentRequests int
		liveRequests          int32
		want                  bool
	}{
		// 0 means unlimited, so it must never report congestion — otherwise the
		// slow-upload check (skipped while congested) is silently disabled.
		{name: "unlimited is never congested", maxConcurrentRequests: 0, liveRequests: 1, want: false},
		{name: "unlimited stays uncongested under heavy load", maxConcurrentRequests: 0, liveRequests: 100000, want: false},
		{name: "negative is treated as unlimited", maxConcurrentRequests: -1, liveRequests: 100, want: false},

		// With a cap, congestion is strictly above the threshold (80% of 100).
		{name: "below threshold", maxConcurrentRequests: 100, liveRequests: 50, want: false},
		{name: "at threshold is not congested", maxConcurrentRequests: 100, liveRequests: 80, want: false},
		{name: "above threshold is congested", maxConcurrentRequests: 100, liveRequests: 81, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := &Endpoint{
				config: Config{
					MaxConcurrentRequests:             tt.maxConcurrentRequests,
					MinUploadSpeedCongestionThreshold: congestionThreshold,
				},
			}
			endpoint.liveRequests.Store(tt.liveRequests)
			require.Equal(t, tt.want, endpoint.isCongested())
		})
	}
}

// TestWithTimeoutUnblocksStuckSend checks that withTimeout unblocks a send which
// is stuck writing to a peer that stopped reading (e.g. full TCP buffers).
func TestWithTimeoutUnblocksStuckSend(t *testing.T) {
	ctx := testcontext.New(t)

	// net.Pipe has no buffering: writes block until the other side reads.
	serverConn, clientConn := net.Pipe()
	defer ctx.Check(clientConn.Close)

	manager := drpcmanager.New(serverConn)
	defer ctx.Check(manager.Close)

	// The client sends an invoke, and then never reads anything.
	ctx.Go(func() error {
		wr := drpcwire.NewWriter(clientConn, 0)
		if err := wr.WritePacket(drpcwire.Packet{
			ID:   drpcwire.ID{Stream: 1, Message: 1},
			Kind: drpcwire.KindInvoke,
			Data: []byte("/test/Method"),
		}); err != nil {
			return err
		}
		return wr.Flush()
	})

	stream, _, err := manager.NewServerStream(ctx)
	require.NoError(t, err)

	cancelStream, ok := getCanceler(stream)
	require.True(t, ok)

	streamCtx := drpcctx.WithTransport(ctx, serverConn)

	done := make(chan error, 1)
	go func() {
		_, err := withTimeout(streamCtx, 100*time.Millisecond, cancelStream,
			func(ctx context.Context) (_ any, err error) {
				if err := stream.RawWrite(drpcwire.KindMessage, make([]byte, memory.MiB)); err != nil {
					return nil, err
				}
				return nil, stream.RawFlush()
			})
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		// unblock the stuck goroutine so the test can finish
		_ = serverConn.Close()
		t.Fatal("withTimeout did not unblock the stuck send")
	}
}
