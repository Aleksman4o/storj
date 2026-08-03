// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package contact

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"go.uber.org/zap"

	"storj.io/common/pb"
	"storj.io/common/rpc"
	"storj.io/common/storj"
)

func TestAmnestyClientPreservesReason(t *testing.T) {
	client := NewAmnestyClient(zap.NewNop(), rpc.Dialer{}, nil)
	client.flushInterval = time.Hour

	var satellite storj.NodeID
	satellite[0] = 1
	var hashMismatch storj.PieceID
	hashMismatch[0] = 2
	var readFailure storj.PieceID
	readFailure[0] = 3

	assert.NoError(t, client.ReportBadPiece(t.Context(), satellite, hashMismatch))
	assert.NoError(t, client.ReportBadPieceWithReason(
		t.Context(),
		satellite,
		readFailure,
		pb.LostPieceReason_READ_FAILURE,
	))

	client.mu.Lock()
	batch := client.batches[satellite]
	assert.NotNil(t, batch)
	assert.Equal(t, len(batch.pieces), 2)
	assert.Equal(t, batch.pieces[0].PieceId, hashMismatch)
	assert.Equal(t, batch.pieces[0].Reason, pb.LostPieceReason_HASH_MISMATCH)
	assert.Equal(t, batch.pieces[1].PieceId, readFailure)
	assert.Equal(t, batch.pieces[1].Reason, pb.LostPieceReason_READ_FAILURE)
	if batch.timer != nil {
		batch.timer.Stop()
	}
	client.shutdown = true
	client.mu.Unlock()
}
