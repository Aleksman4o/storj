// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"storj.io/common/memory"
	"storj.io/common/rpc"
	"storj.io/storj/storagenode/contact"
)

type staticSpaceReport struct {
	space DiskSpace
}

func (report staticSpaceReport) DiskSpace(context.Context) (DiskSpace, error) {
	return report.space, nil
}

func TestReportedCapacityDoesNotChangeLocalAvailableSpace(t *testing.T) {
	log := zaptest.NewLogger(t)
	contactService := contact.NewService(log, rpc.Dialer{}, contact.NodeInfo{}, nil, nil, nil)
	report := staticSpaceReport{space: DiskSpace{Available: (20 * memory.GB).Int64()}}
	service := NewService(
		log,
		nil,
		contactService,
		report,
		Config{},
		(15 * memory.GB).Int64(),
		time.Second,
	)

	require.NoError(t, service.updateNodeInformation(context.Background()))
	require.Equal(t, (5 * memory.GB).Int64(), contactService.Local().Capacity.FreeDisk)

	local, err := service.DiskSpace(context.Background())
	require.NoError(t, err)
	require.Equal(t, (20 * memory.GB).Int64(), local.Available)
}
