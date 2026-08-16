// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package multinode_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/multinodepb"
	"storj.io/storj/storagenode"
	"storj.io/storj/storagenode/apikeys"
	"storj.io/storj/storagenode/multinode"
	"storj.io/storj/storagenode/storagenodedb/storagenodedbtest"
	"storj.io/storj/storagenode/storageusage"
	"storj.io/storj/storagenode/trust"
)

func TestStorageEndpointNormalizesUsageForDisplay(t *testing.T) {
	storagenodedbtest.Run(t, func(ctx *testcontext.Context, t *testing.T, db storagenode.DB) {
		firstSatellite := testrand.NodeID()
		secondSatellite := testrand.NodeID()
		from := time.Date(2020, time.July, 1, 0, 0, 0, 0, time.UTC)
		through := time.Date(2020, time.July, 4, 12, 0, 0, 0, time.UTC)

		stamps := []storageusage.Stamp{
			{
				SatelliteID:     firstSatellite,
				AtRestTotal:     2400,
				IntervalStart:   from,
				IntervalEndTime: from.Add(12 * time.Hour),
			},
			{
				SatelliteID:   firstSatellite,
				IntervalStart: from.AddDate(0, 0, 1),
			},
			{
				SatelliteID:     firstSatellite,
				AtRestTotal:     4800,
				IntervalStart:   from.AddDate(0, 0, 2),
				IntervalEndTime: from.AddDate(0, 0, 2).Add(12 * time.Hour),
			},
		}
		for day := 0; day < 3; day++ {
			start := from.AddDate(0, 0, day)
			stamps = append(stamps, storageusage.Stamp{
				SatelliteID:     secondSatellite,
				AtRestTotal:     1200,
				IntervalStart:   start,
				IntervalEndTime: start.Add(12 * time.Hour),
			})
		}
		require.NoError(t, db.StorageUsage().Store(ctx, stamps))

		apiKeys := apikeys.NewService(db.APIKeys())
		key, err := apiKeys.Issue(ctx)
		require.NoError(t, err)

		poolConfig := trust.Config{CachePath: ctx.File("trust-cache.json")}
		poolConfig.Sources = append(poolConfig.Sources,
			&trust.StaticURLSource{URL: trust.SatelliteURL{ID: firstSatellite, Host: "first.test", Port: 7777}},
			&trust.StaticURLSource{URL: trust.SatelliteURL{ID: secondSatellite, Host: "second.test", Port: 7777}},
		)
		trustPool, err := trust.NewPool(zaptest.NewLogger(t), trust.Dialer(rpc.Dialer{}), poolConfig, db.Satellites())
		require.NoError(t, err)
		require.NoError(t, trustPool.Refresh(ctx))
		require.ElementsMatch(t, []storj.NodeID{firstSatellite, secondSatellite}, trustPool.GetSatellites(ctx))

		endpoint := multinode.NewStorageEndpoint(zaptest.NewLogger(t), apiKeys, nil, db.StorageUsage(), trustPool)
		header := &multinodepb.RequestHeader{ApiKey: key.Secret[:]}

		satelliteResponse, err := endpoint.UsageSatellite(ctx, &multinodepb.StorageUsageSatelliteRequest{
			Header:      header,
			SatelliteId: firstSatellite,
			From:        from,
			To:          through,
		})
		require.NoError(t, err)
		require.Len(t, satelliteResponse.StorageUsage, 4)
		for _, stamp := range satelliteResponse.StorageUsage {
			require.Equal(t, float64(100), stamp.AtRestTotalBytes)
		}
		require.Equal(t, float64(7200), satelliteResponse.Summary)
		require.Equal(t, float64(100), satelliteResponse.AverageUsageBytes)

		response, err := endpoint.Usage(ctx, &multinodepb.StorageUsageRequest{
			Header: header,
			From:   from,
			To:     through,
		})
		require.NoError(t, err)
		require.Len(t, response.StorageUsage, 4)
		for _, stamp := range response.StorageUsage {
			require.Equal(t, float64(150), stamp.AtRestTotalBytes)
		}
		require.Equal(t, float64(10800), response.Summary)
		require.Equal(t, float64(150), response.AverageUsageBytes)
	})
}
