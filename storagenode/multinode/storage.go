// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package multinode

import (
	"context"
	"time"

	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/rpc/rpcstatus"
	"storj.io/storj/private/multinodepb"
	"storj.io/storj/storagenode/apikeys"
	"storj.io/storj/storagenode/monitor"
	"storj.io/storj/storagenode/storageusage"
	"storj.io/storj/storagenode/trust"
)

var _ multinodepb.DRPCStorageServer = (*StorageEndpoint)(nil)

// StorageEndpoint implements multinode storage endpoint.
//
// architecture: Endpoint
type StorageEndpoint struct {
	multinodepb.DRPCStorageUnimplementedServer

	log     *zap.Logger
	apiKeys *apikeys.Service
	monitor *monitor.Service
	usage   storageusage.DB
	trust   trust.TrustedSatelliteSource
}

// NewStorageEndpoint creates new multinode storage endpoint.
func NewStorageEndpoint(log *zap.Logger, apiKeys *apikeys.Service, monitor *monitor.Service, usage storageusage.DB, trust trust.TrustedSatelliteSource) *StorageEndpoint {
	return &StorageEndpoint{
		log:     log,
		apiKeys: apiKeys,
		monitor: monitor,
		usage:   usage,
		trust:   trust,
	}
}

// DiskSpace returns disk space state.
func (storage *StorageEndpoint) DiskSpace(ctx context.Context, req *multinodepb.DiskSpaceRequest) (_ *multinodepb.DiskSpaceResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	if err = authenticate(ctx, storage.apiKeys, req.GetHeader()); err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Unauthenticated, err)
	}

	diskSpace, err := storage.monitor.DiskSpace(ctx)
	if err != nil {
		storage.log.Error("disk space internal error", zap.Error(err))
		return nil, rpcstatus.Wrap(rpcstatus.Internal, err)
	}

	return &multinodepb.DiskSpaceResponse{
		Allocated:       diskSpace.Allocated,
		Used:            diskSpace.Used,
		UsedPieces:      diskSpace.UsedForPieces,
		UsedReclaimable: diskSpace.UsedReclaimable,
		UsedTrash:       diskSpace.UsedForTrash,
		Free:            diskSpace.Free,
		Available:       diskSpace.Available,
		Overused:        diskSpace.Overused,
	}, nil
}

// Usage returns daily storage usage for a given interval.
func (storage *StorageEndpoint) Usage(ctx context.Context, req *multinodepb.StorageUsageRequest) (_ *multinodepb.StorageUsageResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	if err = authenticate(ctx, storage.apiKeys, req.GetHeader()); err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Unauthenticated, err)
	}

	from := req.GetFrom()
	if from.IsZero() {
		return nil, rpcstatus.Wrap(rpcstatus.InvalidArgument, errs.New("from timestamp is not provided"))
	}
	to := req.GetTo()
	if to.IsZero() {
		return nil, rpcstatus.Wrap(rpcstatus.InvalidArgument, errs.New("to timestamp is not provided"))
	}

	through := displayThrough(to)
	satellites := storage.trust.GetSatellites(ctx)
	normalizedBySatellite := make([][]storageusage.Stamp, 0, len(satellites))
	for _, satelliteID := range satellites {
		rawStamps, err := storage.usage.GetDailyRawForNormalization(ctx, satelliteID, from, through)
		if err != nil {
			return nil, rpcstatus.Wrap(rpcstatus.Internal, err)
		}
		normalizedBySatellite = append(normalizedBySatellite, storageusage.NormalizeForDisplay(rawStamps, from, through))
	}
	stamps := storageusage.CombineForDisplay(normalizedBySatellite...)

	summary, _, err := storage.usage.Summary(ctx, from, to)
	if err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Internal, err)
	}
	averageUsageInBytes := storageusage.DisplayAverage(stamps)

	var usage []*multinodepb.StorageUsage
	for _, stamp := range stamps {
		usage = append(usage, &multinodepb.StorageUsage{
			AtRestTotal:      stamp.AtRestTotal,
			AtRestTotalBytes: stamp.AtRestTotalBytes,
			IntervalStart:    stamp.IntervalStart,
		})
	}

	return &multinodepb.StorageUsageResponse{
		StorageUsage:      usage,
		Summary:           summary,
		AverageUsageBytes: averageUsageInBytes,
	}, nil
}

// UsageSatellite returns daily storage usage for a given interval and satellite.
func (storage *StorageEndpoint) UsageSatellite(ctx context.Context, req *multinodepb.StorageUsageSatelliteRequest) (_ *multinodepb.StorageUsageSatelliteResponse, err error) {
	defer mon.Task()(&ctx)(&err)

	if err = authenticate(ctx, storage.apiKeys, req.GetHeader()); err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Unauthenticated, err)
	}

	if req.SatelliteId.IsZero() {
		return nil, rpcstatus.Wrap(rpcstatus.InvalidArgument, errs.New("satellite id is not provided"))
	}

	from := req.GetFrom()
	if from.IsZero() {
		return nil, rpcstatus.Wrap(rpcstatus.InvalidArgument, errs.New("from timestamp is not provided"))
	}
	to := req.GetTo()
	if to.IsZero() {
		return nil, rpcstatus.Wrap(rpcstatus.InvalidArgument, errs.New("to timestamp is not provided"))
	}

	through := displayThrough(to)
	rawStamps, err := storage.usage.GetDailyRawForNormalization(ctx, req.SatelliteId, from, through)
	if err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Internal, err)
	}
	stamps := storageusage.NormalizeForDisplay(rawStamps, from, through)

	summary, _, err := storage.usage.SatelliteSummary(ctx, req.SatelliteId, from, to)
	if err != nil {
		return nil, rpcstatus.Wrap(rpcstatus.Internal, err)
	}
	averageUsageInBytes := storageusage.DisplayAverage(stamps)

	var usage []*multinodepb.StorageUsage
	for _, stamp := range stamps {
		usage = append(usage, &multinodepb.StorageUsage{
			AtRestTotal:      stamp.AtRestTotal,
			AtRestTotalBytes: stamp.AtRestTotalBytes,
			IntervalStart:    stamp.IntervalStart,
		})
	}

	return &multinodepb.StorageUsageSatelliteResponse{
		StorageUsage:      usage,
		Summary:           summary,
		AverageUsageBytes: averageUsageInBytes,
	}, nil
}

func displayThrough(to time.Time) time.Time {
	now := time.Now().UTC()
	if to.Before(now) {
		return to
	}
	return now
}
