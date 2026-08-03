// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package console

import (
	"context"

	"storj.io/common/storj"
	"storj.io/storj/storagenode/hashstore"
)

// CompactionTotals contains compaction counters accumulated since the process started.
type CompactionTotals struct {
	FinishedAttempts   uint64 `json:"finishedAttempts"`
	FailedAttempts     uint64 `json:"failedAttempts"`
	LogsRewritten      uint64 `json:"logsRewritten"`
	DataRewrittenBytes int64  `json:"dataRewrittenBytes"`
	DataReclaimedBytes int64  `json:"dataReclaimedBytes"`
}

// CompactionSalvage contains salvage counters accumulated since the process started.
type CompactionSalvage struct {
	SuccessfulRounds uint64 `json:"successfulRounds"`
	LostPieces       uint64 `json:"lostPieces"`
	LostBytes        uint64 `json:"lostBytes"`
	AffectedLogs     uint64 `json:"affectedLogs"`
	AbortedRounds    uint64 `json:"abortedRounds"`
}

// CompactionProgress contains progress for the current atomic compaction round.
type CompactionProgress struct {
	ElapsedSeconds   float64 `json:"elapsedSeconds"`
	RemainingSeconds float64 `json:"remainingSeconds"`
	ProcessedRecords uint64  `json:"processedRecords"`
	TotalRecords     uint64  `json:"totalRecords"`
}

// SatelliteCompaction contains compaction statistics for one satellite database.
type SatelliteCompaction struct {
	SatelliteID      storj.NodeID        `json:"satelliteID"`
	Compacting       bool                `json:"compacting"`
	CurrentRound     *CompactionProgress `json:"currentRound,omitempty"`
	ReclaimableBytes int64               `json:"reclaimableBytes"`
	RuntimeTotals    CompactionTotals    `json:"runtimeTotals"`
	Salvage          CompactionSalvage   `json:"salvage"`
}

// CompactionInfo contains current and runtime compaction statistics for the node.
type CompactionInfo struct {
	Compacting       bool                  `json:"compacting"`
	SalvageEnabled   bool                  `json:"salvageEnabled"`
	ReclaimableBytes int64                 `json:"reclaimableBytes"`
	RuntimeTotals    CompactionTotals      `json:"runtimeTotals"`
	Salvage          CompactionSalvage     `json:"salvage"`
	Satellites       []SatelliteCompaction `json:"satellites"`
}

// GetCompactionData returns current and runtime compaction statistics.
func (s *Service) GetCompactionData(ctx context.Context) (_ *CompactionInfo, err error) {
	defer mon.Task()(&ctx)(&err)

	stats := s.hashStore.CompactionStats()
	data := &CompactionInfo{
		SalvageEnabled: s.hashStore.SalvageEnabled(),
		Satellites:     make([]SatelliteCompaction, 0, len(stats)),
	}

	for _, stat := range stats {
		satellite := SatelliteCompaction{
			SatelliteID:      stat.SatelliteID,
			Compacting:       stat.Database.Compacting,
			ReclaimableBytes: stat.Database.DataReclaimable.Int64(),
			RuntimeTotals:    compactionTotals(stat.Database),
			Salvage:          compactionSalvage(stat.Database.Salvage),
		}
		for _, store := range stat.Stores {
			if store.Compacting {
				satellite.CurrentRound = &CompactionProgress{
					ElapsedSeconds:   store.Compaction.Elapsed,
					RemainingSeconds: store.Compaction.Remaining,
					ProcessedRecords: store.Compaction.ProcessedRecords,
					TotalRecords:     store.Compaction.TotalRecords,
				}
				break
			}
		}

		data.Compacting = data.Compacting || satellite.Compacting
		data.ReclaimableBytes += satellite.ReclaimableBytes
		data.RuntimeTotals.add(satellite.RuntimeTotals)
		data.Salvage.add(satellite.Salvage)
		data.Satellites = append(data.Satellites, satellite)
	}

	return data, nil
}

func compactionTotals(stats hashstore.DBStats) CompactionTotals {
	return CompactionTotals{
		FinishedAttempts:   stats.Compactions,
		FailedAttempts:     stats.CompactionFailures,
		LogsRewritten:      stats.LogsRewritten,
		DataRewrittenBytes: stats.DataRewritten.Int64(),
		DataReclaimedBytes: stats.DataReclaimed.Int64(),
	}
}

func (totals *CompactionTotals) add(other CompactionTotals) {
	totals.FinishedAttempts += other.FinishedAttempts
	totals.FailedAttempts += other.FailedAttempts
	totals.LogsRewritten += other.LogsRewritten
	totals.DataRewrittenBytes += other.DataRewrittenBytes
	totals.DataReclaimedBytes += other.DataReclaimedBytes
}

func compactionSalvage(stats hashstore.SalvageStats) CompactionSalvage {
	return CompactionSalvage{
		SuccessfulRounds: stats.SuccessfulRounds,
		LostPieces:       stats.LostPieces,
		LostBytes:        stats.LostBytes,
		AffectedLogs:     stats.AffectedLogs,
		AbortedRounds:    stats.AbortedRounds,
	}
}

func (salvage *CompactionSalvage) add(other CompactionSalvage) {
	salvage.SuccessfulRounds += other.SuccessfulRounds
	salvage.LostPieces += other.LostPieces
	salvage.LostBytes += other.LostBytes
	salvage.AffectedLogs += other.AffectedLogs
	salvage.AbortedRounds += other.AbortedRounds
}
