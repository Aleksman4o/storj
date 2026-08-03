// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package hashstore

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"sort"

	"go.uber.org/zap"

	"storj.io/storj/storagenode/hashstore/platform"
)

const (
	salvageMaxAffectedLogs = 3
	salvageMaxReadFailures = 10
	salvageMaxLostRecords  = 10_000
	salvageCopyBufferSize  = 256 * 1024
)

type salvageCause uint8

const (
	salvageCauseTruncated salvageCause = iota + 1
	salvageCauseReadFailure
)

// SalvageStats contains runtime totals for compaction salvage.
type SalvageStats struct {
	SuccessfulRounds uint64
	LostPieces       uint64
	LostBytes        uint64
	AffectedLogs     uint64
	AbortedRounds    uint64
}

// salvageBudget contains data loss committed during one Compact call. It prevents separate atomic
// rounds in the same compaction from collectively exceeding the salvage limits.
type salvageBudget struct {
	maxLostBytes uint64

	lost       map[Key]struct{}
	logs       map[uint64]struct{}
	lostBytes  uint64
	readErrors uint64
}

func newSalvageBudget(maxLostBytes uint64) *salvageBudget {
	return &salvageBudget{
		maxLostBytes: maxLostBytes,
		lost:         make(map[Key]struct{}),
		logs:         make(map[uint64]struct{}),
	}
}

// salvageRound contains data loss that has been proven during one atomic compaction round.
// Nothing in this structure is reported or added to the compaction budget until the new table has
// been committed.
type salvageRound struct {
	budget *salvageBudget

	lost         map[Key]struct{}
	logs         map[uint64]struct{}
	shortLogs    map[uint64]uint64
	lostBytes    uint64
	readErrors   uint64
	short        uint64
	observedLoss bool
}

func (budget *salvageBudget) newRound() *salvageRound {
	return &salvageRound{
		budget:    budget,
		lost:      make(map[Key]struct{}),
		logs:      make(map[uint64]struct{}),
		shortLogs: make(map[uint64]uint64),
	}
}

func (budget *salvageBudget) commit(round *salvageRound) {
	if round.empty() {
		return
	}

	for key := range round.lost {
		budget.lost[key] = struct{}{}
	}
	for logID := range round.logs {
		budget.logs[logID] = struct{}{}
	}
	budget.lostBytes += round.lostBytes
	budget.readErrors += round.readErrors
}

func (round *salvageRound) empty() bool {
	return round == nil || len(round.lost) == 0
}

func (round *salvageRound) has(key Key) bool {
	_, ok := round.lost[key]
	return ok
}

func (round *salvageRound) beyondKnownEOF(rec Record) bool {
	size, ok := round.shortLogs[rec.Log]
	return ok && recordBeyondEOF(rec, size)
}

func (round *salvageRound) rememberEOF(logID, size uint64) {
	round.shortLogs[logID] = size
}

func (round *salvageRound) add(rec Record, cause salvageCause) error {
	if round.has(rec.Key) {
		return nil
	}
	if _, ok := round.budget.lost[rec.Key]; ok {
		return nil
	}
	round.observedLoss = true

	length := uint64(rec.Length) + RecordSize
	if math.MaxUint64-round.budget.lostBytes < round.lostBytes ||
		math.MaxUint64-round.budget.lostBytes-round.lostBytes < length {
		return Error.New("compaction salvage lost byte count overflow")
	}

	affectedLogs := len(round.budget.logs)
	for logID := range round.logs {
		if _, committed := round.budget.logs[logID]; !committed {
			affectedLogs++
		}
	}
	if _, committed := round.budget.logs[rec.Log]; !committed {
		if _, pending := round.logs[rec.Log]; !pending {
			affectedLogs++
		}
	}
	lostRecords := len(round.budget.lost) + len(round.lost) + 1
	lostBytes := round.budget.lostBytes + round.lostBytes + length
	readErrors := round.budget.readErrors + round.readErrors
	if cause == salvageCauseReadFailure {
		readErrors++
	}

	var limitErr error
	switch {
	case affectedLogs > salvageMaxAffectedLogs:
		limitErr = Error.New("compaction salvage affected log limit exceeded: %d > %d",
			affectedLogs, salvageMaxAffectedLogs)
	case readErrors > salvageMaxReadFailures:
		limitErr = Error.New("compaction salvage read failure limit exceeded: %d > %d",
			readErrors, salvageMaxReadFailures)
	case lostRecords > salvageMaxLostRecords:
		limitErr = Error.New("compaction salvage lost record limit exceeded: %d > %d",
			lostRecords, salvageMaxLostRecords)
	case lostBytes > round.budget.maxLostBytes:
		limitErr = Error.New("compaction salvage lost byte limit exceeded: %d > %d",
			lostBytes, round.budget.maxLostBytes)
	}
	if limitErr != nil {
		return limitErr
	}

	round.lost[rec.Key] = struct{}{}
	round.logs[rec.Log] = struct{}{}
	round.lostBytes += length
	switch cause {
	case salvageCauseReadFailure:
		round.readErrors++
	case salvageCauseTruncated:
		round.short++
	}
	return nil
}

func (s *Store) markSalvageAborted(round *salvageRound) {
	if round.observedLoss {
		s.stats.salvageAborted.Add(1)
		mon.Meter("compaction_salvage_aborted").Mark(1)
	}
}

func (s *Store) salvageStats() SalvageStats {
	return SalvageStats{
		SuccessfulRounds: s.stats.salvageRounds.Load(),
		LostPieces:       s.stats.salvageLostPieces.Load(),
		LostBytes:        s.stats.salvageLostBytes.Load(),
		AffectedLogs:     s.stats.salvageAffectedLogs.Load(),
		AbortedRounds:    s.stats.salvageAborted.Load(),
	}
}

func recordBeyondEOF(rec Record, size uint64) bool {
	return rec.Offset > size || uint64(rec.Length) > size-rec.Offset
}

func (round *salvageRound) keys() []Key {
	keys := make([]Key, 0, len(round.lost))
	for key := range round.lost {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return string(keys[i][:]) < string(keys[j][:])
	})
	return keys
}

func (round *salvageRound) logIDs() []uint64 {
	logs := make([]uint64, 0, len(round.logs))
	for id := range round.logs {
		logs = append(logs, id)
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i] < logs[j] })
	return logs
}

func (s *Store) reportSalvage(ctx context.Context, round *salvageRound) {
	if round.empty() {
		return
	}

	keys := round.keys()
	logIDs := round.logIDs()
	s.log.Warn("compaction salvaged unreadable records",
		zap.Uint64s("logs", logIDs),
		zap.Int("lost_records", len(keys)),
		zap.Uint64("lost_bytes", round.lostBytes),
		zap.Uint64("read_failures", round.readErrors),
		zap.Uint64("truncated_records", round.short),
	)

	mon.Meter("compaction_salvage_round").Mark(1)
	mon.Meter("compaction_salvage_lost_records").Mark(len(keys))
	mon.Meter("compaction_salvage_lost_bytes").Mark64(int64(round.lostBytes))
	mon.Meter("compaction_salvage_affected_logs").Mark(len(logIDs))
	s.stats.salvageRounds.Add(1)
	s.stats.salvageLostPieces.Add(uint64(len(keys)))
	s.stats.salvageLostBytes.Add(round.lostBytes)
	s.stats.salvageAffectedLogs.Add(uint64(len(logIDs)))

	s.amnesty(ctx, keys, AmnestyReasonReadFailure)
}

func (s *Store) rollbackSalvageWrite(fh *os.File, offset int64) error {
	truncate := fh.Truncate
	if s.fakes.compactionTruncate != nil {
		truncate = func(size int64) error {
			return s.fakes.compactionTruncate(fh, size)
		}
	}
	if err := truncate(offset); err != nil {
		return Error.New("unable to roll back compacted log to %d: %w", offset, err)
	}

	syncFile := fh.Sync
	if s.fakes.compactionSync != nil {
		syncFile = func() error {
			return s.fakes.compactionSync(fh)
		}
	}
	if err := syncFile(); err != nil {
		return Error.New("unable to sync rolled back compacted log: %w", err)
	}

	if _, err := fh.Seek(offset, io.SeekStart); err != nil {
		return Error.New("unable to seek rolled back compacted log to %d: %w", offset, err)
	}
	return nil
}

func (s *Store) compactionSourceSize(fh *os.File) (uint64, error) {
	stat := fh.Stat
	if s.fakes.compactionStat != nil {
		stat = func() (os.FileInfo, error) {
			return s.fakes.compactionStat(fh)
		}
	}
	info, err := stat()
	if err != nil {
		return 0, Error.New("unable to stat compaction source log: %w", err)
	}
	if info.Size() < 0 {
		return 0, Error.New("compaction source log has negative size: %d", info.Size())
	}
	return uint64(info.Size()), nil
}

// copyRecordChecked copies one record without copy_file_range so that read and write failures are
// distinguishable. It returns at most one of readErr and writeErr.
func (s *Store) copyRecordChecked(
	ctx context.Context,
	dst *os.File,
	src *os.File,
	offset uint64,
	length uint32,
) (readErr, writeErr error) {
	buf := make([]byte, salvageCopyBufferSize)
	remaining := uint64(length)

	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err, nil
		}

		want := uint64(len(buf))
		if remaining < want {
			want = remaining
		}

		readAt := src.ReadAt
		if s.fakes.compactionReadAt != nil {
			readAt = func(p []byte, off int64) (int, error) {
				return s.fakes.compactionReadAt(src, p, off)
			}
		}
		n, err := readAt(buf[:want], int64(offset))
		if n > 0 {
			write := dst.Write
			if s.fakes.compactionWrite != nil {
				write = func(p []byte) (int, error) {
					return s.fakes.compactionWrite(dst, p)
				}
			}
			written, writeErr := writeFull(write, buf[:n])
			if writeErr != nil {
				return nil, writeErr
			}
			if written != n {
				return nil, io.ErrShortWrite
			}
			offset += uint64(n)
			remaining -= uint64(n)
		}
		if err != nil {
			return err, nil
		}
		if n == 0 {
			return io.ErrNoProgress, nil
		}
	}
	return nil, nil
}

func writeFull(write func([]byte) (int, error), data []byte) (int, error) {
	total := 0
	for len(data) > 0 {
		n, err := write(data)
		if n < 0 || n > len(data) {
			return total, errors.New("invalid write count")
		}
		total += n
		data = data[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (s *Store) salvageableSourceReadError(err error) bool {
	if s.fakes.salvageableReadErr != nil {
		return s.fakes.salvageableReadErr(err)
	}
	return platform.IsSalvageableReadError(err)
}
