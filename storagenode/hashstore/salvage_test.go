// Copyright (C) 2026 Storj Labs, Inc.
// See LICENSE for copying information.

package hashstore

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestStore_CompactionSalvagesTruncatedLog(t *testing.T) {
	forAllTables(t, func(t *testing.T, cfg Config) {
		forAllBool(t, "orderedRewrite", func(t *testing.T, orderedRewrite bool) {
			forAllBool(t, "disableCopyFileRange", func(t *testing.T, disableCopyFileRange bool) {
				testStore_CompactionSalvagesTruncatedLog(t, cfg, orderedRewrite, disableCopyFileRange)
			})
		})
	})
}

func testStore_CompactionSalvagesTruncatedLog(
	t *testing.T,
	cfg Config,
	orderedRewrite bool,
	disableCopyFileRange bool,
) {
	cfg.Compaction.Salvage = true
	cfg.Compaction.OrderedRewrite = orderedRewrite
	cfg.Store.DisableCopyFileRange = disableCopyFileRange

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	good := s.AssertCreate(WithDataSize(512))
	lost0 := s.AssertCreate(WithDataSize(512))
	lost1 := s.AssertCreate(WithDataSize(512))
	dead := s.AssertCreate(WithDataSize(2048))

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	rec, ok, err := s.tbl.Lookup(t.Context(), lost0)
	assert.NoError(t, err)
	assert.True(t, ok)
	lf, ok := s.lfs.Lookup(rec.Log)
	assert.True(t, ok)
	sourcePath := lf.path
	assert.NoError(t, lf.fh.Truncate(int64(rec.Offset)+int64(rec.Length)/2))

	assert.NoError(t, s.Compact(t.Context(), CompactArguments{}))
	stats := s.Stats()
	assert.Equal(t, stats.Salvage.SuccessfulRounds, uint64(1))
	assert.Equal(t, stats.Salvage.LostPieces, uint64(2))
	assert.That(t, stats.Salvage.LostBytes > 0)
	assert.Equal(t, stats.Salvage.AffectedLogs, uint64(1))
	assert.Equal(t, stats.Salvage.AbortedRounds, uint64(0))

	s.AssertRead(good, WithDataSize(512))
	s.AssertNotExist(lost0)
	s.AssertNotExist(lost1)
	s.AssertNotExist(dead)

	sortKeys(amnestied)
	want := []Key{lost0, lost1}
	sortKeys(want)
	assert.DeepEqual(t, amnestied, want)

	_, err = os.Stat(sourcePath)
	assert.That(t, errors.Is(err, os.ErrNotExist))

	reported := len(amnestied)
	s.AssertReopen(WithoutHintFile(true))
	s.AssertRead(good, WithDataSize(512))
	s.AssertNotExist(lost0)
	s.AssertNotExist(lost1)
	assert.Equal(t, len(amnestied), reported)
}

func TestStore_CompactionDoesNotSalvageWhenDisabled(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = false

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	good := s.AssertCreate(WithDataSize(512))
	lost := s.AssertCreate(WithDataSize(512))
	dead := s.AssertCreate(WithDataSize(2048))

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	rec, ok, err := s.tbl.Lookup(t.Context(), lost)
	assert.NoError(t, err)
	assert.True(t, ok)
	lf, ok := s.lfs.Lookup(rec.Log)
	assert.True(t, ok)
	sourcePath := lf.path
	oldTable := s.tbl.Handle().Name()
	assert.NoError(t, lf.fh.Truncate(int64(rec.Offset)+int64(rec.Length)/2))

	err = s.Compact(t.Context(), CompactArguments{})
	assert.Error(t, err)

	assert.Equal(t, s.tbl.Handle().Name(), oldTable)
	s.AssertRead(good, WithDataSize(512))
	s.AssertExist(lost)
	assert.Equal(t, len(amnestied), 0)
	_, err = os.Stat(sourcePath)
	assert.NoError(t, err)
}

func TestStore_CompactionSalvagesSourceReadFailure(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	lost := s.AssertCreate(WithDataSize(512))
	good := s.AssertCreate(WithDataSize(512))
	dead := s.AssertCreate(WithDataSize(2048))

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	sourceErr := errors.New("injected source read failure")
	fastCopies := 0
	s.fakes.compactionCopy = func(dst io.Writer, src io.Reader, length int64) (int64, error) {
		fastCopies++
		if fastCopies == 1 {
			return 0, sourceErr
		}
		return io.CopyN(dst, src, length)
	}
	s.fakes.compactionReadAt = func(src io.ReaderAt, p []byte, offset int64) (int, error) {
		return 0, sourceErr
	}
	s.fakes.salvageableReadErr = func(err error) bool {
		return errors.Is(err, sourceErr)
	}

	assert.NoError(t, s.Compact(t.Context(), CompactArguments{}))

	s.AssertNotExist(lost)
	s.AssertRead(good, WithDataSize(512))
	assert.DeepEqual(t, amnestied, []Key{lost})
}

func TestStore_CompactionSalvagesMultipleTruncatedLogs(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true
	cfg.Compaction.MaxLogSize = 1100
	cfg.Compaction.AliveFraction = 0.9

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	good0 := s.AssertCreate(WithDataSize(256))
	lost0 := s.AssertCreate(WithDataSize(256))
	dead0 := s.AssertCreate(WithDataSize(512))
	good1 := s.AssertCreate(WithDataSize(256))
	lost1 := s.AssertCreate(WithDataSize(256))
	dead1 := s.AssertCreate(WithDataSize(512))

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead0 || key == dead1
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	var sourceLogs []uint64
	for _, key := range []Key{lost0, lost1} {
		rec, ok, err := s.tbl.Lookup(t.Context(), key)
		assert.NoError(t, err)
		assert.True(t, ok)
		sourceLogs = append(sourceLogs, rec.Log)
		lf, ok := s.lfs.Lookup(rec.Log)
		assert.True(t, ok)
		assert.NoError(t, os.Truncate(lf.path, int64(rec.Offset)+int64(rec.Length)/2))
	}
	assert.NotEqual(t, sourceLogs[0], sourceLogs[1])

	assert.NoError(t, s.Compact(t.Context(), CompactArguments{}))

	s.AssertRead(good0, WithDataSize(256))
	s.AssertRead(good1, WithDataSize(256))
	s.AssertNotExist(lost0)
	s.AssertNotExist(lost1)
	s.AssertNotExist(dead0)
	s.AssertNotExist(dead1)

	sortKeys(amnestied)
	want := []Key{lost0, lost1}
	sortKeys(want)
	assert.DeepEqual(t, amnestied, want)
}

func TestStore_CompactionAbortsOnDestinationWriteFailure(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	first := s.AssertCreate(WithDataSize(512))
	second := s.AssertCreate(WithDataSize(512))
	dead := s.AssertCreate(WithDataSize(2048))

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	oldTable := s.tbl.Handle().Name()
	copyErr := errors.New("injected fast copy failure")
	writeErr := errors.New("injected destination write failure")
	s.fakes.compactionCopy = func(io.Writer, io.Reader, int64) (int64, error) {
		return 0, copyErr
	}
	s.fakes.compactionWrite = func(io.Writer, []byte) (int, error) {
		return 0, writeErr
	}

	err := s.Compact(t.Context(), CompactArguments{})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), writeErr.Error()))

	assert.Equal(t, s.tbl.Handle().Name(), oldTable)
	s.AssertRead(first, WithDataSize(512))
	s.AssertRead(second, WithDataSize(512))
	assert.Equal(t, len(amnestied), 0)
}

func TestStore_CompactionDoesNotReportSalvageBeforeCommit(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	lost := s.AssertCreate(WithDataSize(512))
	good := s.AssertCreate(WithDataSize(512))
	dead := s.AssertCreate(WithDataSize(2048))
	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		return key == dead
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	sourceErr := errors.New("injected source read failure")
	s.fakes.compactionCopy = func(io.Writer, io.Reader, int64) (int64, error) {
		return 0, sourceErr
	}
	s.fakes.compactionReadAt = func(io.ReaderAt, []byte, int64) (int, error) {
		return 0, sourceErr
	}
	s.fakes.salvageableReadErr = func(err error) bool {
		return errors.Is(err, sourceErr)
	}

	oldTable := s.tbl.Handle().Name()
	nextTable := filepath.Join(s.tablePath, createHashtblName(s.maxTbl.Load()+1))
	assert.NoError(t, os.Mkdir(nextTable, 0700))
	abortedBefore := mon.Meter("compaction_salvage_aborted").Total()

	err := s.Compact(t.Context(), CompactArguments{})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "unable to commit newly compacted hashtbl"))
	assert.Equal(t, mon.Meter("compaction_salvage_aborted").Total(), abortedBefore+1)
	stats := s.Stats()
	assert.Equal(t, stats.Compactions, uint64(2))
	assert.Equal(t, stats.CompactionFailures, uint64(1))
	assert.Equal(t, stats.Salvage.SuccessfulRounds, uint64(0))
	assert.Equal(t, stats.Salvage.LostPieces, uint64(0))
	assert.Equal(t, stats.Salvage.AbortedRounds, uint64(1))
	assert.NoError(t, os.Remove(nextTable))

	assert.Equal(t, s.tbl.Handle().Name(), oldTable)
	s.AssertRead(lost, WithDataSize(512))
	s.AssertRead(good, WithDataSize(512))
	assert.Equal(t, len(amnestied), 0)
}

func TestStore_CompactionSalvageFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		install func(*Store, error)
		message string
	}{
		{
			name: "truncate",
			install: func(s *Store, injected error) {
				s.fakes.compactionTruncate = func(*os.File, int64) error { return injected }
			},
			message: "destination rollback failed",
		},
		{
			name: "sync",
			install: func(s *Store, injected error) {
				s.fakes.compactionSync = func(*os.File) error { return injected }
			},
			message: "destination rollback failed",
		},
		{
			name: "stat",
			install: func(s *Store, injected error) {
				s.fakes.compactionStat = func(*os.File) (os.FileInfo, error) { return nil, injected }
			},
			message: "unable to stat compaction source log",
		},
		{
			name: "unknown source read error",
			install: func(s *Store, injected error) {
				s.fakes.compactionReadAt = func(io.ReaderAt, []byte, int64) (int, error) {
					return 0, injected
				}
			},
			message: "reading compaction source during checked copy",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.Compaction.Salvage = true

			var amnestied []Key
			s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
				assert.Equal(t, reason, AmnestyReasonReadFailure)
				amnestied = append(amnestied, keys...)
			}))
			defer s.Close()

			first := s.AssertCreate(WithDataSize(512))
			second := s.AssertCreate(WithDataSize(512))
			dead := s.AssertCreate(WithDataSize(2048))
			s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
				return key == dead
			}))
			s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

			oldTable := s.tbl.Handle().Name()
			fastErr := errors.New("injected fast copy failure")
			s.fakes.compactionCopy = func(io.Writer, io.Reader, int64) (int64, error) {
				return 0, fastErr
			}
			injected := errors.New("injected " + test.name + " failure")
			test.install(s.Store, injected)
			abortedBefore := mon.Meter("compaction_salvage_aborted").Total()

			err := s.Compact(t.Context(), CompactArguments{})
			assert.Error(t, err)
			assert.That(t, strings.Contains(err.Error(), test.message))
			assert.Equal(t, mon.Meter("compaction_salvage_aborted").Total(), abortedBefore)

			assert.Equal(t, s.tbl.Handle().Name(), oldTable)
			s.AssertRead(first, WithDataSize(512))
			s.AssertRead(second, WithDataSize(512))
			assert.Equal(t, len(amnestied), 0)
		})
	}
}

func TestStore_CompactionSalvageAffectedLogLimitIsAtomic(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true
	cfg.Compaction.MaxLogSize = 1100
	cfg.Compaction.AliveFraction = 0.9

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	var good, lost, dead []Key
	for range salvageMaxAffectedLogs + 1 {
		good = append(good, s.AssertCreate(WithDataSize(256)))
		lost = append(lost, s.AssertCreate(WithDataSize(256)))
		dead = append(dead, s.AssertCreate(WithDataSize(512)))
	}

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		for _, deadKey := range dead {
			if key == deadKey {
				return true
			}
		}
		return false
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	for _, key := range lost {
		rec, ok, err := s.tbl.Lookup(t.Context(), key)
		assert.NoError(t, err)
		assert.True(t, ok)
		lf, ok := s.lfs.Lookup(rec.Log)
		assert.True(t, ok)
		assert.NoError(t, os.Truncate(lf.path, int64(rec.Offset)+int64(rec.Length)/2))
	}

	// The logs were created with a small limit. Raise only the salvage byte budget so this test
	// reaches the affected-log guard first.
	s.cfg.Compaction.MaxLogSize = 1 << 30
	oldTable := s.tbl.Handle().Name()
	abortedBefore := mon.Meter("compaction_salvage_aborted").Total()

	err := s.Compact(t.Context(), CompactArguments{})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "affected log limit exceeded"))
	assert.Equal(t, mon.Meter("compaction_salvage_aborted").Total(), abortedBefore+1)
	assert.Equal(t, s.tbl.Handle().Name(), oldTable)
	assert.Equal(t, len(amnestied), 0)

	for _, key := range good {
		s.AssertRead(key, WithDataSize(256))
	}
	for _, key := range lost {
		s.AssertExist(key)
	}
}

func TestStore_CompactionSalvageAffectedLogLimitSpansRounds(t *testing.T) {
	cfg := defaultConfig()
	cfg.Compaction.Salvage = true
	cfg.Compaction.MaxLogSize = 1100
	cfg.Compaction.AliveFraction = 0.9
	cfg.Compaction.RewriteMultiple = 1e-10

	var amnestied []Key
	s := newTestStore(t, cfg, WithAmnesty(func(ctx context.Context, keys []Key, reason AmnestyReason) {
		assert.Equal(t, reason, AmnestyReasonReadFailure)
		amnestied = append(amnestied, keys...)
	}))
	defer s.Close()

	var good, lost, dead []Key
	for range salvageMaxAffectedLogs + 1 {
		good = append(good, s.AssertCreate(WithDataSize(256)))
		lost = append(lost, s.AssertCreate(WithDataSize(256)))
		dead = append(dead, s.AssertCreate(WithDataSize(512)))
	}

	s.AssertCompact(WithShouldTrash(func(ctx context.Context, key Key, created time.Time) bool {
		for _, deadKey := range dead {
			if key == deadKey {
				return true
			}
		}
		return false
	}))
	s.today += uint32(s.cfg.Compaction.ExpiresDays) + 1

	for _, key := range lost {
		rec, ok, err := s.tbl.Lookup(t.Context(), key)
		assert.NoError(t, err)
		assert.True(t, ok)
		lf, ok := s.lfs.Lookup(rec.Log)
		assert.True(t, ok)
		assert.NoError(t, os.Truncate(lf.path, int64(rec.Offset)+int64(rec.Length)/2))
	}

	// Raise only the byte limit. The tiny rewrite multiple forces one source log into each atomic
	// round, so the fourth damaged log can only be rejected by a Compact-wide budget.
	s.cfg.Compaction.MaxLogSize = 1 << 30
	oldTable := s.tbl.Handle().Name()

	err := s.Compact(t.Context(), CompactArguments{})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "affected log limit exceeded"))
	assert.NotEqual(t, s.tbl.Handle().Name(), oldTable)
	assert.Equal(t, len(amnestied), salvageMaxAffectedLogs)

	amnestiedSet := make(map[Key]struct{}, len(amnestied))
	for _, key := range amnestied {
		amnestiedSet[key] = struct{}{}
	}
	for _, key := range good {
		s.AssertRead(key, WithDataSize(256))
	}
	for _, key := range lost {
		if _, ok := amnestiedSet[key]; ok {
			s.AssertNotExist(key)
		} else {
			s.AssertExist(key)
		}
	}
}

func TestSalvageRoundLimits(t *testing.T) {
	t.Run("affected logs", func(t *testing.T) {
		round := newSalvageBudget(math.MaxUint64).newRound()
		for id := uint64(1); id <= salvageMaxAffectedLogs; id++ {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: id}, salvageCauseTruncated))
		}
		err := round.add(Record{Key: newKey(), Log: salvageMaxAffectedLogs + 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "affected log limit"))
	})

	t.Run("read failures", func(t *testing.T) {
		round := newSalvageBudget(math.MaxUint64).newRound()
		for range salvageMaxReadFailures {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: 1}, salvageCauseReadFailure))
		}
		err := round.add(Record{Key: newKey(), Log: 1}, salvageCauseReadFailure)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "read failure limit"))
	})

	t.Run("lost records", func(t *testing.T) {
		round := newSalvageBudget(math.MaxUint64).newRound()
		for range salvageMaxLostRecords {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated))
		}
		err := round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "lost record limit"))
	})

	t.Run("lost bytes", func(t *testing.T) {
		round := newSalvageBudget(RecordSize).newRound()
		err := round.add(Record{Key: newKey(), Log: 1, Length: 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "lost byte limit"))
	})
}

func TestRecordBeyondEOF(t *testing.T) {
	tests := []struct {
		name string
		rec  Record
		size uint64
		want bool
	}{
		{name: "within file", rec: Record{Offset: 2, Length: 3}, size: 5},
		{name: "zero length at EOF", rec: Record{Offset: 5}, size: 5},
		{name: "length beyond EOF", rec: Record{Offset: 2, Length: 4}, size: 5, want: true},
		{name: "offset beyond EOF", rec: Record{Offset: 6}, size: 5, want: true},
		{name: "end overflow", rec: Record{Offset: math.MaxUint64, Length: 1}, size: math.MaxUint64, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, recordBeyondEOF(test.rec, test.size), test.want)
		})
	}
}

func TestSalvageLimitsSpanCommittedRounds(t *testing.T) {
	t.Run("affected logs", func(t *testing.T) {
		budget := newSalvageBudget(math.MaxUint64)
		round := budget.newRound()
		for id := uint64(1); id <= salvageMaxAffectedLogs; id++ {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: id}, salvageCauseTruncated))
		}
		budget.commit(round)

		round = budget.newRound()
		err := round.add(Record{Key: newKey(), Log: salvageMaxAffectedLogs + 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "affected log limit"))
		assert.True(t, round.empty())
	})

	t.Run("read failures", func(t *testing.T) {
		budget := newSalvageBudget(math.MaxUint64)
		round := budget.newRound()
		for range salvageMaxReadFailures {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: 1}, salvageCauseReadFailure))
		}
		budget.commit(round)

		round = budget.newRound()
		err := round.add(Record{Key: newKey(), Log: 1}, salvageCauseReadFailure)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "read failure limit"))
		assert.True(t, round.empty())
	})

	t.Run("lost records", func(t *testing.T) {
		budget := newSalvageBudget(math.MaxUint64)
		round := budget.newRound()
		for range salvageMaxLostRecords {
			assert.NoError(t, round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated))
		}
		budget.commit(round)

		round = budget.newRound()
		err := round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "lost record limit"))
		assert.True(t, round.empty())
	})

	t.Run("lost bytes", func(t *testing.T) {
		budget := newSalvageBudget(RecordSize)
		round := budget.newRound()
		assert.NoError(t, round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated))
		budget.commit(round)

		round = budget.newRound()
		err := round.add(Record{Key: newKey(), Log: 1}, salvageCauseTruncated)
		assert.Error(t, err)
		assert.That(t, strings.Contains(err.Error(), "lost byte limit"))
		assert.True(t, round.empty())
	})
}

func TestConfigSalvageDisabledByDefault(t *testing.T) {
	assert.False(t, defaultConfig().Compaction.Salvage)
}

func sortKeys(keys []Key) {
	sort.Slice(keys, func(i, j int) bool {
		return string(keys[i][:]) < string(keys[j][:])
	})
}
