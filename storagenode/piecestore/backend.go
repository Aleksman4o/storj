// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package piecestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/spacemonkeygo/monkit/v3"
	"github.com/zeebo/errs"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"storj.io/common/pb"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/storj"
	"storj.io/storj/storagenode/contact"
	"storj.io/storj/storagenode/hashstore"
	"storj.io/storj/storagenode/monitor"
	"storj.io/storj/storagenode/pieces"
	"storj.io/storj/storagenode/retain"
)

// PieceBackend is the minimal interface needed for the endpoints to do its job.
type PieceBackend interface {
	Writer(context.Context, storj.NodeID, storj.PieceID, pb.PieceHashAlgorithm, time.Time) (PieceWriter, error)
	Reader(context.Context, storj.NodeID, storj.PieceID) (PieceReader, error)
	StartRestore(context.Context, storj.NodeID) error
}

// PieceWriter is an interface for writing a piece.
type PieceWriter interface {
	io.Writer
	Size() int64
	Hash() []byte
	Cancel(context.Context) error
	Commit(context.Context, *pb.PieceHeader) error
}

// PieceReader is an interface for reading a piece.
type PieceReader interface {
	io.ReadSeekCloser
	Trash() bool
	Size() int64
	GetPieceHeader() (*pb.PieceHeader, error)
}

//
// hash store backend
//

// HashStoreBackend implements PieceBackend using the hashstore.
type HashStoreBackend struct {
	logsPath  string
	tablePath string
	cfg       hashstore.Config

	bfm     *retain.BloomFilterManager
	rtm     *retain.RestoreTimeManager
	log     *zap.Logger
	amnesty *contact.AmnestyClient

	mu  sync.Mutex
	dbs map[storj.NodeID]*hashstore.DB

	runnerCtx    context.Context
	runnerCancel context.CancelFunc
	runnerWG     sync.WaitGroup

	manualMu      sync.Mutex
	manualClosing bool
	manualNextID  uint64
	manualStatus  ManualCompactionStatus
}

// ManualCompactionState identifies the state of the latest manual compaction job.
type ManualCompactionState string

const (
	// ManualCompactionIdle means no manual compaction has been started in this process.
	ManualCompactionIdle ManualCompactionState = "idle"
	// ManualCompactionRunning means a manual compaction is currently running.
	ManualCompactionRunning ManualCompactionState = "running"
	// ManualCompactionSucceeded means the latest manual compaction completed without failures.
	ManualCompactionSucceeded ManualCompactionState = "succeeded"
	// ManualCompactionFailed means at least one satellite failed during the latest manual compaction.
	ManualCompactionFailed ManualCompactionState = "failed"
	// ManualCompactionCanceled means node shutdown canceled the latest manual compaction.
	ManualCompactionCanceled ManualCompactionState = "canceled"
)

var (
	// ErrManualCompactionDisabled is returned when manual log compaction is not enabled.
	ErrManualCompactionDisabled = errors.New("manual log compaction is disabled")
	// ErrManualCompactionRunning is returned when a manual compaction is already running.
	ErrManualCompactionRunning = errors.New("manual compaction is already running")
	// ErrHashStoreBackendClosed is returned when the hashstore backend is closing.
	ErrHashStoreBackendClosed = errors.New("hashstore backend is closed")
)

// ManualCompactionResult contains the result for one satellite in a manual compaction job.
type ManualCompactionResult struct {
	SatelliteID storj.NodeID
	Status      string
	Error       string
}

// ManualCompactionStatus contains a snapshot of the latest manual compaction job.
type ManualCompactionStatus struct {
	ID                  uint64
	State               ManualCompactionState
	StartedAt           time.Time
	FinishedAt          time.Time
	CurrentSatellite    storj.NodeID
	TotalSatellites     int
	ProcessedSatellites int
	Results             []ManualCompactionResult
}

func (status ManualCompactionStatus) clone() ManualCompactionStatus {
	status.Results = append([]ManualCompactionResult(nil), status.Results...)
	return status
}

// NewHashStoreBackend constructs a new HashStoreBackend with the provided values. The log and hash
// directory are allowed to be the same.
func NewHashStoreBackend(
	ctx context.Context,
	cfg hashstore.Config,
	logsPath string,
	tablePath string,
	bfm *retain.BloomFilterManager,
	rtm *retain.RestoreTimeManager,
	log *zap.Logger,
	amnesty *contact.AmnestyClient,
) (*HashStoreBackend, error) {

	if tablePath == "" {
		tablePath = logsPath
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())

	hsb := &HashStoreBackend{
		logsPath:  logsPath,
		tablePath: tablePath,
		cfg:       cfg,
		bfm:       bfm,
		rtm:       rtm,
		log:       log,
		amnesty:   amnesty,

		dbs:          map[storj.NodeID]*hashstore.DB{},
		runnerCtx:    runnerCtx,
		runnerCancel: runnerCancel,
		manualStatus: ManualCompactionStatus{State: ManualCompactionIdle},
	}

	// open any existing databases
	entries, err := os.ReadDir(logsPath)
	if errors.Is(err, fs.ErrNotExist) {
		return hsb, nil
	} else if err != nil {
		return nil, errs.Wrap(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		satellite, err := storj.NodeIDFromString(entry.Name())
		if err != nil {
			continue // ignore directories that aren't node IDs
		}
		if _, err := hsb.getDB(ctx, satellite); err != nil {
			return nil, errs.Wrap(err)
		}
	}

	return hsb, nil
}

// TestingCompact calls Compact on all of the hashstore databases.
func (hsb *HashStoreBackend) TestingCompact(ctx context.Context) error {
	hsb.mu.Lock()
	defer hsb.mu.Unlock()

	for _, db := range hsb.dbs {
		if err := db.Compact(ctx); err != nil {
			return err
		}
	}
	return nil
}

// LogsPath returns the path to the logs directory.
func (hsb *HashStoreBackend) LogsPath() string {
	return hsb.logsPath
}

// SalvageEnabled reports whether compaction salvage is enabled.
func (hsb *HashStoreBackend) SalvageEnabled() bool {
	return hsb.cfg.Compaction.Salvage
}

// ManualLogCompactionEnabled reports whether automatic compactions avoid rewriting partial logs
// and the operator API may start full compactions.
func (hsb *HashStoreBackend) ManualLogCompactionEnabled() bool {
	return hsb.cfg.Compaction.ManualLogCompaction
}

// ManualCompactionStatus returns a snapshot of the latest manual compaction job.
func (hsb *HashStoreBackend) ManualCompactionStatus() ManualCompactionStatus {
	hsb.manualMu.Lock()
	defer hsb.manualMu.Unlock()
	return hsb.manualStatus.clone()
}

// StartManualCompaction starts one asynchronous full compaction job across all satellite DBs.
// Satellite DBs are compacted sequentially, while their automatic table-only compactions remain
// independent.
func (hsb *HashStoreBackend) StartManualCompaction() (ManualCompactionStatus, error) {
	if !hsb.ManualLogCompactionEnabled() {
		return hsb.ManualCompactionStatus(), ErrManualCompactionDisabled
	}

	hsb.manualMu.Lock()
	defer hsb.manualMu.Unlock()

	if hsb.manualClosing {
		return hsb.manualStatus.clone(), ErrHashStoreBackendClosed
	}
	if hsb.manualStatus.State == ManualCompactionRunning {
		return hsb.manualStatus.clone(), ErrManualCompactionRunning
	}

	dbs := hsb.compactionDBsSnapshot()
	hsb.manualNextID++
	hsb.manualStatus = ManualCompactionStatus{
		ID:              hsb.manualNextID,
		State:           ManualCompactionRunning,
		StartedAt:       time.Now().UTC(),
		TotalSatellites: len(dbs),
		Results:         make([]ManualCompactionResult, 0, len(dbs)),
	}
	status := hsb.manualStatus.clone()

	hsb.runnerWG.Add(1)
	hsb.log.Info("manual compaction job accepted",
		zap.Uint64("job_id", status.ID),
		zap.Int("satellite_count", status.TotalSatellites),
	)
	go hsb.runManualCompaction(status.ID, dbs)
	return status, nil
}

type compactionDB struct {
	satelliteID storj.NodeID
	db          *hashstore.DB
}

func (hsb *HashStoreBackend) compactionDBsSnapshot() []compactionDB {
	dbs := hsb.dbsCopy()
	snapshot := make([]compactionDB, 0, len(dbs))
	for satelliteID, db := range dbs {
		snapshot = append(snapshot, compactionDB{satelliteID: satelliteID, db: db})
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].satelliteID.String() < snapshot[j].satelliteID.String()
	})
	return snapshot
}

func (hsb *HashStoreBackend) isCurrentDB(entry compactionDB) bool {
	hsb.mu.Lock()
	defer hsb.mu.Unlock()
	db, ok := hsb.dbs[entry.satelliteID]
	return ok && db == entry.db
}

func (hsb *HashStoreBackend) runManualCompaction(jobID uint64, dbs []compactionDB) {
	defer hsb.runnerWG.Done()

	failed := false
	canceled := false
	for _, entry := range dbs {
		if err := hsb.runnerCtx.Err(); err != nil {
			canceled = true
			break
		}

		if !hsb.isCurrentDB(entry) {
			hsb.finishManualSatellite(jobID, entry.satelliteID, "skipped", "satellite removed")
			continue
		}

		hsb.manualMu.Lock()
		if hsb.manualStatus.ID == jobID && hsb.manualStatus.State == ManualCompactionRunning {
			hsb.manualStatus.CurrentSatellite = entry.satelliteID
		}
		hsb.manualMu.Unlock()

		hsb.log.Info("manual compaction starting satellite", zap.Stringer("satellite", entry.satelliteID))
		err := entry.db.Compact(hsb.runnerCtx)
		if err == nil {
			hsb.finishManualSatellite(jobID, entry.satelliteID, "succeeded", "")
			hsb.log.Info("manual compaction finished satellite", zap.Stringer("satellite", entry.satelliteID))
			continue
		}

		if hsb.runnerCtx.Err() != nil {
			canceled = true
			break
		}
		if !hsb.isCurrentDB(entry) {
			hsb.finishManualSatellite(jobID, entry.satelliteID, "skipped", "satellite removed")
			continue
		}

		failed = true
		hsb.finishManualSatellite(jobID, entry.satelliteID, "failed", err.Error())
		hsb.log.Error("manual compaction failed for satellite",
			zap.Stringer("satellite", entry.satelliteID),
			zap.Error(err),
		)
	}

	hsb.manualMu.Lock()
	if hsb.manualStatus.ID != jobID {
		hsb.manualMu.Unlock()
		return
	}
	hsb.manualStatus.CurrentSatellite = storj.NodeID{}
	hsb.manualStatus.FinishedAt = time.Now().UTC()
	switch {
	case canceled:
		hsb.manualStatus.State = ManualCompactionCanceled
	case failed:
		hsb.manualStatus.State = ManualCompactionFailed
	default:
		hsb.manualStatus.State = ManualCompactionSucceeded
	}
	status := hsb.manualStatus.clone()
	hsb.manualMu.Unlock()

	hsb.log.Info("manual compaction job finished",
		zap.Uint64("job_id", status.ID),
		zap.String("state", string(status.State)),
		zap.Int("processed_satellites", status.ProcessedSatellites),
		zap.Int("satellite_count", status.TotalSatellites),
		zap.Duration("duration", status.FinishedAt.Sub(status.StartedAt)),
	)
}

func (hsb *HashStoreBackend) finishManualSatellite(jobID uint64, satelliteID storj.NodeID, status, errMessage string) {
	hsb.manualMu.Lock()
	defer hsb.manualMu.Unlock()
	if hsb.manualStatus.ID != jobID || hsb.manualStatus.State != ManualCompactionRunning {
		return
	}
	hsb.manualStatus.Results = append(hsb.manualStatus.Results, ManualCompactionResult{
		SatelliteID: satelliteID,
		Status:      status,
		Error:       errMessage,
	})
	hsb.manualStatus.ProcessedSatellites++
}

// Close closes the HashStoreBackend.
func (hsb *HashStoreBackend) Close() error {
	hsb.manualMu.Lock()
	hsb.manualClosing = true
	hsb.runnerCancel()
	hsb.manualMu.Unlock()
	hsb.runnerWG.Wait()

	hsb.mu.Lock()
	defer hsb.mu.Unlock()

	var eg errs.Group
	for _, db := range hsb.dbs {
		eg.Add(db.Close())
	}
	return eg.Err()
}

func (hsb *HashStoreBackend) dbsCopy() map[storj.NodeID]*hashstore.DB {
	hsb.mu.Lock()
	defer hsb.mu.Unlock()

	return maps.Clone(hsb.dbs)
}

// SatelliteCompactionStats contains a hashstore statistics snapshot for one satellite.
type SatelliteCompactionStats struct {
	SatelliteID storj.NodeID
	Database    hashstore.DBStats
	Stores      [2]hashstore.StoreStats
}

// CompactionStats returns hashstore statistics sorted by satellite ID.
func (hsb *HashStoreBackend) CompactionStats() []SatelliteCompactionStats {
	dbs := hsb.dbsCopy()
	stats := make([]SatelliteCompactionStats, 0, len(dbs))
	for id, db := range dbs {
		dbStats, s0Stats, s1Stats := db.Stats()
		stats = append(stats, SatelliteCompactionStats{
			SatelliteID: id,
			Database:    dbStats,
			Stores:      [2]hashstore.StoreStats{s0Stats, s1Stats},
		})
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].SatelliteID.String() < stats[j].SatelliteID.String()
	})
	return stats
}

// Stats implements monkit.StatSource.
func (hsb *HashStoreBackend) Stats(cb func(key monkit.SeriesKey, field string, val float64)) {
	for _, stats := range hsb.CompactionStats() {
		taggedSeries := monkit.NewSeriesKey("hashstore").WithTag("satellite", stats.SatelliteID.String())
		monkit.StatSourceFromStruct(taggedSeries, stats.Database).Stats(cb)
		monkit.StatSourceFromStruct(taggedSeries.WithTag("db", "s0"), stats.Stores[0]).Stats(cb)
		monkit.StatSourceFromStruct(taggedSeries.WithTag("db", "s1"), stats.Stores[1]).Stats(cb)
	}
}

// SpaceUsage gets a monitor.SpaceUsage from the HashStoreBackend.
func (hsb *HashStoreBackend) SpaceUsage() (subs monitor.SpaceUsage) {
	for _, db := range hsb.dbsCopy() {
		stats, _, _ := db.Stats()
		subs.UsedTotal += int64(stats.LenLogs + stats.TableSize)
		subs.UsedForPieces += int64(stats.LenSet - stats.LenTrash)
		subs.UsedForTrash += int64(stats.LenTrash)
		subs.UsedForMetadata += int64(stats.TableSize)
		subs.UsedReclaimable += int64(stats.LenLogs - stats.LenSet)
		subs.Reserved += int64(stats.FreeRequired)
	}
	return subs
}

// ForgetSatellite closes the database for the satellite and removes the directory.
func (hsb *HashStoreBackend) ForgetSatellite(ctx context.Context, satellite storj.NodeID) (err error) {
	defer mon.Task()(&ctx)(&err)

	hsb.mu.Lock()
	defer hsb.mu.Unlock()

	db, exists := hsb.dbs[satellite]
	if !exists {
		return nil
	}
	delete(hsb.dbs, satellite)

	_ = db.Close()

	err = os.RemoveAll(filepath.Join(hsb.logsPath, satellite.String()))
	if err != nil {
		return errs.Wrap(err)
	}

	err = os.RemoveAll(filepath.Join(hsb.tablePath, satellite.String()))
	if err != nil {
		return errs.Wrap(err)
	}
	return nil
}

func (hsb *HashStoreBackend) getDB(ctx context.Context, satellite storj.NodeID) (*hashstore.DB, error) {
	hsb.mu.Lock()
	defer hsb.mu.Unlock()

	if db, exists := hsb.dbs[satellite]; exists {
		return db, nil
	}

	start := time.Now()

	var log *zap.Logger
	if hsb.log != nil {
		log = hsb.log.With(zap.String("satellite", satellite.String()))
	} else {
		log = zap.NewNop()
	}

	var (
		shouldTrash   func(ctx context.Context, pieceID storj.PieceID, created time.Time) bool
		lastRestore   func(ctx context.Context) time.Time
		amnestyReport hashstore.AmnestyCallback
	)
	if hsb.bfm != nil {
		shouldTrash = hsb.bfm.GetBloomFilter(satellite)
	}
	if hsb.rtm != nil {
		lastRestore = func(ctx context.Context) time.Time {
			return hsb.rtm.GetRestoreTime(ctx, satellite, time.Now())
		}
	}
	if hsb.amnesty != nil {
		amnestyReport = func(ctx context.Context, pieceIDs []storj.PieceID, reason hashstore.AmnestyReason) {
			var lostPieceReason pb.LostPieceReason
			switch reason {
			case hashstore.AmnestyReasonHashMismatch:
				lostPieceReason = pb.LostPieceReason_HASH_MISMATCH
			case hashstore.AmnestyReasonReadFailure:
				lostPieceReason = pb.LostPieceReason_READ_FAILURE
			default:
				log.Error("refusing to report bad pieces with unknown amnesty reason",
					zap.Uint8("reason", uint8(reason)),
				)
				return
			}

			for _, pieceID := range pieceIDs {
				if err := hsb.amnesty.ReportBadPieceWithReason(ctx, satellite, pieceID, lostPieceReason); err != nil {
					log.Error("failed to report bad piece to amnesty",
						zap.Stringer("reason", lostPieceReason),
						zap.Stringer("piece_id", pieceID),
						zap.Error(err),
					)
				}
			}
		}
	}

	db, err := hashstore.New(
		ctx,
		hsb.cfg,
		filepath.Join(hsb.logsPath, satellite.String()),
		filepath.Join(hsb.tablePath, satellite.String()),
		log,
		hashstore.Callbacks{
			ShouldTrash: shouldTrash,
			LastRestore: lastRestore,
			Valid:       pieceValid,
			Amnesty:     amnestyReport,
		},
	)
	if err != nil {
		return nil, err
	}

	hsb.dbs[satellite] = db

	stats, _, _ := db.Stats()
	log.Info("hashstore opened successfully",
		zap.Duration("open_time", time.Since(start)),
		zap.Int("logs_skipped", stats.LogsSkipped),
		zap.Int("logs_matched", stats.LogsMatched),
		zap.Int("logs_mismatched", stats.LogsMismatched),
	)
	return db, nil
}

// Writer implements PieceBackend.
func (hsb *HashStoreBackend) Writer(ctx context.Context, satellite storj.NodeID, pieceID storj.PieceID, hashAlgo pb.PieceHashAlgorithm, expires time.Time) (_ PieceWriter, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := hsb.getDB(ctx, satellite)
	if err != nil {
		return nil, err
	}
	writer, err := db.Create(ctx, pieceID, expires)
	if err != nil {
		return nil, err
	}
	var hasher hash.Hash
	if hashAlgo == -1 {
		hasher = nohash{}
	} else {
		hasher = pb.NewHashFromAlgorithm(hashAlgo)
	}
	return &hashStoreWriter{
		writer: writer,
		hasher: hasher,
	}, nil
}

// Reader implements PieceBackend.
func (hsb *HashStoreBackend) Reader(ctx context.Context, satellite storj.NodeID, pieceID storj.PieceID) (_ PieceReader, err error) {
	defer mon.Task()(&ctx)(&err)

	db, err := hsb.getDB(ctx, satellite)
	if err != nil {
		return nil, err
	}
	ttfb := newTimer(mon.DurationVal("download_time_to_first_byte_read"))
	reader, err := db.Read(ctx, pieceID)
	if err != nil {
		return nil, err
	}
	return &hashStoreReader{
		sr:     io.NewSectionReader(reader, 0, reader.Size()-512),
		reader: reader,
		ttfb:   ttfb,
	}, nil
}

// StartRestore implements PieceBackend.
func (hsb *HashStoreBackend) StartRestore(ctx context.Context, satellite storj.NodeID) (err error) {
	defer mon.Task()(&ctx)(&err)

	return hsb.rtm.SetRestoreTime(ctx, satellite, time.Now())
}

type hashStoreWriter struct {
	writer *hashstore.Writer
	size   int64

	hasher hash.Hash
}

func (hw *hashStoreWriter) Write(p []byte) (int, error) {
	n, err := hw.writer.Write(p)
	hw.size += int64(n)
	hw.hasher.Write(p[:n])
	return n, err
}

func (hw *hashStoreWriter) Size() int64                      { return hw.size }
func (hw *hashStoreWriter) Hash() []byte                     { return hw.hasher.Sum(nil) }
func (hw *hashStoreWriter) Cancel(ctx context.Context) error { hw.writer.Cancel(); return nil }

func (hw *hashStoreWriter) Commit(ctx context.Context, header *pb.PieceHeader) (err error) {
	defer mon.Task()(&ctx)(&err)

	defer func() { _ = hw.Cancel(ctx) }()

	// marshal the header so we can put it as a footer.
	buf, err := pb.Marshal(header)
	if err != nil {
		return err
	} else if len(buf) > 512-2 {
		return errs.New("header too large")
	}

	// make a length prefixed footer and copy the header into it.
	var tmp [512]byte
	binary.BigEndian.PutUint16(tmp[0:2], uint16(len(buf)))
	copy(tmp[2:], buf)

	// write the footer.. header? footer.
	if _, err := hw.writer.Write(tmp[:]); err != nil {
		return err
	}

	// commit the piece.
	return hw.writer.Close()
}

type hashStoreReader struct {
	sr     *io.SectionReader
	reader *hashstore.Reader
	ttfb   *timer
}

func (hr *hashStoreReader) Read(p []byte) (int, error) {
	defer hr.ttfb.Trigger()
	return hr.sr.Read(p)
}

func (hr *hashStoreReader) Seek(offset int64, whence int) (int64, error) {
	return hr.sr.Seek(offset, whence)
}

func (hr *hashStoreReader) Close() error { return hr.reader.Close() }
func (hr *hashStoreReader) Trash() bool  { return hr.reader.Trash() }
func (hr *hashStoreReader) Size() int64  { return hr.reader.Size() - 512 }

func (hr *hashStoreReader) GetPieceHeader() (_ *pb.PieceHeader, err error) {
	data, err := io.ReadAll(io.NewSectionReader(hr.reader, hr.reader.Size()-512, 512))
	hr.ttfb.Trigger()
	if err != nil {
		return nil, err
	}
	if len(data) != 512 {
		return nil, errs.New("footer too small")
	}
	l := binary.BigEndian.Uint16(data[0:2])
	if int(l) > len(data) {
		return nil, errs.New("footer length field too large: %d > %d", l, len(data))
	}
	var header pb.PieceHeader
	if err := pb.Unmarshal(data[2:2+l], &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func pieceValid(pieceID storj.PieceID, contents []byte) bool {
	// we need at least enough data for the footer
	if len(contents) < 512 {
		return false
	}

	// split the contents into the data portion and the footer portion
	data, suffix := contents[:len(contents)-512], contents[len(contents)-512:]
	if len(suffix) < 512 {
		return false
	}

	// read the footer length
	l := uint(binary.BigEndian.Uint16(suffix[0:2]))
	if l > 510 {
		return false
	}

	// unmarshal the header
	var header pb.PieceHeader
	if err := pb.Unmarshal(suffix[2:2+l], &header); err != nil {
		return false
	}

	// verify the piece ID matches
	if header.OrderLimit.PieceId != pieceID {
		return false
	}

	// verify the hash matches
	hasher := pb.NewHashFromAlgorithm(header.HashAlgorithm)
	hasher.Write(data)
	return bytes.Equal(hasher.Sum(nil), header.Hash)
}

//
// the old stuff
//

// OldPieceBackend takes a bunch of pieces the endpoint used and packages them into a PieceBackend.
type OldPieceBackend struct {
	store      *pieces.Store
	trashChore RestoreTrash
	monitor    *monitor.Service
}

// NewOldPieceBackend constructs an OldPieceBackend.
func NewOldPieceBackend(store *pieces.Store, trashChore RestoreTrash, monitor *monitor.Service) *OldPieceBackend {
	return &OldPieceBackend{
		store:      store,
		trashChore: trashChore,
		monitor:    monitor,
	}
}

// Writer implements PieceBackend and returns a PieceWriter for a piece.
func (opb *OldPieceBackend) Writer(ctx context.Context, satellite storj.NodeID, pieceID storj.PieceID, hashAlgorithm pb.PieceHashAlgorithm, expiration time.Time) (_ PieceWriter, err error) {
	defer mon.Task()(&ctx)(&err)

	writer, err := opb.store.Writer(ctx, satellite, pieceID, hashAlgorithm)
	if err != nil {
		return nil, err
	}
	return &oldPieceWriter{
		Writer:      writer,
		store:       opb.store,
		satelliteID: satellite,
		pieceID:     pieceID,
		expiration:  expiration,
	}, nil
}

// Reader implements PieceBackend and returns a PieceReader for a piece.
func (opb *OldPieceBackend) Reader(ctx context.Context, satellite storj.NodeID, pieceID storj.PieceID) (_ PieceReader, err error) {
	defer mon.Task()(&ctx)(&err)

	reader, err := opb.store.Reader(ctx, satellite, pieceID)
	if err == nil {
		return &oldPieceReader{
			Reader:    reader,
			store:     opb.store,
			satellite: satellite,
			pieceID:   pieceID,
			trash:     false,
		}, nil
	}
	if !errs.Is(err, fs.ErrNotExist) {
		return nil, rpcstatus.NamedWrap("old-piece-backend-open-fail", rpcstatus.Internal, err)
	}

	// check if the file is in trash, if so, restore it and
	// continue serving the download request.
	tryRestoreErr := opb.store.TryRestoreTrashPiece(ctx, satellite, pieceID)
	if tryRestoreErr != nil {
		opb.monitor.VerifyDirReadableLoop.TriggerWait()

		// we want to return the original "file does not exist" error to the rpc client
		return nil, rpcstatus.NamedWrap("not-found", rpcstatus.NotFound, err)
	}

	// try to open the file again
	reader, err = opb.store.Reader(ctx, satellite, pieceID)
	if err != nil {
		return nil, rpcstatus.NamedWrap("old-piece-backend-open-fail-after-trash-restore", rpcstatus.Internal, err)
	}
	return &oldPieceReader{
		Reader:    reader,
		store:     opb.store,
		satellite: satellite,
		pieceID:   pieceID,
		trash:     true,
	}, nil
}

// StartRestore implements PieceBackend and starts a restore operation for a satellite.
func (opb *OldPieceBackend) StartRestore(ctx context.Context, satellite storj.NodeID) (err error) {
	defer mon.Task()(&ctx)(&err)

	return opb.trashChore.StartRestore(ctx, satellite)
}

type oldPieceWriter struct {
	*pieces.Writer
	store       *pieces.Store
	satelliteID storj.NodeID
	pieceID     storj.PieceID
	expiration  time.Time
}

func (o *oldPieceWriter) Commit(ctx context.Context, header *pb.PieceHeader) (err error) {
	defer mon.Task()(&ctx)(&err)

	if err := o.Writer.Commit(ctx, header); err != nil {
		return err
	}
	if !o.expiration.IsZero() {
		return o.store.SetExpiration(ctx, o.satelliteID, o.pieceID, o.expiration, o.Writer.Size())
	}
	return nil
}

type oldPieceReader struct {
	*pieces.Reader
	store     *pieces.Store
	satellite storj.NodeID
	pieceID   storj.PieceID
	trash     bool
}

func (o *oldPieceReader) Trash() bool { return o.trash }

type nohash struct {
}

func (n2 nohash) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (n2 nohash) Sum(b []byte) []byte {
	return []byte{}
}

func (n2 nohash) Reset() {
}

func (n2 nohash) Size() int {
	return 0
}

func (n2 nohash) BlockSize() int {
	return 0
}
