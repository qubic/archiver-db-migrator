package v1

import (
	"context"
	"fmt"
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/qubic/go-archiver/store"
)

type ArchiverStoreV1 struct {
	db            *pebble.DB
	ArchiverStore *store.PebbleStore
	StoreMetadata StoreMetadata
}

func NewArchiverStoreV1(path string) (*ArchiverStoreV1, error) {
	db, err := pebble.Open(path, getPebbleOptions())
	if err != nil {
		return nil, fmt.Errorf("opening archiver v1 database: %w", err)
	}

	archiverStore := store.NewPebbleStore(db, nil)

	s := ArchiverStoreV1{
		db:            db,
		ArchiverStore: archiverStore,
	}
	err = s.loadStoreMetadata()
	if err != nil {
		return nil, fmt.Errorf("loading archiver store v1 metadata: %w", err)
	}

	return &s, nil
}

func (s *ArchiverStoreV1) loadStoreMetadata() error {

	lastProcessedTickPerEpoch, err := s.ArchiverStore.GetLastProcessedTicksPerEpoch(context.Background())
	if err != nil {
		return fmt.Errorf("getting last processed tick per epoch: %w", err)
	}

	processedTickIntervalsPerEpoch, err := s.ArchiverStore.GetProcessedTickIntervals(context.Background())
	if err != nil {
		return fmt.Errorf("getting processed tick intervals per epoch: %w", err)
	}

	var storeMetadata StoreMetadata
	storeMetadata.Epochs = make(map[uint32]EpochMetadata)

	for _, epochIntervals := range processedTickIntervalsPerEpoch {

		var epochMetadata EpochMetadata
		epochMetadata.Epoch = epochIntervals.Epoch
		epochMetadata.LastProcessedTick = lastProcessedTickPerEpoch[epochIntervals.Epoch]

		for _, interval := range epochIntervals.Intervals {
			tickRange := TickRange{
				Start: interval.InitialProcessedTick,
				End:   interval.LastProcessedTick,
			}
			epochMetadata.ProcessedTickRanges = append(epochMetadata.ProcessedTickRanges, tickRange)
		}
		storeMetadata.Epochs[epochMetadata.Epoch] = epochMetadata
	}

	s.StoreMetadata = storeMetadata
	return nil
}

func (s *ArchiverStoreV1) GetDB() *pebble.DB {
	return s.db
}

func (s *ArchiverStoreV1) Close() error {
	return s.db.Close()
}

func getPebbleOptions() *pebble.Options {
	l1Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.NoCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       268435456, // 256 MB
	}
	l2Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l1Options.TargetFileSize * 10, // 2.5 GB
	}
	l3Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l2Options.TargetFileSize * 10, // 25 GB
	}
	l4Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l3Options.TargetFileSize * 10, // 250 GB
	}

	pebbleOptions := pebble.Options{
		Levels:                   []pebble.LevelOptions{l1Options, l2Options, l3Options, l4Options},
		MaxConcurrentCompactions: func() int { return runtime.NumCPU() },
		MemTableSize:             268435456, // 256 MB
		ReadOnly:                 true,      // IMPORTANT: read only mode disables compactions which may occur when loading the large db
	}

	return &pebbleOptions
}
