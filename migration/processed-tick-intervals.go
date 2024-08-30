package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateProcessedTickIntervals(from, to *pebble.DB) error {

	allProcessedTickIntervals, err := readAllProcessedTickIntervals(from)
	if err != nil {
		return errors.Wrap(err, "reading all processed tick intervals from old db")
	}

	err = writeAllProcessedTickIntervals(to, allProcessedTickIntervals)
	if err != nil {
		return errors.Wrap(err, "writing all processed tick intervals to new db")
	}

	return nil
}

func readAllProcessedTickIntervals(db *pebble.DB) ([]*protobuff.ProcessedTickIntervalsPerEpoch, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.ProcessedTickIntervals},
		UpperBound: store.AssembleKey(store.ProcessedTickIntervals, store.UpperBoundUint),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allProcessedTickIntervalsPerEpoch := make([]*protobuff.ProcessedTickIntervalsPerEpoch, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var processedTickInterval protobuff.ProcessedTickIntervalsPerEpoch
		err = proto.Unmarshal(data, &processedTickInterval)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing tick data object")
		}
		allProcessedTickIntervalsPerEpoch = append(allProcessedTickIntervalsPerEpoch, &processedTickInterval)
	}

	return allProcessedTickIntervalsPerEpoch, nil
}

func writeAllProcessedTickIntervals(db *pebble.DB, allProcessedTickIntervals []*protobuff.ProcessedTickIntervalsPerEpoch) error {

	batch := db.NewBatch()
	defer batch.Close()

	for _, processedTickInterval := range allProcessedTickIntervals {
		key := store.AssembleKey(store.ProcessedTickIntervals, processedTickInterval.Epoch)
		value, err := proto.Marshal(processedTickInterval)
		if err != nil {
			return errors.Wrap(err, "serializing processed tick interval")
		}

		err = batch.Set(key, value, pebble.Sync)
		if err != nil {
			return errors.Wrap(err, "writing processed tick interval")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
