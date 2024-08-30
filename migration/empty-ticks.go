package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
)

func MigrateAllEmptyTicksPerEpoch(from, to *pebble.DB) error {

	allEmptyTicksPerEpoch, err := readAllEmptyTicksPerEpoch(from)
	if err != nil {
		return errors.Wrap(err, "reading all empty ticks per epoch from old db")
	}

	err = writeAllEmptyTicksPerEpoch(to, allEmptyTicksPerEpoch)
	if err != nil {
		return errors.Wrap(err, "writing all empty ticks per epoch to new db")
	}

	return nil

}

func readAllEmptyTicksPerEpoch(db *pebble.DB) (map[uint32]uint32, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.EmptyTicksPerEpoch},
		UpperBound: store.AssembleKey(store.EmptyTicksPerEpoch, store.UpperBoundUint),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allEmptyTicksPerEpoch := make(map[uint32]uint32)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		emptyTicks := binary.LittleEndian.Uint32(data)

		key := iter.Key()
		epoch := uint32(key[1])

		allEmptyTicksPerEpoch[epoch] = emptyTicks
	}

	return allEmptyTicksPerEpoch, nil
}

func writeAllEmptyTicksPerEpoch(db *pebble.DB, allEmptyTicksPerEpoch map[uint32]uint32) error {

	batch := db.NewBatch()
	defer batch.Close()

	for epoch, emptyTicks := range allEmptyTicksPerEpoch {
		key := store.AssembleKey(store.EmptyTicksPerEpoch, epoch)
		value := make([]byte, 4)
		binary.LittleEndian.PutUint32(value, emptyTicks)

		err := batch.Set(key, value, nil)
		if err != nil {
			return errors.Wrap(err, "adding empty ticks to batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
