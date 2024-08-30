package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
	"strconv"
)

func MigrateLastProcessedTickPerEpoch(from, to *pebble.DB) error {

	allLastProcessedTicksPerEpoch, err := readAllLastProcessedTicksPerEpoch(from)
	if err != nil {
		return errors.Wrap(err, "reading all last processed ticks from old db")
	}
	err = writeAllLastProcessedTicksPerEpoch(to, allLastProcessedTicksPerEpoch)
	if err != nil {
		return errors.Wrap(err, "writing all last processed ticks to new db")
	}

	return nil
}

func readAllLastProcessedTicksPerEpoch(db *pebble.DB) (map[uint32]*protobuff.LastProcessedTick, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.LastProcessedTickPerEpoch},
		UpperBound: append([]byte{store.LastProcessedTickPerEpoch}, []byte(strconv.FormatUint(store.UpperBoundUint, 10))...),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allLastProcessedTicksPerEpoch := make(map[uint32]*protobuff.LastProcessedTick)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var lastProcessedTick protobuff.LastProcessedTick
		err = proto.Unmarshal(data, &lastProcessedTick)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing last processed tick object")
		}

		key := iter.Key()
		epoch := binary.BigEndian.Uint32(key[1:])

		allLastProcessedTicksPerEpoch[epoch] = &lastProcessedTick
	}

	return allLastProcessedTicksPerEpoch, nil
}

func writeAllLastProcessedTicksPerEpoch(db *pebble.DB, allLastProcessedTicksPerEpoch map[uint32]*protobuff.LastProcessedTick) error {
	batch := db.NewBatchWithSize(len(allLastProcessedTicksPerEpoch))
	defer batch.Close()

	for epoch, lastProcessedTick := range allLastProcessedTicksPerEpoch {

		key := store.AssembleKey(store.LastProcessedTickPerEpoch, epoch)

		serialized, err := proto.Marshal(lastProcessedTick)
		if err != nil {
			return errors.Wrap(err, "serializing last processed tick per epoch object")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "adding last processed tick per epoch to store batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "commiting batch for all processed ticks per epoch data")
	}

	return nil
}
