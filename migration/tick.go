package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"strconv"
)
import "github.com/qubic/go-archiver/protobuff"

func MigrateTickData(from, to *pebble.DB) error {

	allTickData, err := readAllTickData(from)
	if err != nil {
		return errors.Wrap(err, "reading all tick data from old db")
	}

	err = writeAllTickData(to, allTickData)
	if err != nil {
		return errors.Wrap(err, "writing all tick data to new db")
	}

	return nil
}

func readAllTickData(db *pebble.DB) ([]*protobuff.TickData, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.TickData},
		UpperBound: append([]byte{store.TickData}, []byte(strconv.FormatUint(store.UpperBoundUint, 10))...),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allTickData := make([]*protobuff.TickData, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var tickData protobuff.TickData
		err = proto.Unmarshal(data, &tickData)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing tick data object")
		}
		allTickData = append(allTickData, &tickData)
	}

	return allTickData, nil
}

func writeAllTickData(db *pebble.DB, allTickData []*protobuff.TickData) error {

	batch := db.NewBatchWithSize(len(allTickData))
	defer batch.Close()

	for _, tickData := range allTickData {

		key := store.AssembleKey(store.TickData, tickData.TickNumber)

		serialized, err := proto.Marshal(tickData)
		if err != nil {
			return errors.Wrap(err, "serializing tick data object")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "adding tick data to store batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "commiting batch for all tick data")
	}

	return nil
}
