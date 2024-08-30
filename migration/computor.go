package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
	"strconv"
)

func MigrateComputorList(from, to *pebble.DB) error {

	allComputorsList, err := readAllComputors(from)
	if err != nil {
		return errors.Wrap(err, "reading all computors from old db")
	}

	err = writeAllComputors(to, allComputorsList)
	if err != nil {
		return errors.Wrap(err, "writing all computors to new db")
	}
	return nil
}

func readAllComputors(db *pebble.DB) ([]*protobuff.Computors, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.ComputorList},
		UpperBound: append([]byte{store.ComputorList}, []byte(strconv.FormatUint(store.UpperBoundUint, 10))...),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allComputorsList := make([]*protobuff.Computors, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var computors protobuff.Computors
		err = proto.Unmarshal(data, &computors)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing compoutors object")
		}
		allComputorsList = append(allComputorsList, &computors)
	}

	return allComputorsList, nil

}

func writeAllComputors(db *pebble.DB, allComputorsList []*protobuff.Computors) error {

	batch := db.NewBatchWithSize(len(allComputorsList))
	defer batch.Close()

	for _, computors := range allComputorsList {

		key := store.AssembleKey(store.ComputorList, computors.Epoch)

		serialized, err := proto.Marshal(computors)
		if err != nil {
			return errors.Wrap(err, "serializing computors object")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "adding computors to store batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "commiting batch for all computors")
	}

	return nil
}
