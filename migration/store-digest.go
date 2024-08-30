package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
)

func MigrateAllStoreDigests(from, to *pebble.DB) error {

	allStoreDigests, err := readAllStoreDigests(from)
	if err != nil {
		return errors.Wrap(err, "reading all store digests from old db")
	}

	err = writeAllStoreDigests(to, allStoreDigests)
	if err != nil {
		return errors.Wrap(err, "writing all store digests to new db")
	}

	return nil
}

func readAllStoreDigests(db *pebble.DB) (map[uint32][]byte, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.StoreDigest},
		UpperBound: store.AssembleKey(store.StoreDigest, store.UpperBoundUint),
	})
	if err != nil {

		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allStoreDigests := make(map[uint32][]byte)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		key := iter.Key()
		tickNumber := binary.BigEndian.Uint32(key[1:])

		allStoreDigests[tickNumber] = data
	}

	return nil, nil
}

func writeAllStoreDigests(db *pebble.DB, allStoreDigests map[uint32][]byte) error {

	batch := db.NewBatch()
	defer batch.Close()

	for tickNumber, digest := range allStoreDigests {
		key := store.AssembleKey(store.StoreDigest, tickNumber)
		err := batch.Set(key, digest, &pebble.WriteOptions{})
		if err != nil {
			return errors.Wrap(err, "setting key-value pair in batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
