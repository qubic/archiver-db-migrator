package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
)

func MigrateAllChainDigests(from, to *pebble.DB) error {

	allChainDigests, err := readAllChainDigests(from)
	if err != nil {
		return errors.Wrap(err, "reading all chain digests from old db")
	}

	err = writeAllChainDigests(to, allChainDigests)
	if err != nil {
		return errors.Wrap(err, "writing all chain digests to new db")
	}

	return nil
}

func readAllChainDigests(db *pebble.DB) (map[uint32][]byte, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.ChainDigest},
		UpperBound: store.AssembleKey(store.ChainDigest, store.UpperBoundUint),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allChainDigests := make(map[uint32][]byte)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		key := iter.Key()
		tickNumber := binary.BigEndian.Uint32(key[1:])

		allChainDigests[tickNumber] = data
	}

	return allChainDigests, nil
}

func writeAllChainDigests(db *pebble.DB, allChainDigests map[uint32][]byte) error {

	batch := db.NewBatch()
	defer batch.Close()

	for tickNumber, digest := range allChainDigests {
		key := store.AssembleKey(store.ChainDigest, tickNumber)
		err := batch.Set(key, digest, nil)
		if err != nil {
			return errors.Wrap(err, "adding chain digest to batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
