package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateIdentityTransferTransactions(from, to *pebble.DB) error {

	allIdentityTransferTransactions, err := readAllIdentityTransferTransactions(from)
	if err != nil {
		return errors.Wrap(err, "reading all identity transfer transactions from old db")
	}

	err = writeAllIdentityTransferTransactions(to, allIdentityTransferTransactions)
	if err != nil {
		return errors.Wrap(err, "writing all identity transfer transactions to new db")
	}

	return nil
}

func readAllIdentityTransferTransactions(db *pebble.DB) ([]*protobuff.TransferTransactionsPerTick, error) {

	lowerBound := []byte{store.IdentityTransferTransactions}

	upperBound := store.AssembleKey(store.IdentityTransferTransactions, store.UpperBoundIdentity)
	upperBound = binary.BigEndian.AppendUint64(upperBound, store.UpperBoundUint)

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allIdentityTransferTransactions := make([]*protobuff.TransferTransactionsPerTick, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var transferTransactions protobuff.TransferTransactionsPerTick
		err = proto.Unmarshal(data, &transferTransactions)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing transfer transactions object")
		}
		allIdentityTransferTransactions = append(allIdentityTransferTransactions, &transferTransactions)

	}

	return allIdentityTransferTransactions, nil
}

func writeAllIdentityTransferTransactions(db *pebble.DB, allIdentityTransferTransactions []*protobuff.TransferTransactionsPerTick) error {

	batch := db.NewBatch()
	defer batch.Close()

	for _, transferTransactions := range allIdentityTransferTransactions {

		key := store.AssembleKey(store.IdentityTransferTransactions, transferTransactions.Identity)
		key = binary.BigEndian.AppendUint64(key, uint64(transferTransactions.TickNumber))

		value, err := proto.Marshal(transferTransactions)
		if err != nil {
			return errors.Wrap(err, "marshalling transfer transactions")
		}

		err = batch.Set(key, value, &pebble.WriteOptions{})
		if err != nil {
			return errors.Wrap(err, "writing transfer transactions to batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
