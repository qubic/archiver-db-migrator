package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateTransactions(from, to *pebble.DB) error {

	allTransactions, err := readAllTransactions(from)
	if err != nil {
		return errors.Wrap(err, "reading all transactions from old db")
	}

	err = writeAllTransactions(to, allTransactions)
	if err != nil {
		return errors.Wrap(err, "writing all transactions to new db")
	}

	return nil
}

func readAllTransactions(db *pebble.DB) ([]*protobuff.Transaction, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.Transaction},
		UpperBound: store.AssembleKey(store.Transaction, store.UpperBoundTransaction),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allTransactions := make([]*protobuff.Transaction, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var transaction protobuff.Transaction
		err = proto.Unmarshal(data, &transaction)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing transaction object")
		}
		allTransactions = append(allTransactions, &transaction)
	}

	return allTransactions, nil
}

func writeAllTransactions(db *pebble.DB, allTransactions []*protobuff.Transaction) error {

	batch := db.NewBatchWithSize(len(allTransactions))
	defer batch.Close()

	for _, transaction := range allTransactions {

		key := store.AssembleKey(store.Transaction, transaction.TxId)

		serialized, err := proto.Marshal(transaction)
		if err != nil {
			return errors.Wrap(err, "serializing transaction object")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "adding transaction to store batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "commiting batch for all transactions")
	}

	return nil
}
