package migration

import (
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateAllTransactionStatuses(from, to *pebble.DB) error {

	tickTransactionStatuses, err := readAllTickTransactionStatuses(from)
	if err != nil {
		return errors.Wrap(err, "reading all tick transaction statuses from old db")
	}
	err = writeAllTickTransactionStatuses(to, tickTransactionStatuses)
	if err != nil {
		return errors.Wrap(err, "writing all tick transaction statuses to new db")
	}

	transactionStatuses, err := readAllTransactionStatuses(from)
	if err != nil {
		return errors.Wrap(err, "reading all transaction statuses from old db")
	}

	err = writeAllTransactionStatuses(to, transactionStatuses)
	if err != nil {
		return errors.Wrap(err, "writing all transaction statuses to new db")
	}

	return nil
}

func readAllTickTransactionStatuses(db *pebble.DB) (map[uint32]*protobuff.TickTransactionsStatus, error) {

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.TickTransactionsStatus},
		UpperBound: store.AssembleKey(store.TickTransactionsStatus, store.UpperBoundUint),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")

	}
	defer iter.Close()

	allTickTransactionStatuses := make(map[uint32]*protobuff.TickTransactionsStatus)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var tickTransactionStatus protobuff.TickTransactionsStatus
		err = proto.Unmarshal(data, &tickTransactionStatus)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing tick transactions status object")
		}

		key := iter.Key()
		tickNumber := binary.BigEndian.Uint32(key[1:])

		allTickTransactionStatuses[tickNumber] = &tickTransactionStatus
	}

	return allTickTransactionStatuses, nil
}

func writeAllTickTransactionStatuses(db *pebble.DB, allTickTransactionStatuses map[uint32]*protobuff.TickTransactionsStatus) error {

	batch := db.NewBatch()
	defer batch.Close()

	for tickNumber, status := range allTickTransactionStatuses {
		key := store.AssembleKey(store.TickTransactionsStatus, tickNumber)
		data, err := proto.Marshal(status)
		if err != nil {
			return errors.Wrap(err, "serializing tick transactions status object")
		}

		err = batch.Set(key, data, pebble.Sync)
		if err != nil {
			return errors.Wrap(err, "setting data in batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func readAllTransactionStatuses(db *pebble.DB) ([]*protobuff.TransactionStatus, error) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.TransactionStatus},
		UpperBound: store.AssembleKey(store.TransactionStatus, store.UpperBoundTransaction),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	allTransactionStatuses := make([]*protobuff.TransactionStatus, 0)

	for iter.First(); iter.Valid(); iter.Next() {

		data, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting data from iter")
		}

		var transactionStatus protobuff.TransactionStatus
		err = proto.Unmarshal(data, &transactionStatus)
		if err != nil {
			return nil, errors.Wrap(err, "de-serializing transaction status object")
		}
		allTransactionStatuses = append(allTransactionStatuses, &transactionStatus)
	}

	return allTransactionStatuses, nil
}

func writeAllTransactionStatuses(db *pebble.DB, allTransactionStatuses []*protobuff.TransactionStatus) error {

	batch := db.NewBatchWithSize(len(allTransactionStatuses))
	defer batch.Close()

	for _, transactionStatus := range allTransactionStatuses {

		key := store.AssembleKey(store.TransactionStatus, transactionStatus.TxId)

		serialized, err := proto.Marshal(transactionStatus)
		if err != nil {
			return errors.Wrap(err, "serializing transaction status object")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "adding transaction status to store batch")
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
