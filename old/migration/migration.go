package migration

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
)

func MigrateTickData(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.TickData},
		UpperBound: store.AssembleKey(store.TickData, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating tick data")
	}
	return nil
}

func MigrateQuorumData(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.QuorumData},
		UpperBound: store.AssembleKey(store.QuorumData, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating quorum data")
	}
	return nil
}

func MigrateQuorumDataV2(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.QuorumData},
		UpperBound: store.AssembleKey(store.QuorumData, store.UpperBoundUint),
	}

	err := migrateQuorumDataV2(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating quorum data")
	}
	return nil
}

func MigrateComputorList(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.ComputorList},
		UpperBound: store.AssembleKey(store.ComputorList, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating computor list")
	}
	return nil
}

func MigrateTransactions(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.Transaction},
		UpperBound: store.AssembleKey(store.Transaction, store.UpperBoundTransaction),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating transactions")
	}
	return nil
}

func MigrateLastProcessedTick(from, to *pebble.DB) error {
	key := []byte{store.LastProcessedTick}

	value, closer, err := from.Get(key)
	if err != nil {
		return errors.Wrap(err, "getting last processed tick from db")
	}
	defer closer.Close()

	err = to.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "writing last processed tick to new db")
	}

	return nil
}

func MigrateLastProcessedTicksPerEpoch(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.LastProcessedTickPerEpoch},
		UpperBound: store.AssembleKey(store.LastProcessedTickPerEpoch, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating last processed ticks per epoch")
	}
	return nil
}

func MigrateSkippedTicksIntervals(from, to *pebble.DB) error {
	key := []byte{store.SkippedTicksInterval}

	value, closer, err := from.Get(key)
	if err != nil {
		return errors.Wrap(err, "getting skipped ticks intervals from db")
	}
	defer closer.Close()

	err = to.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "writing skipped ticks intervals to new db")
	}

	return nil
}

func MigrateIdentityTransferTransactions(from, to *pebble.DB) error {
	upperBound := store.AssembleKey(store.IdentityTransferTransactions, store.UpperBoundIdentity)
	upperBound = binary.BigEndian.AppendUint64(upperBound, store.UpperBoundUint)

	settings := Settings{
		LowerBound: []byte{store.IdentityTransferTransactions},
		UpperBound: upperBound,
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating identity transfer transactions")
	}

	return nil
}

func MigrateChainDigest(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.ChainDigest},
		UpperBound: store.AssembleKey(store.ChainDigest, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating chain digest")
	}

	return nil
}

func MigrateProcessedTickIntervals(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.ProcessedTickIntervals},
		UpperBound: store.AssembleKey(store.ProcessedTickIntervals, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating processed tick intervals")
	}

	return nil
}

func MigrateTickTransactionsStatus(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.TickTransactionsStatus},
		UpperBound: store.AssembleKey(store.TickTransactionsStatus, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating tick transactions status")
	}

	return nil
}

func MigrateStoreDigest(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.StoreDigest},
		UpperBound: store.AssembleKey(store.StoreDigest, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating store digest")
	}

	return nil
}

func MigrateEmptyTicksPerEpoch(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.EmptyTicksPerEpoch},
		UpperBound: store.AssembleKey(store.EmptyTicksPerEpoch, store.UpperBoundUint),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating empty ticks per epoch")
	}

	return nil
}

func MigrateTransactionStatus(from, to *pebble.DB) error {
	settings := Settings{
		LowerBound: []byte{store.TransactionStatus},
		UpperBound: store.AssembleKey(store.TransactionStatus, store.UpperBoundTransaction),
	}

	err := MigrateData(from, to, settings)
	if err != nil {
		return errors.Wrap(err, "migrating transaction statuses")
	}
	return nil
}
