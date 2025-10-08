package migration

import (
	"context"
	"fmt"
	"runtime"

	pebbleV2 "github.com/cockroachdb/pebble/v2"
	"github.com/golang/protobuf/proto"
	migratorStore "github.com/qubic/archiver-db-migrator/store"
	v1 "github.com/qubic/archiver-db-migrator/store/v1"
	v2 "github.com/qubic/archiver-db-migrator/store/v2"
	archiverV2Store "github.com/qubic/go-archiver-v2/db"
	protoV2 "github.com/qubic/go-archiver-v2/protobuf"
	"github.com/schollz/progressbar/v3"
)

func (m *Migrator) migrateTransactionsList(txIdsPerTick map[uint32][]string, txCount int, newStore *v2.ArchiverEpochStoreV2) error {

	bar := progressbar.Default(int64(txCount), "Migrating transactions list")

	batch := newStore.ArchiverStore.GetDB().NewBatch()
	defer batch.Close()

	counter := 0

	for _, txList := range txIdsPerTick {

		for _, txId := range txList {
			_ = bar.Add(1)

			txV1, err := m.oldStore.ArchiverStore.GetTransaction(context.Background(), txId)
			if err != nil {
				return fmt.Errorf("getting transaction %s: %w", txId, err)
			}

			txV2 := protoV2.Transaction{
				SourceId:     txV1.SourceId,
				DestId:       txV1.DestId,
				Amount:       txV1.Amount,
				TickNumber:   txV1.TickNumber,
				InputType:    txV1.InputType,
				InputSize:    txV1.InputSize,
				InputHex:     txV1.InputHex,
				SignatureHex: txV1.SignatureHex,
				TxId:         txV1.TxId,
			}

			data, err := proto.Marshal(&txV2)
			if err != nil {
				return fmt.Errorf("marshaling transaction v2 %s: %w", txId, err)
			}

			err = batch.Set(migratorStore.AssembleKey(archiverV2Store.Transaction, txId), data, nil)
			if err != nil {
				return fmt.Errorf("setting transaction %s in batch: %w", txId, err)
			}
			counter++

			if counter >= m.batchSize {
				err = batch.Commit(pebbleV2.Sync)
				if err != nil {
					return fmt.Errorf("committing batch: %w", err)
				}

				batch.Reset()
				runtime.GC()
				counter = 0
			}
		}
	}

	err := batch.Commit(pebbleV2.Sync)
	if err != nil {
		return fmt.Errorf("committing final batch: %w", err)
	}
	return nil
}

func (m *Migrator) migrateTransactionsRange(tickRange v1.TickRange, newStore *v2.ArchiverEpochStoreV2) error {

	bar := progressbar.Default(int64(tickRange.End-tickRange.Start), fmt.Sprintf("Migrating transactions ticks %d to %d", tickRange.Start, tickRange.End))

	for tickNumber := tickRange.Start; tickNumber <= tickRange.End; tickNumber++ {

		tickTransactionsV1, err := m.oldStore.ArchiverStore.GetTickTransactions(context.Background(), tickNumber)
		if err != nil {
			return fmt.Errorf("getting transactions for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		var tickTransactionsV2 []*protoV2.Transaction

		for _, transaction := range tickTransactionsV1 {
			tickTransactionsV2 = append(tickTransactionsV2, &protoV2.Transaction{
				SourceId:     transaction.SourceId,
				DestId:       transaction.DestId,
				Amount:       transaction.Amount,
				TickNumber:   transaction.TickNumber,
				InputType:    transaction.InputType,
				InputSize:    transaction.InputSize,
				InputHex:     transaction.InputHex,
				SignatureHex: transaction.SignatureHex,
				TxId:         transaction.TxId,
			})
		}

		err = newStore.ArchiverStore.SetTransactions(context.Background(), tickTransactionsV2)
		if err != nil {
			return fmt.Errorf("setting transactions for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		_ = bar.Add(1)

	}

	return nil
}
