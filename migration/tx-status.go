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
	"golang.org/x/sync/errgroup"
)

func (m *Migrator) processTxStatusInParallel(txList []string, ttsV2 *protoV2.TickTransactionsStatus, batch *pebbleV2.Batch) error {
	var errorGroup errgroup.Group
	for _, tick := range txList {
		errorGroup.Go(func() error {
			txStatus, err := m.processTxStatus(tick, batch)
			if err == nil && txStatus != nil {
				m.lock.Lock() // for appending
				defer m.lock.Unlock()
				ttsV2.Transactions = append(ttsV2.Transactions, txStatus)
			}
			return err
		})
	}
	return errorGroup.Wait()
}

func (m *Migrator) processTxStatus(hash string, batch *pebbleV2.Batch) (*protoV2.TransactionStatus, error) {
	txStatusV1, err := m.oldStore.ArchiverStore.GetTransactionStatus(context.Background(), hash)
	if err != nil {
		return nil, fmt.Errorf("getting transaction status for tx %s: %w", hash, err)
	}

	txStatusV2 := protoV2.TransactionStatus{
		TxId:      txStatusV1.TxId,
		MoneyFlew: txStatusV1.MoneyFlew,
	}

	data, err := proto.Marshal(&txStatusV2)
	if err != nil {
		return nil, fmt.Errorf("marshaling transaction status v2 for tx %s: %w", hash, err)
	}

	err = batch.Set(migratorStore.AssembleKey(archiverV2Store.TransactionStatus, hash), data, nil)
	if err != nil {
		return nil, fmt.Errorf("setting transaction status for tx %s in batch: %w", hash, err)
	}

	return &txStatusV2, nil
}

func (m *Migrator) migrateTransactionsStatusList(txIdsPerTick map[uint32][]string, newStore *v2.ArchiverEpochStoreV2) error {

	bar := progressbar.Default(int64(len(txIdsPerTick)), "Migrating transactions status")

	batch := newStore.ArchiverStore.GetDB().NewBatch()
	defer batch.Close()

	counter := 0

	for tickNumber, txs := range txIdsPerTick {

		var ttsV2 protoV2.TickTransactionsStatus
		_ = bar.Add(1)

		lastIndex := len(txs) - 1
		if lastIndex < m.numWorkers {
			err := m.processTxStatusInParallel(txs, &ttsV2, batch)
			if err != nil {
				return fmt.Errorf("processing tx status in parallel: %w", err)
			}
			counter += len(txs)
		} else {
			var nextTransactions = make([]string, 0, m.numWorkers)
			for i, hash := range txs {
				nextTransactions = append(nextTransactions, hash)
				if len(nextTransactions) >= m.numWorkers || i == lastIndex {
					err := m.processTxStatusInParallel(nextTransactions, &ttsV2, batch)
					if err != nil {
						return fmt.Errorf("processing tx status in parallel: %w", err)
					}
					counter += len(nextTransactions)
					nextTransactions = make([]string, 0, m.numWorkers) // reset
				}
			}
		}

		if counter >= m.batchSize {
			err := batch.Commit(pebbleV2.Sync)
			if err != nil {
				return fmt.Errorf("committing batch while migrating transactions status list: %w", err)
			}

			batch.Reset()
			runtime.GC()
			counter = 0
		}

		data, err := proto.Marshal(&ttsV2)
		if err != nil {
			return fmt.Errorf("marshaling tick transactions status v2 for tick %d: %w", tickNumber, err)
		}
		err = batch.Set(migratorStore.AssembleKey(archiverV2Store.TickTransactionsStatus, uint64(tickNumber)), data, nil) // uint64 is not a mistake, the db code in archiver v1 and v2 used an uint64 key for some reason
		if err != nil {
			return fmt.Errorf("setting tick transactions status for tick %d in batch: %w", tickNumber, err)
		}
	}

	err := batch.Commit(pebbleV2.Sync)
	if err != nil {
		return fmt.Errorf("committing final batch while migrating transactions status list: %w", err)
	}

	return nil
}

func (m *Migrator) migrateTransactionsStatusRange(tickRange v1.TickRange, newStore *v2.ArchiverEpochStoreV2) error {

	bar := progressbar.Default(int64(tickRange.End-tickRange.Start), fmt.Sprintf("Migrating transactions status ticks %d to %d", tickRange.Start, tickRange.End))

	for tickNumber := tickRange.Start; tickNumber <= tickRange.End; tickNumber++ {

		tickTransactionsStatusV1, err := m.oldStore.ArchiverStore.GetTickTransactionsStatus(context.Background(), uint64(tickNumber))
		if err != nil {
			return fmt.Errorf("getting transactions status for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		tickTransactionsStatusV2 := protoV2.TickTransactionsStatus{}

		for _, transactionStatus := range tickTransactionsStatusV1.Transactions {
			tickTransactionsStatusV2.Transactions = append(tickTransactionsStatusV2.Transactions, &protoV2.TransactionStatus{
				TxId:      transactionStatus.TxId,
				MoneyFlew: transactionStatus.MoneyFlew,
			})
		}

		err = newStore.ArchiverStore.SetTickTransactionsStatus(context.Background(), uint64(tickNumber), &tickTransactionsStatusV2)
		if err != nil {
			return fmt.Errorf("setting transactions status for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		_ = bar.Add(1)
	}
	return nil
}
