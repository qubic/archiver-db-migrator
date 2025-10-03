package migration

import (
	"fmt"
	"log"

	v1 "github.com/qubic/archiver-db-migrator/store/v1"
	v2 "github.com/qubic/archiver-db-migrator/store/v2"
)

func (m *Migrator) MigrateEpochTicks(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	epochMetadata, exists := m.oldStore.StoreMetadata.Epochs[epoch]
	if !exists {
		return fmt.Errorf("epoch %d metadata not found", epoch)
	}

	log.Printf("Migrating tick related data for epoch %d\n", epoch)

	log.Println("Migrating tick data...")
	err := m.MigrateTickData(epochMetadata, newStore)
	if err != nil {
		return fmt.Errorf("migrating tick data for epoch %d: %w", epoch, err)
	}

	log.Println("Migrating quorum data...")
	err = m.MigrateQuorumData(epochMetadata, newStore)
	if err != nil {
		return fmt.Errorf("migrating quorum data for epoch %d: %w", epoch, err)
	}

	/*log.Println("Migrating transactions...")
	err = m.MigrateTransactions(epochMetadata, newStore)
	if err != nil {
		return fmt.Errorf("migrating transactions for epoch %d: %w", epoch, err)
	}*/

	/*log.Println("Migrating transactions status...")
	err = m.MigrateTransactionsStatus(epochMetadata, newStore)
	if err != nil {
		return fmt.Errorf("migrating transactions status for epoch %d: %w", epoch, err)
	}*/

	log.Println("Done.")
	return nil
}

func (m *Migrator) MigrateTickData(epochMetadata v1.EpochMetadata, newStore *v2.ArchiverEpochStoreV2) error {

	for _, tickRange := range epochMetadata.ProcessedTickRanges {
		txIds, txCount, err := m.migrateTickDataRange(tickRange, newStore)
		if err != nil {
			return fmt.Errorf("migrating tick data range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}

		err = m.migrateTransactionsList(txIds, txCount, newStore)
		if err != nil {
			return fmt.Errorf("migrating transactions list for tick range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}

		err = m.migrateTransactionsStatusList(txIds, newStore)
		if err != nil {
			return fmt.Errorf("migrating transactions status list for tick range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}

	}
	return nil
}

func (m *Migrator) MigrateQuorumData(epochMetadata v1.EpochMetadata, newStore *v2.ArchiverEpochStoreV2) error {
	for _, tickRange := range epochMetadata.ProcessedTickRanges {
		err := m.migrateQuorumDataRange(tickRange, newStore)
		if err != nil {
			return fmt.Errorf("migrating quorum data range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}
	}
	return nil
}

/*func (m *Migrator) MigrateTransactions(epochMetadata v1.EpochMetadata, newStore *v2.ArchiverEpochStoreV2) error {
	for _, tickRange := range epochMetadata.ProcessedTickRanges {
		err := m.migrateTransactionsRange(tickRange, newStore)
		if err != nil {
			return fmt.Errorf("migrating transactions range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}
	}
	return nil
}*/

/*func (m *Migrator) MigrateTransactionsStatus(epochMetadata v1.EpochMetadata, newStore *v2.ArchiverEpochStoreV2) error {
	for _, tickRange := range epochMetadata.ProcessedTickRanges {
		err := m.migrateTransactionsStatusRange(tickRange, newStore)
		if err != nil {
			return fmt.Errorf("migrating transactions status range %v for epoch %d: %w", tickRange, epochMetadata.Epoch, err)
		}
	}
	return nil
}*/
