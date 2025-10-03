package migration

import (
	"context"
	"fmt"
	"log"

	v1 "github.com/qubic/archiver-db-migrator/store/v1"
	v2 "github.com/qubic/archiver-db-migrator/store/v2"
)

type Migrator struct {
	oldStore            *v1.ArchiverStoreV1
	newStorePath        string
	batchSize           int
	compactAfterMigrate bool
}

func NewMigrator(oldStore *v1.ArchiverStoreV1, newStorePath string, batchSize int, compactAfterMigrate bool) *Migrator {
	return &Migrator{
		oldStore:            oldStore,
		newStorePath:        newStorePath,
		batchSize:           batchSize,
		compactAfterMigrate: compactAfterMigrate,
	}
}

func (m *Migrator) MigrateEpoch(epoch uint32) error {

	newStore, err := v2.NewArchiverEpochStoreV2(m.newStorePath, epoch)
	if err != nil {
		return fmt.Errorf("creating new epoch store v2 for epoch %d: %w", epoch, err)
	}
	defer newStore.Close()

	err = m.MigrateEpochMetadata(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating epoch metadata for epoch %d: %w", epoch, err)
	}

	err = m.MigrateEpochTicks(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating epoch ticks for epoch %d: %w", epoch, err)
	}

	if m.compactAfterMigrate {

		log.Println("Performing compaction on migrated database...")
		err = newStore.ArchiverStore.GetDB().Compact(context.Background(), []byte{0x00}, []byte{0xFF}, true)
		if err != nil {
			return fmt.Errorf("compacting new epoch store v2 for epoch %d: %w", epoch, err)
		}
	}

	return nil
}

func (m *Migrator) MigrateAllEpochs() error {
	oldStoreMetadata := m.oldStore.StoreMetadata

	for _, epochMetadata := range oldStoreMetadata.Epochs {
		err := m.MigrateEpoch(epochMetadata.Epoch)
		if err != nil {
			return fmt.Errorf("migrating epoch %d: %w", epochMetadata.Epoch, err)
		}
	}
	return nil
}

func (m *Migrator) MigrateEpochRange(start, end uint32) error {

	for epoch := start; epoch <= end; epoch++ {
		err := m.MigrateEpoch(epoch)
		if err != nil {
			return fmt.Errorf("migrating epoch %d: %w", epoch, err)
		}
	}
	return nil
}
