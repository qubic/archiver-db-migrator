package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/ardanlabs/conf/v3"
	"github.com/qubic/archiver-db-migrator/migration"
	v1 "github.com/qubic/archiver-db-migrator/store/v1"
)

const confPrefix = "ARCHIVER_MIGRATOR_V2"

func main() {
	err := run()
	if err != nil {
		log.Printf("error while running migrator: %v", err)
	}
}

func run() error {

	var config struct {
		Database struct {
			PathOld             string `conf:"default:storage/old"`
			PathNew             string `conf:"default:storage/new"`
			CompactAfterMigrate bool   `conf:"default:false"`
		}
		BatchSize int `conf:"default:10000"`
		Migrate   struct {
			All        bool   `conf:"default:false"`
			Epoch      uint32 `conf:"default:0"`
			EpochRange struct {
				Start uint32 `conf:"default:0"`
				End   uint32 `conf:"default:0"`
			}
		}
	}

	help, err := conf.Parse(confPrefix, &config)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			log.Println(help)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	oldStore, err := v1.NewArchiverStoreV1(config.Database.PathOld)
	if err != nil {
		return fmt.Errorf("opening old archiver store v1: %w", err)
	}

	defer oldStore.Close()

	migrator := migration.NewMigrator(oldStore, config.Database.PathNew, config.BatchSize, config.Database.CompactAfterMigrate)

	if config.Migrate.All {
		log.Println("Starting migration of all epochs")

		err := migrator.MigrateAllEpochs()
		if err != nil {
			return fmt.Errorf("migrating all epochs: %w", err)
		}
		return nil
	} else if config.Migrate.Epoch != 0 {
		log.Printf("Starting migration of epoch %d", config.Migrate.Epoch)

		err := migrator.MigrateEpoch(config.Migrate.Epoch)
		if err != nil {
			return fmt.Errorf("migrating epoch %d: %w", config.Migrate.Epoch, err)
		}
		return nil
	} else if config.Migrate.EpochRange.Start != 0 && config.Migrate.EpochRange.End != 0 {
		log.Printf("Starting migration of epoch range %d to %d", config.Migrate.EpochRange.Start, config.Migrate.EpochRange.End)

		err := migrator.MigrateEpochRange(config.Migrate.EpochRange.Start, config.Migrate.EpochRange.End)
		if err != nil {
			return fmt.Errorf("migrating epoch range %d to %d: %w", config.Migrate.EpochRange.Start, config.Migrate.EpochRange.End, err)
		}
		return nil
	} else {
		oldStore.StoreMetadata.PrintStoreMetadata()
	}

	return nil
}
