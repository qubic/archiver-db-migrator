package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/migration"
	"github.com/qubic/archiver-db-migrator/store"
	"log"
	"os"
)

const prefix = "QUBIC_ARCHIVER_DB_MIGRATOR"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	var config struct {
		Migration struct {
			TickData                     bool `conf:"default:true"`
			QuorumData                   bool `conf:"default:true"`
			ComputorList                 bool `conf:"default:true"`
			Transactions                 bool `conf:"default:true"`
			LastProcessedTick            bool `conf:"default:true"`
			LastProcessedTickPerEpoch    bool `conf:"default:true"`
			SkippedTicksInterval         bool `conf:"default:true"`
			IdentityTransferTransactions bool `conf:"default:true"`
			ChainDigest                  bool `conf:"default:true"`
			ProcessedTickIntervals       bool `conf:"default:true"`
			TickTransactionStatus        bool `conf:"default:true"`
			StoreDigest                  bool `conf:"default:true"`
			EmptyTicksPerEpoch           bool `conf:"default:true"`
		}
		Options struct {
			MigrateQuorumDataToV2 bool `conf:"default:true"`
		}
		Database struct {
			OldPath string `conf:"default:./storage/old"`
			//OldPath        string `conf:"default:/home/linckode/data/Projects/qubic/DB/storage/old"`
			OldCompression string `conf:"default:Snappy"`

			NewPath        string `conf:"default:./storage/new/zstd"`
			NewCompression string `conf:"default:Zstd"`
		}
	}

	if err := conf.Parse(os.Args[1:], prefix, &config); err != nil {
		switch err {
		case conf.ErrHelpWanted:
			usage, err := conf.Usage(prefix, &config)
			if err != nil {
				return errors.Wrap(err, "generating config usage")
			}
			fmt.Println(usage)
			return nil
		case conf.ErrVersionWanted:
			version, err := conf.VersionString(prefix, &config)
			if err != nil {
				return errors.Wrap(err, "generating config version")
			}
			fmt.Println(version)
			return nil
		}
		return errors.Wrap(err, "parsing config")
	}

	out, err := conf.String(&config)
	if err != nil {
		return errors.Wrap(err, "generating output for config")
	}
	log.Printf("main: Config :\n%v\n", out)

	println("Migrator started")

	oldDB, err := createDBFromConfig(config.Database.OldPath, config.Database.OldCompression)
	if err != nil {
		return errors.Wrap(err, "creating old db")
	}
	defer oldDB.Close()

	newDB, err := createDBFromConfig(config.Database.NewPath, config.Database.NewCompression)
	if err != nil {
		return errors.Wrap(err, "creating new db")
	}
	defer newDB.Close()

	if config.Migration.TickData {
		println("Migrating tick data...")

		err = migration.MigrateTickData(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating tick data")
		}
	}

	if config.Migration.QuorumData {
		println("Migrating quorum data...")

		if !config.Options.MigrateQuorumDataToV2 {
			err = migration.MigrateQuorumData(oldDB, newDB)
			if err != nil {
				return errors.Wrap(err, "migrating quorum data")
			}
		}
		err = migration.MigrateQuorumDataV2(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating quorum data to v2")
		}

	}

	if config.Migration.ComputorList {
		println("Migrating computor list...")

		err = migration.MigrateComputorList(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating computor list")
		}
	}

	if config.Migration.Transactions {
		println("Migrating transactions...")

		err = migration.MigrateTransactions(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating transactions")
		}
	}

	if config.Migration.LastProcessedTick {
		println("Migrating last processed tick...")

		err = migration.MigrateLastProcessedTick(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating last processed tick")
		}
	}

	if config.Migration.LastProcessedTickPerEpoch {
		println("Migrating last processed tick per epoch...")

		err = migration.MigrateLastProcessedTicksPerEpoch(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating last processed tick per epoch")
		}
	}

	if config.Migration.SkippedTicksInterval {
		println("Migrating skipped ticks interval...")

		err = migration.MigrateSkippedTicksIntervals(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating skipped ticks interval")
		}
	}

	if config.Migration.IdentityTransferTransactions {
		println("Migrating identity transfer transactions...")

		err = migration.MigrateIdentityTransferTransactions(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating identity transfer transactions")
		}
	}

	if config.Migration.ChainDigest {
		println("Migrating chain digest...")

		err = migration.MigrateChainDigest(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating chain digest")
		}
	}

	if config.Migration.ProcessedTickIntervals {
		println("Migrating processed tick intervals...")

		err = migration.MigrateProcessedTickIntervals(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating processed tick intervals")
		}
	}

	if config.Migration.TickTransactionStatus {
		println("Migrating transaction status...")

		err = migration.MigrateTickTransactionsStatus(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating transaction status")
		}
	}

	if config.Migration.StoreDigest {
		println("Migrating store digest...")

		err = migration.MigrateStoreDigest(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating store digest")
		}
	}

	if config.Migration.EmptyTicksPerEpoch {
		println("Migrating empty ticks per epoch...")

		err = migration.MigrateEmptyTicksPerEpoch(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating empty ticks per epoch")
		}
	}

	println("Migration done.")
	return nil
}

func createDBFromConfig(path, compressionType string) (*pebble.DB, error) {

	switch compressionType {

	case "Snappy":
		return store.CreateDBWithDefaultOptions(path)

	case "Zstd":
		return store.CreateDBWithZstdCompression(path)

	default:
		return nil, errors.New("unknown compression type")
	}
}
