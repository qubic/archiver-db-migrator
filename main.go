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
			TransactionStatus            bool `conf:"default:true"`
			StoreDigest                  bool `conf:"default:true"`
			EmptyTicksPerEpoch           bool `conf:"default:true"`
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

	println("")
	oldDB, err := createDBFromConfig(config.Database.OldPath, config.Database.OldCompression)
	if err != nil {
		return errors.Wrap(err, "creating old db")
	}
	defer oldDB.Close()

	newDB, err := createDBFromConfig(config.Database.NewPath, config.Database.NewCompression)
	if err != nil {
		return errors.Wrap(err, "creating new db")
	}
	defer oldDB.Close()

	if config.Migration.TickData {
		err = migration.MigrateTickData(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating tick data")
		}
	}

	if config.Migration.QuorumData {
		err = migration.MigrateQuorumData(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating quorum data")
		}
	}

	if config.Migration.ComputorList {
		err = migration.MigrateComputorList(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating computor list")
		}
	}

	if config.Migration.Transactions {
		err = migration.MigrateTransactions(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating transactions")
		}
	}

	if config.Migration.LastProcessedTick {
		err = migration.MigrateLastProcessedTick(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating last processed tick")
		}
	}

	if config.Migration.LastProcessedTick {
		err = migration.MigrateLastProcessedTick(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating last processed tick")
		}
	}

	if config.Migration.LastProcessedTickPerEpoch {
		err = migration.MigrateLastProcessedTickPerEpoch(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating last processed tick per epoch")
		}
	}

	if config.Migration.SkippedTicksInterval {
		err = migration.MigrateSkippedTicksIntervals(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating skipped ticks interval")
		}
	}

	if config.Migration.IdentityTransferTransactions {
		err = migration.MigrateIdentityTransferTransactions(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating identity transfer transactions")
		}
	}

	if config.Migration.ChainDigest {
		err = migration.MigrateAllChainDigests(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating chain digest")
		}
	}

	if config.Migration.ProcessedTickIntervals {
		err = migration.MigrateProcessedTickIntervals(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating processed tick intervals")
		}
	}

	if config.Migration.TransactionStatus {
		err = migration.MigrateAllTransactionStatuses(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating transactions status")
		}
	}

	if config.Migration.StoreDigest {
		err = migration.MigrateAllStoreDigests(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating store digest")
		}
	}

	if config.Migration.EmptyTicksPerEpoch {
		err = migration.MigrateAllEmptyTicksPerEpoch(oldDB, newDB)
		if err != nil {
			return errors.Wrap(err, "migrating empty ticks per epoch")
		}
	}

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
