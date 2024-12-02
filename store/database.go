package store

import "github.com/cockroachdb/pebble"
import "github.com/pkg/errors"

func CreateDBWithDefaultOptions(path string) (*pebble.DB, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}

	return db, nil
}

func CreateDBWithZstdCompression(path string, betterCompaction bool) (*pebble.DB, error) {

	var pebbleOptions pebble.Options

	if betterCompaction {
		l1Options := pebble.LevelOptions{
			BlockRestartInterval: 16,
			BlockSize:            4096,
			BlockSizeThreshold:   90,
			Compression:          pebble.NoCompression,
			FilterPolicy:         nil,
			FilterType:           pebble.TableFilter,
			IndexBlockSize:       4096,
			TargetFileSize:       268435456, // 256 MB
		}
		l2Options := pebble.LevelOptions{
			BlockRestartInterval: 16,
			BlockSize:            4096,
			BlockSizeThreshold:   90,
			Compression:          pebble.ZstdCompression,
			FilterPolicy:         nil,
			FilterType:           pebble.TableFilter,
			IndexBlockSize:       4096,
			TargetFileSize:       l1Options.TargetFileSize * 10, // 2.5 GB
		}
		l3Options := pebble.LevelOptions{
			BlockRestartInterval: 16,
			BlockSize:            4096,
			BlockSizeThreshold:   90,
			Compression:          pebble.ZstdCompression,
			FilterPolicy:         nil,
			FilterType:           pebble.TableFilter,
			IndexBlockSize:       4096,
			TargetFileSize:       l2Options.TargetFileSize * 10, // 25 GB
		}

		l4Options := pebble.LevelOptions{
			BlockRestartInterval: 16,
			BlockSize:            4096,
			BlockSizeThreshold:   90,
			Compression:          pebble.ZstdCompression,
			FilterPolicy:         nil,
			FilterType:           pebble.TableFilter,
			IndexBlockSize:       4096,
			TargetFileSize:       l3Options.TargetFileSize * 10, // 250 GB
		}

		pebbleOptions = pebble.Options{
			Levels:                   []pebble.LevelOptions{l1Options, l2Options, l3Options, l4Options},
			MaxConcurrentCompactions: func() int { return 12 },
			MemTableSize:             268435456, // 256 MB
			EventListener:            NewPebbleEventListener(),
		}
	} else {
		levelOptions := pebble.LevelOptions{
			BlockRestartInterval: 16,
			BlockSize:            4096,
			BlockSizeThreshold:   90,
			Compression:          pebble.ZstdCompression,
			FilterPolicy:         nil,
			FilterType:           pebble.TableFilter,
			IndexBlockSize:       4096,
			TargetFileSize:       2097152,
		}

		pebbleOptions = pebble.Options{
			Levels: []pebble.LevelOptions{levelOptions},
		}
	}

	db, err := pebble.Open(path, &pebbleOptions)
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}

	return db, nil
}
