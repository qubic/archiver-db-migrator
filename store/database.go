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

func CreateDBWithZstdCompression(path string) (*pebble.DB, error) {

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

	pebbleOptions := pebble.Options{
		Levels: []pebble.LevelOptions{levelOptions},
	}

	db, err := pebble.Open(path, &pebbleOptions)
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}

	return db, nil
}
