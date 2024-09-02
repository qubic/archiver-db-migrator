package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"runtime"
)

const maxArraySize = 10000

type Settings struct {
	LowerBound []byte
	UpperBound []byte
}

func MigrateData(from, to *pebble.DB, settings Settings) error {

	iter, err := from.NewIter(&pebble.IterOptions{
		LowerBound: settings.LowerBound,
		UpperBound: settings.UpperBound,
	})
	if err != nil {
		errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	counter := 0

	batch := to.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {

		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return errors.Wrap(err, "getting data from iter")
		}

		err = batch.Set(key, value, nil)
		if err != nil {
			return errors.Wrap(err, "setting data in batch")
		}
		counter++

		if counter >= maxArraySize {
			err = batch.Commit(pebble.Sync)
			if err != nil {
				return errors.Wrap(err, "committing batch")
			}

			batch.Reset()
			runtime.GC()
			counter = 0
		}

	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
