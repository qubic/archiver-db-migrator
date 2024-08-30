package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateSkippedTicksIntervals(from, to *pebble.DB) error {

	skippedTicksIntervalList, err := readAllSkippedIntervals(from)
	if err != nil {
		return errors.Wrap(err, "reading all skipped ticks intervals from old db")
	}

	err = writeSkippedTicksIntervals(to, skippedTicksIntervalList)
	if err != nil {
		return errors.Wrap(err, "writing all skipped ticks intervals to new db")
	}

	return nil
}

func readAllSkippedIntervals(db *pebble.DB) (*protobuff.SkippedTicksIntervalList, error) {

	key := []byte{store.SkippedTicksInterval}

	value, closer, err := db.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "getting data from db")
	}
	defer closer.Close()

	var skippedTicksIntervalList protobuff.SkippedTicksIntervalList
	err = proto.Unmarshal(value, &skippedTicksIntervalList)
	if err != nil {
		return nil, errors.Wrap(err, "de-serializing skipped ticks interval list")
	}

	return &skippedTicksIntervalList, nil
}

func writeSkippedTicksIntervals(db *pebble.DB, skippedTicksIntervalList *protobuff.SkippedTicksIntervalList) error {

	key := []byte{store.SkippedTicksInterval}

	value, err := proto.Marshal(skippedTicksIntervalList)
	if err != nil {
		return errors.Wrap(err, "serializing skipped ticks interval list")
	}

	err = db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "writing skipped ticks interval list")
	}

	return nil

}
