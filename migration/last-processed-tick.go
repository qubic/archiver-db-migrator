package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
)

func MigrateLastProcessedTick(from, to *pebble.DB) error {

	lastProcessedTick, err := readLastProcessedTick(from)
	if err != nil {
		return errors.Wrap(err, "reading last processed tick")
	}

	err = writeLastProcessedTick(to, lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "writing last processed tick")
	}

	return nil
}

func readLastProcessedTick(db *pebble.DB) (*protobuff.LastProcessedTick, error) {

	key := []byte{store.LastProcessedTick}

	value, closer, err := db.Get(key)
	if err != nil {
		return nil, errors.Wrap(err, "getting data from db")
	}
	defer closer.Close()

	var lastProcessedTick protobuff.LastProcessedTick
	err = proto.Unmarshal(value, &lastProcessedTick)
	if err != nil {
		return nil, errors.Wrap(err, "de-serializing last processed tick")
	}

	return &lastProcessedTick, nil
}

func writeLastProcessedTick(db *pebble.DB, lastProcessedTick *protobuff.LastProcessedTick) error {

	key := []byte{store.LastProcessedTick}

	value, err := proto.Marshal(lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "serializing last processed tick")
	}

	err = db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "writing last processed tick")
	}

	return nil

}
