package util

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
	archivestore "github.com/qubic/go-archiver/store"
)

func ReadFullQuorumData(db *pebble.DB, tickNumber uint32) (*protobuff.QuorumTickData, error) {
	key := store.AssembleKey(store.QuorumData, tickNumber)

	value, closer, err := db.Get(key)
	if err != nil {
		return nil, errors.Wrapf(err, "reading quorum data for tick %d", tickNumber)
	}
	defer closer.Close()

	var quorumData protobuff.QuorumTickData

	err = proto.Unmarshal(value, &quorumData)
	if err != nil {
		return nil, errors.Wrapf(err, "de-serializing quorum data for tick %d", tickNumber)
	}

	return &quorumData, nil
}

func ExportLastTickQuorumDataPerEpochInterval(from, to *pebble.DB) error {

	fromStore := archivestore.NewPebbleStore(from, nil)

	processedTickIntervals, err := fromStore.GetProcessedTickIntervals(nil)
	if err != nil {
		return errors.Wrap(err, "getting processed tick intervals from old database")
	}

	for _, epochIntervals := range processedTickIntervals {

		lastTickQuorumDataPerEpochIntervals := protobuff.LastTickQuorumDataPerEpochIntervals{
			QuorumDataPerInterval: make(map[int32]*protobuff.QuorumTickData),
		}

		epoch := epochIntervals.Epoch

		for index, interval := range epochIntervals.Intervals {

			tickNumber := interval.LastProcessedTick

			fullQuorumData, err := ReadFullQuorumData(from, tickNumber)
			if err != nil {
				return errors.Wrapf(err, "reading full quorum data for tick %d", tickNumber)
			}

			lastTickQuorumDataPerEpochIntervals.QuorumDataPerInterval[int32(index)] = fullQuorumData
		}

		toStore := archivestore.NewPebbleStore(to, nil)

		err = toStore.SetLastTickQuorumDataPerEpochIntervals(epoch, &lastTickQuorumDataPerEpochIntervals)
		if err != nil {
			return errors.Wrapf(err, "saving last quorum tick data for intervals of epoch %d", epoch)
		}

	}

	return nil
}

func ImportLastTickDataPerEpochInterval(from, to *pebble.DB) error {

	fromStore := archivestore.NewPebbleStore(from, nil)
	toStore := archivestore.NewPebbleStore(to, nil)

	epochs, err := toStore.GetLastProcessedTicksPerEpoch(nil)
	if err != nil {
		return errors.Wrap(err, "getting epoch list from database")
	}
	fmt.Printf("%v\n", epochs)

	for epoch, _ := range epochs {
		lastQuorumDataPerEpochInterval, err := fromStore.GetLastTickQuorumDataListPerEpochInterval(epoch)
		if err != nil {
			return errors.Wrapf(err, "getting last quorum tick data for intervals of epoch %d", epoch)
		}

		err = toStore.SetLastTickQuorumDataPerEpochIntervals(epoch, lastQuorumDataPerEpochInterval)
		if err != nil {
			return errors.Wrapf(err, "saving last quorum tick data for intervals of epoch %d", epoch)
		}

	}

	return nil
}
