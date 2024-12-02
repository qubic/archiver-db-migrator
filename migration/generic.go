package migration

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
	archivestore "github.com/qubic/go-archiver/store"
	"runtime"
)

const maxArraySize = 20000

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
		return errors.Wrap(err, "creating iter")
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

func migrateQuorumDataV2(from, to *pebble.DB, settings Settings) error {

	iter, err := from.NewIter(&pebble.IterOptions{
		LowerBound: settings.LowerBound,
		UpperBound: settings.UpperBound,
	})
	if err != nil {
		return errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	counter := 0

	pebbleStore := archivestore.NewPebbleStore(from, nil)

	processedIntervals, err := pebbleStore.GetProcessedTickIntervals(nil)
	if err != nil {
		return errors.Wrap(err, "reading epoch intervals from old database")
	}

	intervalsPerEpoch := make(map[uint32][]*protobuff.ProcessedTickInterval)

	for _, intervals := range processedIntervals {
		intervalsPerEpoch[intervals.Epoch] = intervals.Intervals
	}

	lastQuorumDataPerEpochIntervals := make(map[uint32]map[int32]*protobuff.QuorumTickData)

	batch := to.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {

		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return errors.Wrap(err, "getting data from iter")
		}

		var qtd protobuff.QuorumTickData
		err = proto.Unmarshal(value, &qtd)
		if err != nil {
			return errors.Wrap(err, "unmarshalling quorum tick v1 data")
		}

		epoch := qtd.QuorumTickStructure.Epoch
		tickNumber := qtd.QuorumTickStructure.TickNumber

		epochIntervals := intervalsPerEpoch[epoch]

		intervalIndex := findIntervalIndexForTick(tickNumber, epochIntervals)
		if intervalIndex == -1 {
			return errors.New(fmt.Sprintf("could not find which interval tick %d belongs to", tickNumber))
		}

		if lastQuorumDataPerEpochIntervals[epoch][int32(intervalIndex)] == nil || tickNumber > lastQuorumDataPerEpochIntervals[epoch][int32(intervalIndex)].QuorumTickStructure.TickNumber {
			lastQuorumDataPerEpochIntervals[epoch][int32(intervalIndex)] = &qtd
		}

		qtdV2 := protobuff.QuorumTickDataStored{
			QuorumTickStructure:   qtd.QuorumTickStructure,
			QuorumDiffPerComputor: make(map[uint32]*protobuff.QuorumDiffStored),
		}

		for id, diff := range qtd.QuorumDiffPerComputor {
			qtdV2.QuorumDiffPerComputor[id] = &protobuff.QuorumDiffStored{
				ExpectedNextTickTxDigestHex: diff.ExpectedNextTickTxDigestHex,
				SignatureHex:                diff.SignatureHex,
			}
		}

		marshalled, err := proto.Marshal(&qtdV2)
		if err != nil {
			return errors.Wrap(err, "marshalling quorum tick data v2")
		}

		err = batch.Set(key, marshalled, nil)
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

	for epoch, intervalMap := range lastQuorumDataPerEpochIntervals {

		lastTickQuorumDataPerEpochIntervals := protobuff.LastTickQuorumDataPerEpochIntervals{
			QuorumDataPerInterval: intervalMap,
		}

		key := store.AssembleKey(store.LastTickQuorumDataPerEpochInterval, epoch)

		value, err := proto.Marshal(&lastTickQuorumDataPerEpochIntervals)
		if err != nil {
			return errors.Wrapf(err, "serializing last quorum data per epoch intervals for epoch %d", epoch)
		}

		err = batch.Set(key, value, nil)
		if err != nil {
			return errors.Wrap(err, "setting data in batch")
		}

	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func findIntervalIndexForTick(tickNumber uint32, intervals []*protobuff.ProcessedTickInterval) int {
	for index, interval := range intervals {
		if interval.InitialProcessedTick <= tickNumber && tickNumber <= interval.LastProcessedTick {
			return index
		}
	}
	return -1
}
