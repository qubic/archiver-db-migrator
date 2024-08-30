package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/archiver-db-migrator/store"
	"github.com/qubic/go-archiver/protobuff"
	"runtime"
)

const quorumSliceSize = 10000

func MigrateQuorumData(from, to *pebble.DB) error {

	err := readAllQuorumData(from, to)
	if err != nil {
		return errors.Wrap(err, "reading all quorum data")
	}

	return nil

}

func readAllQuorumData(from, to *pebble.DB) error {

	upperBound := store.AssembleKey(store.QuorumData, store.UpperBoundUint)
	iter, err := from.NewIter(&pebble.IterOptions{
		LowerBound: []byte{store.QuorumData},
		UpperBound: upperBound,
	})
	if err != nil {
		return errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	quorumDataArray := make([]*protobuff.QuorumTickData, 0)

	chunkCount := 0

	for iter.First(); iter.Valid(); iter.Next() {

		value, err := iter.ValueAndErr()
		if err != nil {
			return errors.Wrap(err, "getting value from iter")
		}

		var quorumData protobuff.QuorumTickData
		err = proto.Unmarshal(value, &quorumData)
		if err != nil {
			return errors.Wrap(err, "unmarshalling quorum data")
		}
		quorumDataArray = append(quorumDataArray, &quorumData)

		if len(quorumDataArray) >= quorumSliceSize {
			chunkCount++

			// TODO: Change to V2 later
			err = writeSliceQuorumDataV1(to, quorumDataArray)
			if err != nil {
				return errors.Wrap(err, "saving quorum data slice")
			}

			quorumDataArray = nil
			runtime.GC()
			quorumDataArray = make([]*protobuff.QuorumTickData, 0)

		}

	}

	if len(quorumDataArray) != 0 {

		//TODO: Change to V2 later
		err = writeSliceQuorumDataV1(to, quorumDataArray)
		if err != nil {
			return errors.Wrap(err, "saving quorum data slice")
		}
	}

	return nil

}

func writeSliceQuorumDataV1(db *pebble.DB, quorumDataArray []*protobuff.QuorumTickData) error {
	batch := db.NewBatchWithSize(len(quorumDataArray))
	defer batch.Close()

	for _, quorumData := range quorumDataArray {

		key := store.AssembleKey(store.QuorumData, quorumData.QuorumTickStructure.TickNumber)

		serialized, err := proto.Marshal(quorumData)
		if err != nil {
			return errors.Wrapf(err, "serializing quorum data for tick %d", quorumData.QuorumTickStructure.TickNumber)
		}
		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "setting quorum data in batch")
		}

	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.Wrap(err, "commiting batch")
	}

	return nil
}

func writeSliceQuorumDataV2(db *pebble.DB, quorumDataArray []*protobuff.QuorumTickData) error {
	batch := db.NewBatchWithSize(len(quorumDataArray))
	defer batch.Close()

	for _, quorumData := range quorumDataArray {

		newQuorumData := protobuff.QuorumTickDataV2{
			QuorumTickStructure:   quorumData.QuorumTickStructure,
			QuorumDiffPerComputor: make(map[uint32]*protobuff.QuorumDiffV2),
		}

		for id, diff := range quorumData.QuorumDiffPerComputor {
			newDiff := protobuff.QuorumDiffV2{
				SignatureHex:                diff.SignatureHex,
				ExpectedNextTickTxDigestHex: diff.ExpectedNextTickTxDigestHex,
			}
			newQuorumData.QuorumDiffPerComputor[id] = &newDiff
		}

		key := store.AssembleKey(store.QuorumData, quorumData.QuorumTickStructure.TickNumber)

		serialized, err := proto.Marshal(&newQuorumData)
		if err != nil {
			return errors.Wrapf(err, "serializing quorum data for tick %d", quorumData.QuorumTickStructure.TickNumber)
		}
		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "setting quorum data in batch")
		}

	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.Wrap(err, "commiting batch")
	}

	return nil
}
