package migration

import (
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
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

func migrateQuorumDataV2(from, to *pebble.DB, settings Settings) error {

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

		var qtd protobuff.QuorumTickData
		err = proto.Unmarshal(value, &qtd)
		if err != nil {
			return errors.Wrap(err, "unmarshalling quorum tick v1 data")
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

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
