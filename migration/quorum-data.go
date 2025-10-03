package migration

import (
	"encoding/binary"
	"fmt"
	"runtime"

	pebbleV1 "github.com/cockroachdb/pebble"
	pebbleV2 "github.com/cockroachdb/pebble/v2"
	"github.com/golang/protobuf/proto"
	migratorStore "github.com/qubic/archiver-db-migrator/store"
	v1 "github.com/qubic/archiver-db-migrator/store/v1"
	v2 "github.com/qubic/archiver-db-migrator/store/v2"
	archiverV2Store "github.com/qubic/go-archiver-v2/db"
	protoV2 "github.com/qubic/go-archiver-v2/protobuf"
	protoV1 "github.com/qubic/go-archiver/protobuff"
	archiverV1Store "github.com/qubic/go-archiver/store"
	"github.com/schollz/progressbar/v3"
)

func (m *Migrator) migrateQuorumDataRange(tickRange v1.TickRange, newStore *v2.ArchiverEpochStoreV2) error {

	bar := progressbar.Default(int64(tickRange.End-tickRange.Start), fmt.Sprintf("Migrating quorum data ticks %d to %d", tickRange.Start, tickRange.End))

	iter, err := m.oldStore.GetDB().NewIter(
		&pebbleV1.IterOptions{
			LowerBound: migratorStore.AssembleKey(archiverV1Store.QuorumData, tickRange.Start),
			UpperBound: migratorStore.AssembleKey(archiverV1Store.QuorumData, tickRange.End),
		})
	if err != nil {
		return fmt.Errorf("creating iterator for quorum data range %v: %w", tickRange, err)
	}
	defer iter.Close()

	counter := 0

	batch := newStore.ArchiverStore.GetDB().NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {

		_ = bar.Add(1)

		key := iter.Key()
		tickNumber := binary.BigEndian.Uint32(key[1:])

		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("getting value for quorum data tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		var quorumDataV1 protoV1.QuorumTickDataStored
		err = proto.Unmarshal(value, &quorumDataV1)
		if err != nil {
			return fmt.Errorf("unmarshaling quorum data for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		quorumDiffPerComputorV2 := make(map[uint32]*protoV2.QuorumDiffStored)
		for index, diff := range quorumDataV1.QuorumDiffPerComputor {
			quorumDiffPerComputorV2[index] = &protoV2.QuorumDiffStored{
				ExpectedNextTickTxDigestHex: diff.ExpectedNextTickTxDigestHex,
				SignatureHex:                diff.SignatureHex,
			}
		}

		quorumDataV2 := protoV2.QuorumTickDataStored{
			QuorumTickStructure: &protoV2.QuorumTickStructure{
				Epoch:                        quorumDataV1.QuorumTickStructure.Epoch,
				TickNumber:                   quorumDataV1.QuorumTickStructure.TickNumber,
				Timestamp:                    quorumDataV1.QuorumTickStructure.Timestamp,
				PrevResourceTestingDigestHex: quorumDataV1.QuorumTickStructure.PrevResourceTestingDigestHex,
				PrevSpectrumDigestHex:        quorumDataV1.QuorumTickStructure.PrevSpectrumDigestHex,
				PrevUniverseDigestHex:        quorumDataV1.QuorumTickStructure.PrevUniverseDigestHex,
				PrevComputerDigestHex:        quorumDataV1.QuorumTickStructure.PrevComputerDigestHex,
				TxDigestHex:                  quorumDataV1.QuorumTickStructure.TxDigestHex,
				PrevTransactionBodyHex:       quorumDataV1.QuorumTickStructure.PrevTransactionBodyHex,
			},
			QuorumDiffPerComputor: quorumDiffPerComputorV2,
		}

		data, err := proto.Marshal(&quorumDataV2)
		if err != nil {
			return fmt.Errorf("marshaling quorum data v2 for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		err = batch.Set(migratorStore.AssembleKey(archiverV2Store.QuorumData, quorumDataV2.QuorumTickStructure.TickNumber), data, nil)
		if err != nil {
			return fmt.Errorf("setting quorum data v2 for tick %d in range %v: %w", tickNumber, tickRange, err)
		}
		counter++

		if counter >= m.batchSize {
			err = batch.Commit(pebbleV2.Sync)
			if err != nil {
				return fmt.Errorf("committing batch for quorum data range %v: %w", tickRange, err)
			}

			batch.Reset()
			runtime.GC()
			counter = 0
		}
	}

	err = batch.Commit(pebbleV2.Sync)
	if err != nil {
		return fmt.Errorf("committing batch for quorum data range %v: %w", tickRange, err)
	}
	return nil
}
