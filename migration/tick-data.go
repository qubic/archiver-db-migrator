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

func (m *Migrator) migrateTickDataRange(tickRange v1.TickRange, newStore *v2.ArchiverEpochStoreV2) (map[uint32][]string, int, error) {

	bar := progressbar.Default(int64(tickRange.End-tickRange.Start), fmt.Sprintf("Migrating tick data ticks %d to %d", tickRange.Start, tickRange.End))

	txsPerTick := make(map[uint32][]string)
	txCounter := 0

	iter, err := m.oldStore.GetDB().NewIter(
		&pebbleV1.IterOptions{
			LowerBound: migratorStore.AssembleKey(archiverV1Store.TickData, tickRange.Start),
			UpperBound: migratorStore.AssembleKey(archiverV1Store.TickData, tickRange.End),
		})
	if err != nil {
		return nil, 0, fmt.Errorf("creating iterator for tick data range %v: %w", tickRange, err)
	}
	defer iter.Close()

	counter := 0

	batch := newStore.ArchiverStore.GetDB().NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {

		_ = bar.Add(1)

		key := iter.Key()
		tickNumber := binary.BigEndian.Uint32(key[1:]) // TODO: for some reason this is 0 all the time.

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, 0, fmt.Errorf("getting value for tick data tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		var tickDataV1 protoV1.TickData
		err = proto.Unmarshal(value, &tickDataV1)
		if err != nil {
			return nil, 0, fmt.Errorf("unmarshaling tick data for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		_, exists := txsPerTick[tickDataV1.TickNumber]
		if !exists {
			txsPerTick[tickDataV1.TickNumber] = []string{}
		}

		txsPerTick[tickDataV1.TickNumber] = append(txsPerTick[tickDataV1.TickNumber], tickDataV1.TransactionIds...)
		txCounter += len(tickDataV1.TransactionIds)

		tickDataV2 := protoV2.TickData{
			ComputorIndex:  tickDataV1.ComputorIndex,
			Epoch:          tickDataV1.Epoch,
			TickNumber:     tickDataV1.TickNumber,
			Timestamp:      tickDataV1.Timestamp,
			VarStruct:      tickDataV1.VarStruct,
			TimeLock:       tickDataV1.TimeLock,
			TransactionIds: tickDataV1.TransactionIds,
			ContractFees:   tickDataV1.ContractFees,
			SignatureHex:   tickDataV1.SignatureHex,
		}

		data, err := proto.Marshal(&tickDataV2)
		if err != nil {
			return nil, 0, fmt.Errorf("marshaling tick data v2 for tick %d in range %v: %w", tickNumber, tickRange, err)
		}

		err = batch.Set(migratorStore.AssembleKey(archiverV2Store.TickData, tickDataV2.TickNumber), data, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("setting tick data v2 for tick %d in range %v: %w", tickNumber, tickRange, err)
		}
		counter++

		if counter >= m.batchSize {
			err = batch.Commit(pebbleV2.Sync)
			if err != nil {
				return nil, 0, fmt.Errorf("committing batch for tick data range %v: %w", tickRange, err)
			}

			batch.Reset()
			runtime.GC()
			counter = 0
		}
	}

	err = batch.Commit(pebbleV2.Sync)
	if err != nil {
		return nil, 0, fmt.Errorf("committing batch for tick data range %v: %w", tickRange, err)
	}
	return txsPerTick, txCounter, nil
}
