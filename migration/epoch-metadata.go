package migration

import (
	"context"
	"fmt"
	"log"

	v2 "github.com/qubic/archiver-db-migrator/store/v2"
	"github.com/qubic/go-archiver-v2/protobuf"
)

func (m *Migrator) MigrateEpochMetadata(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	log.Printf("Migrating metadata for epoch %d\n", epoch)

	log.Println("Migrating computor list...")
	err := m.MigrateComputorList(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating computor list for epoch %d: %w", epoch, err)
	}

	log.Println("Migrating processed tick ranges...")
	err = m.MigrateProcessedTickRanges(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating processed tick ranges for epoch %d: %w", epoch, err)
	}

	log.Println("Migrating last processed tick...")
	err = m.MigrateLastProcessedTick(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating last processed tick for epoch %d: %w", epoch, err)
	}

	log.Println("Migrating tick range last tick quorum data...")
	err = m.MigrateTickRangeLastTickQuorumData(epoch, newStore)
	if err != nil {
		return fmt.Errorf("migrating tick range last tick quorum data for epoch %d: %w", epoch, err)
	}

	if epoch > 158 {
		log.Println("Migrating target tick vote signature...")
		err = m.MigrateTargetTickVoteSignature(epoch, newStore)
		if err != nil {
			return fmt.Errorf("migrating target tick vote signature for epoch %d: %w", epoch, err)
		}
	}

	log.Println("Done.")
	return nil
}

func (m *Migrator) MigrateComputorList(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	computors, err := m.oldStore.ArchiverStore.GetComputors(context.Background(), epoch)
	if err != nil {
		return fmt.Errorf("getting computors for epoch %d: %w", epoch, err)
	}

	computorsList := protobuf.ComputorsList{
		Computors: make([]*protobuf.Computors, 0),
	}

	computorsList.Computors = append(computorsList.Computors, &protobuf.Computors{
		Epoch:        computors.Epoch,
		Identities:   computors.Identities,
		SignatureHex: computors.SignatureHex,
	})

	err = newStore.ArchiverStore.SetComputors(context.Background(), epoch, &computorsList)
	if err != nil {
		return fmt.Errorf("storing computors for epoch %d: %w", epoch, err)
	}

	return nil
}

func (m *Migrator) MigrateProcessedTickRanges(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	ranges, err := m.oldStore.ArchiverStore.GetProcessedTickIntervals(context.Background())
	if err != nil {
		return fmt.Errorf("getting processed tick intervals: %w", err)
	}

	var epochRanges []*protobuf.ProcessedTickInterval
	for _, e := range ranges {
		if e.Epoch != epoch {
			continue
		}

		for _, interval := range e.Intervals {
			epochRanges = append(epochRanges, &protobuf.ProcessedTickInterval{
				InitialProcessedTick: interval.InitialProcessedTick,
				LastProcessedTick:    interval.LastProcessedTick,
			})
		}

	}
	if len(epochRanges) == 0 {
		return fmt.Errorf("failed to find processed tick intervals for epoch %d", epoch)
	}

	rangesV2 := protobuf.ProcessedTickIntervalsPerEpoch{
		Epoch:     epoch,
		Intervals: epochRanges,
	}

	err = newStore.ArchiverStore.SetProcessedTickIntervalPerEpoch(context.Background(), epoch, &rangesV2)
	if err != nil {
		return fmt.Errorf("storing processed tick intervals for epoch %d: %w", epoch, err)
	}
	return nil
}

func (m *Migrator) MigrateLastProcessedTick(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	lastProcessedTick := m.oldStore.StoreMetadata.Epochs[epoch].LastProcessedTick
	err := newStore.ArchiverStore.SetLastProcessedTick(context.Background(), &protobuf.ProcessedTick{
		TickNumber: lastProcessedTick,
		Epoch:      epoch,
	})
	if err != nil {
		return fmt.Errorf("storing last processed tick for epoch %d: %w", epoch, err)
	}
	return nil
}

func (m *Migrator) MigrateTickRangeLastTickQuorumData(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {

	lastTickQuorumDataPerEpochInterval, err := m.oldStore.ArchiverStore.GetLastTickQuorumDataListPerEpochInterval(epoch)
	if err != nil {
		return fmt.Errorf("getting last tick quorum data list for epoch %d: %w", epoch, err)
	}

	var lastTickQuorumDataPerEpochIntervalV2 protobuf.LastTickQuorumDataPerEpochIntervals
	lastTickQuorumDataPerEpochIntervalV2.QuorumDataPerInterval = make(map[int32]*protobuf.QuorumTickData)
	for index, quorumData := range lastTickQuorumDataPerEpochInterval.QuorumDataPerInterval {

		quorumTickStructure := protobuf.QuorumTickStructure{
			Epoch:                        quorumData.QuorumTickStructure.Epoch,
			TickNumber:                   quorumData.QuorumTickStructure.TickNumber,
			Timestamp:                    quorumData.QuorumTickStructure.Timestamp,
			PrevResourceTestingDigestHex: quorumData.QuorumTickStructure.PrevResourceTestingDigestHex,
			PrevSpectrumDigestHex:        quorumData.QuorumTickStructure.PrevSpectrumDigestHex,
			PrevUniverseDigestHex:        quorumData.QuorumTickStructure.PrevUniverseDigestHex,
			PrevComputerDigestHex:        quorumData.QuorumTickStructure.PrevComputerDigestHex,
			TxDigestHex:                  quorumData.QuorumTickStructure.TxDigestHex,
			PrevTransactionBodyHex:       quorumData.QuorumTickStructure.PrevTransactionBodyHex,
		}

		quorumDiffPerComputor := make(map[uint32]*protobuf.QuorumDiff)
		for index2, diff := range quorumData.QuorumDiffPerComputor {
			quorumDiffPerComputor[index2] = &protobuf.QuorumDiff{
				SaltedResourceTestingDigestHex: diff.SaltedResourceTestingDigestHex,
				SaltedSpectrumDigestHex:        diff.SaltedSpectrumDigestHex,
				SaltedUniverseDigestHex:        diff.SaltedUniverseDigestHex,
				SaltedComputerDigestHex:        diff.SaltedComputerDigestHex,
				ExpectedNextTickTxDigestHex:    diff.ExpectedNextTickTxDigestHex,
				SignatureHex:                   diff.SignatureHex,
				SaltedTransactionBodyHex:       diff.SaltedTransactionBodyHex,
			}
		}

		lastTickQuorumDataPerEpochIntervalV2.QuorumDataPerInterval[index] = &protobuf.QuorumTickData{
			QuorumTickStructure:   &quorumTickStructure,
			QuorumDiffPerComputor: quorumDiffPerComputor,
		}
	}

	err = newStore.ArchiverStore.SetLastTickQuorumDataPerEpochIntervals(epoch, &lastTickQuorumDataPerEpochIntervalV2)
	if err != nil {
		return fmt.Errorf("storing last tick quorum data list for epoch %d: %w", epoch, err)
	}
	return nil
}

func (m *Migrator) MigrateTargetTickVoteSignature(epoch uint32, newStore *v2.ArchiverEpochStoreV2) error {
	targetTickVoteSignature, err := m.oldStore.ArchiverStore.GetTargetTickVoteSignature(epoch)
	if err != nil {
		return fmt.Errorf("getting target tick vote signature for epoch %d: %w", epoch, err)
	}

	err = newStore.ArchiverStore.SetTargetTickVoteSignature(epoch, targetTickVoteSignature)
	if err != nil {
		return fmt.Errorf("storing target tick vote signature for epoch %d: %w", epoch, err)
	}

	return nil
}
