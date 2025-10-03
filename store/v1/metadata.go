package v1

import "log"

type TickRange struct {
	Start uint32
	End   uint32
}

type EpochMetadata struct {
	Epoch               uint32
	ProcessedTickRanges []TickRange
	LastProcessedTick   uint32
}

type StoreMetadata struct {
	Epochs map[uint32]EpochMetadata
}

func (sm *StoreMetadata) PrintStoreMetadata() {

	for epoch, metadata := range sm.Epochs {

		log.Printf("Epoch: %d\n", epoch)
		log.Printf("  - Last processed tick: %d\n", metadata.LastProcessedTick)
		log.Println("  - Tick ranges:")
		for _, tickRange := range metadata.ProcessedTickRanges {
			log.Printf("    - %d : %d\n", tickRange.Start, tickRange.End)
		}

	}

}
