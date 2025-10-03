package v2

import (
	"fmt"

	"github.com/qubic/go-archiver-v2/db"
)

type ArchiverEpochStoreV2 struct {
	ArchiverStore *db.PebbleStore
}

func NewArchiverEpochStoreV2(directory string, epoch uint32) (*ArchiverEpochStoreV2, error) {
	store, err := db.CreateStore(directory, uint16(epoch))
	if err != nil {
		return nil, fmt.Errorf("creating archiver v2 database: %w", err)
	}

	return &ArchiverEpochStoreV2{
		ArchiverStore: store,
	}, nil
}

func (s *ArchiverEpochStoreV2) Close() error {
	return s.ArchiverStore.Close()
}
