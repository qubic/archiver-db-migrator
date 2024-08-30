package store

import "encoding/binary"

const UpperBoundUint = ^uint64(0)
const UpperBoundTransaction = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
const UpperBoundIdentity = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"

const (
	TickData                     = 0x00
	QuorumData                   = 0x01
	ComputorList                 = 0x02
	Transaction                  = 0x03
	LastProcessedTick            = 0x04
	LastProcessedTickPerEpoch    = 0x05
	SkippedTicksInterval         = 0x06
	IdentityTransferTransactions = 0x07
	ChainDigest                  = 0x08
	ProcessedTickIntervals       = 0x09
	TickTransactionsStatus       = 0x10
	TransactionStatus            = 0x11
	StoreDigest                  = 0x12
	EmptyTicksPerEpoch           = 0x13
)

type IDType interface {
	uint32 | uint64 | string
}

func AssembleKey[T IDType](keyPrefix int, id T) []byte {

	prefix := byte(keyPrefix)

	key := []byte{prefix}

	switch any(id).(type) {

	case uint32:
		asserted := any(id).(uint32)
		key = binary.BigEndian.AppendUint64(key, uint64(asserted))
		break

	case uint64:
		asserted := any(id).(uint64)
		key = binary.BigEndian.AppendUint64(key, asserted)
		break

	case string:
		asserted := any(id).(string)
		key = append(key, []byte(asserted)...)
	}
	return key
}
