package store

import "encoding/binary"

const UpperBoundUint = ^uint64(0)
const UpperBoundTransaction = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
const UpperBoundIdentity = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"

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
