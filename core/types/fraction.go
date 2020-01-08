package types

import "github.com/ethereum/go-ethereum/common"

// Fragment of Block or Transactions
type Fragment struct {
	code        []byte
	fingerprint int16
	pos         int8
}

func (fragment *Fragment) Hash() common.Hash {
	v := rlpHash(fragment)
	return v
}
