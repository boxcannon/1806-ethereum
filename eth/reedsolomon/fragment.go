package reedsolomon

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
)

// Fragment of Block or Transactions
type Fragment struct {
	code []byte
	pos  uint8
}

/*func NewFragment() *Fragment{
	return &Fragment{
		code:	make([]byte, 0),
		pos:	0,
	}
}*/

type Fragments struct {
	Frags	[]Fragment
	ID		common.Hash
}

func NewFragments() *Fragments{
	return &Fragments{
		Frags:	make([]Fragment, 0),
	}
}

func (fragment *Fragment) Hash() common.Hash {
	v := rlpHash(fragment)
	return v
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}
