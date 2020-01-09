package eth

import (
	"github.com/ethereum/go-ethereum/eth/reedsolomon"
	"github.com/ethereum/go-ethereum/rlp"
)

var n int = 40
var ecc int = 200

func (block *types.Block) RSEncode() []*types.Fragment {
	id := block.Hash()
	blockrlp := rlp.EncodetoByte(block)
	rs := &reedsolomon.RSCodec{
		Primitive:  0x11d,
		EccSymbols: ecc,
	}
	rs.InitLookupTables()
	frags := rs.DivideAndEncode(blockrlp, n, id)
	return frags
}

func (tx *types.transaction) RSDecode() *types.Fragments {
	a := 1
}
