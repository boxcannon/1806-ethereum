package reedsolomon

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"testing"
	"time"
)

func newTestTransaction(from *ecdsa.PrivateKey, nonce uint64, datasize int) *types.Transaction {
	tx := types.NewTransaction(nonce, common.Address{}, big.NewInt(0), 100000, big.NewInt(0), make([]byte, datasize))
	tx, _ = types.SignTx(tx, types.HomesteadSigner{}, from)
	return tx
}

func TestRSCodec_DivideAndEncode(t *testing.T) {
	var testAccount, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	tx := newTestTransaction(testAccount, 0, 0)
	rs := RSCodec{
		Primitive:  0x11d,
		EccSymbols: 160,
		NumSymbols: 40,
	}
	rs.InitLookupTables()
	txrlp, _ := rlp.EncodeToBytes(tx)
	//txrlp := []byte("hello world")
	fmt.Println(txrlp)
	a := rs.DivideAndEncode(txrlp)
	//for _, line := range a {
	//	fmt.Println(line)
	//}
	fmt.Println(a)
	//_=a
	b, _ := rs.SpliceAndDecode(a)
	var txDecoded *types.Transaction
	rlp.DecodeBytes(b, &txDecoded)
	fmt.Println(b)
	fmt.Println(*txDecoded)
	fmt.Println(*tx)
	frags := NewFragments(0)
	frags.Frags = a
	frags.ID = tx.Hash()
	fragsDecoded := NewFragments(len(frags.Frags))
	for i := 0; i < len(frags.Frags[0].code); i++ {
		fragsDecoded.Frags[i] = NewFragment(len(frags.Frags[0].code))
	}
	// try to encode and decode Fragments
	size, r, _ := rlp.EncodeToReader(frags)
	msg := p2p.Msg{
		Code:       1,
		Size:       uint32(size),
		Payload:    r,
		ReceivedAt: time.Time{},
	}
	s := rlp.NewStream(msg.Payload, uint64(msg.Size))
	err := s.Decode(&fragsDecoded)
	fmt.Println(err)
	PrintFrags(fragsDecoded)
}
