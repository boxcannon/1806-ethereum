package reedsolomon

import (
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"testing"
	"time"
)

func TestFragPool_TryDecode(t *testing.T) {
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
