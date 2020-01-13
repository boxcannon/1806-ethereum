package reedsolomon

import (
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"testing"
)

func TestFragPool_TryDecode(t *testing.T) {
	var testAccount, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	var cnt uint16
	var newtx *types.Transaction

	pool := NewFragPool()
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
	for _, frag := range frags.Frags {
		// Validate and mark the remote transaction
		cnt = pool.Insert(frag, frags.ID)
	}
	fmt.Printf("%d fragments in pool\n", cnt)
	res, flag := pool.TryDecode(frags.ID)
	fmt.Println(res)
	// flag=1 means decode success
	if flag == 1 {
		err := rlp.DecodeBytes(res, &newtx)
		if err != nil{
			fmt.Printf("Oops! Mistake occurs%v\n", err)
		}
	}
	fmt.Println(tx)
	fmt.Println(newtx)
}