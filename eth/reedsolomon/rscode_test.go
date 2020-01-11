package reedsolomon

import (
	"crypto/ecdsa"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"testing"
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
	b, _ := rs.SpliceAndDecode(a)
	var tx_decoded types.Transaction
	rlp.DecodeBytes(b, &tx_decoded)
	fmt.Println(b)
	fmt.Println(tx_decoded)
	fmt.Println(*tx)
}
