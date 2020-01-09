package reedsolomon

import (
	"fmt"
	"testing"
)

func TestRSCodec_DivideAndEncode(t *testing.T) {
	str := "hello-world"
	tx := []byte(str)
	rs := RSCodec{
		Primitive:  0x11d,
		EccSymbols: 6,
	}
	rs.InitLookupTables()
	a := rs.DivideAndEncode(tx, 5, 1)
	fmt.Println(a)
	b, _ := rs.SpliceAndDecode(a[:5])
	fmt.Println(string(b))
}
