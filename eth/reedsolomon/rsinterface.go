package reedsolomon

import "github.com/ethereum/go-ethereum/common"

func (r *RSCodec) DivideAndEncode(bytedata []byte, n int, id common.Hash) Fragments {
	bytedata = append(bytedata, 1)
	lenData := len(bytedata)
	rmd, m := lenData%n, lenData/n
	if rmd != 0 {
		tmp := make([]byte, n-rmd)
		bytedata = append(bytedata, tmp...)
		m++
	}
	subs := SplitSubN(bytedata, n)
	tmp := make([][]int, m)
	for i := 0; i < m; i++ {
		tmp[i] = r.Encode(string(subs[i]))
	}
	frags := make([]Fragment, n+r.EccSymbols)
	for i := 0; i < n+r.EccSymbols; i++ {
		frags[i].pos, frags[i].code = i, make([]uint8, m)
		for j := 0; j < m; j++ {
			frags[i].code[j] = uint8(tmp[j][i])
		}
	}
	res := Fragments{Fragments: frags, id: id}
	return res
}

func (r *RSCodec) SpliceAndDecode(dataCode []Fragment, n int) ([]byte, int) {
	dataLen := len(dataCode)
	m := len(dataCode[0].code)
	tmp := make([][]int, m)
	for i := 0; i < m; i++ {
		tmp[i] = make([]int, n+r.EccSymbols)
	}
	flag := make([]int, n+r.EccSymbols)
	for i := 0; i < dataLen; i++ {
		pos := dataCode[i].pos
		for j := 0; j < m; j++ {
			tmp[j][pos] = int(dataCode[i].code[j])
		}
		flag[pos] = 1
	}
	var errPos []int
	for i := 0; i < n+r.EccSymbols; i++ {
		if flag[i] == 0 {
			errPos = append(errPos, i)
		}
	}
	tmpRes := make([][]int, m)
	var ret []byte
	succ, succ_t := 1, 1
	var littleEndian = IsLittleEndian()
	for j := 0; j < m; j++ {
		tmpRes[j], _, succ_t = r.Decode(tmp[j], errPos)
		succ &= succ_t
		for _, i := range tmpRes[j] {
			if littleEndian {
				ret = append(ret, IntToBytes(i)[7])
			} else {
				ret = append(ret, IntToBytes(i)[0])
			}
		}
	}
	retLen := len(ret)
	i := retLen - 1
	for ; i >= 0; i-- {
		if ret[i] == 1 {
			break
		}
	}
	return ret[0:i], succ
}
