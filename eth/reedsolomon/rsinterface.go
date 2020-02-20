package reedsolomon

import (
	"log"
)

func (r *RSCodec) DivideAndEncode(bytedata []byte) []*Fragment {
	bytedata = append(bytedata, 1)
	lenData := len(bytedata)
	rmd, m := lenData%r.NumSymbols, lenData/r.NumSymbols
	if rmd != 0 {
		tmp := make([]byte, r.NumSymbols-rmd)
		bytedata = append(bytedata, tmp...)
		m++
	}
	subs := SplitSubN(bytedata, r.NumSymbols)
	tmp := make([][]int, m)
	for i := 0; i < m; i++ {
		tmp[i] = r.Encode(string(subs[i]))
	}
	res := make([]*Fragment, r.NumSymbols+r.EccSymbols)
	for i := 0; i < r.NumSymbols+r.EccSymbols; i++ {
		res[i] = NewFragment(m)
		res[i].pos = IntToUint8(i)
		for j := 0; j < m; j++ {
			res[i].code[j] = uint8(tmp[j][i])
		}
	}
	return res
}

func (r *RSCodec) SpliceAndDecode(dataCode []*Fragment) ([]byte, bool) {
	dataLen := len(dataCode)
	m := len(dataCode[0].code)
	tmp := make([][]int, m)
	for i := 0; i < m; i++ {
		tmp[i] = make([]int, r.NumSymbols+r.EccSymbols)
	}
	flag := make([]int, r.NumSymbols+r.EccSymbols)
	for i := 0; i < dataLen; i++ {
		pos := dataCode[i].pos
		for j := 0; j < m; j++ {
			if flag[pos] == 1 && tmp[j][pos] != int(dataCode[i].code[j]) {
				log.Println("Fragments with the same position are received twice and they are different.")
				return nil, false
			}
			tmp[j][pos] = int(dataCode[i].code[j])
		}
		flag[pos] = 1
	}
	var errPos []int
	for i := 0; i < r.NumSymbols+r.EccSymbols; i++ {
		if flag[i] == 0 {
			errPos = append(errPos, i)
		}
	}
	tmpRes := make([][]int, m)
	var ret []byte
	var littleEndian = IsLittleEndian()
	for j := 0; j < m; j++ {
		sucTmp := 1
		tmpRes[j], _, sucTmp = r.Decode(tmp[j], errPos)
		if sucTmp == 0 {
			return nil, false
		}
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
		if ret != nil && ret[i] == 1 {
			break
		}
	}
	if i < 0 {
		return nil, false
	}
	return ret[0:i], true
}
