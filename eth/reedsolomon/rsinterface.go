package reedsolomon

func (r *RSCodec) DivideAndEncode(bytedata []byte, n int, id int) []Fragment {
	data := string(bytedata[:])
	lenData := len(data)
	rmd, m := lenData%n, lenData/n
	if rmd != 0 {
		tmp := make([]byte, n-rmd)
		data += string(tmp)
		m++
	}
	subs := SplitSubN(data, n)
	//fmt.Println(subs)
	tmp := make([][]int, m)
	for i := 0; i < m; i++ {
		tmp[i] = r.Encode(subs[i])
	}
	res := make([]Fragment, n+r.EccSymbols)
	for i := 0; i < n+r.EccSymbols; i++ {
		res[i].fingerprint, res[i].pos, res[i].n, res[i].code = id, i, n, make([]byte, m)
		for j := 0; j < m; j++ {
			res[i].code[j] = byte(tmp[j][i])
		}
	}
	return res
}

func (r *RSCodec) SpliceAndDecode(dataCode []Fragment) ([]byte, int) {
	dataLen := len(dataCode)
	m := len(dataCode[0].code)
	n := dataCode[0].n
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
	ret := ""
	succ, succ_t := 1, 1
	for j := 0; j < m; j++ {
		tmpRes[j], _, succ_t = r.Decode(tmp[j], errPos)
		succ &= succ_t
		for _, i := range tmpRes[j] {
			ret += string(i)
		}
	}
	return []byte(ret), succ
}
