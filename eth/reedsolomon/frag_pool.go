package reedsolomon

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/willf/bitset"
	"math/big"
	"sync"
)


type FragNode struct {
	Content *Fragment
	Next    *FragNode
}

type FragLine struct {
	mutex   sync.Mutex
	head	*FragNode
	Bit		*bitset.BitSet
	Cnt		uint16
	Trial	uint8
	TD      *big.Int
}

type FragPool struct {
	BigMutex	sync.Mutex
	Load		map[common.Hash]*FragLine
}

func NewFragLine(newNode *FragNode) *FragLine{
	return &FragLine {
		head:	newNode,
		Bit:	bitset.New(EccSymbol+NumSymbol),
		Cnt:	0,
		Trial:  0,
		TD:		nil,
	}
}

func NewFragPool() *FragPool {
	return &FragPool{
		Load:  make(map[common.Hash]*FragLine, 0),
	}
}

// urge GC to collect garbage
func (pool *FragPool) Stop() {
	pool.Load = nil
}

// Insert a new fragment into pool
func (pool *FragPool) Insert(frag *Fragment, idx common.Hash, td *big.Int) uint16 {
	//fmt.Printf("Insertion starts here\n")
	tmp := &FragNode {
		Content: frag,
		Next:    nil,
	}
	insPos := idx
	flag := true
	var line *FragLine
	pool.BigMutex.Lock()
	// create new line
	if _, flag := pool.Load[insPos]; !flag {
		pool.Load[insPos] = NewFragLine(tmp)
		pool.Load[insPos].Bit.Set(uint(frag.pos))
		// first insertion decides TD
		pool.Load[insPos].TD = td
		pool.BigMutex.Unlock()
	} else {
		p := pool.Load[insPos].head
		line = pool.Load[insPos]
		line.mutex.Lock()
		defer line.mutex.Unlock()
		pool.BigMutex.Unlock()
		if tmp.Content.pos < p.Content.pos {
			line.head = tmp
			tmp.Next = p
		} else {
			//fmt.Printf("Try to walk list\n")
			for ; p.Next != nil; p = p.Next {
				//already has this block
				if tmp.Content.pos == p.Next.Content.pos {
					flag = false
					break
				}
				if tmp.Content.pos > p.Next.Content.pos {
					break
				}
			}
			if flag {
				tmp.Next = p.Next
				p.Next = tmp
			}
		}
	}
	if flag {
		line.Cnt++
		line.Bit.Set(uint(tmp.Content.pos))
	}
	return pool.Load[insPos].Cnt
}

func (pool *FragPool) Clean(pos common.Hash) {
	pool.BigMutex.Lock()
	delete(pool.Load, pos)
	pool.BigMutex.Unlock()
}

func (pool *FragPool) TryDecode(pos common.Hash, rs *RSCodec) ([]byte, int) {
	data := make([]*Fragment, 0)
	pool.BigMutex.Lock()
	p := pool.Load[pos].head
	line := pool.Load[pos]
	line.mutex.Lock()
	defer line.mutex.Unlock()
	pool.BigMutex.Unlock()
	for ; p != nil; p = p.Next {
		data = append(data, p.Content)
	}
	line.Trial++
	res, flag := rs.SpliceAndDecode(data)
	return res, flag
}

func (pool *FragPool) Prepare(req *Request) *Fragments {
	var flag bool
	tmp := NewFragments(0)
	tmp.ID = req.ID
	pool.BigMutex.Lock()
	line := pool.Load[req.ID]
	line.mutex.Lock()
	defer line.mutex.Unlock()
	pool.BigMutex.Unlock()
	bits := line.Bit.Difference(req.load)
	for p := line.head; p!= nil; p = p.Next {
		flag = bits.Test(uint(p.Content.pos))
		if flag {
			tmp.Frags = append(tmp.Frags, p.Content)
		}
	}
	return tmp
}
