package reedsolomon

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type FragNode struct {
	Content *Fragment
	Next    *FragNode
}

type FragLine struct {
	sync.Mutex
	head *FragNode
}

type FragPool struct {
	load  map[common.Hash]*FragLine
	cnt   map[common.Hash]uint16
	trial map[common.Hash]uint8
}

func NewFragLine(newNode *FragNode) *FragLine{
	return &FragLine {
		Mutex: sync.Mutex{},
		head:  newNode,
	}
}

func NewFragPool() *FragPool {
	return &FragPool{
		load:	make(map[common.Hash]*FragLine, 0),
		cnt:    make(map[common.Hash]uint16, 0),
		trial:	make(map[common.Hash]uint8, 0),
	}
}

// urge GC to collect garbage
func (pool *FragPool) Stop() {
	pool.load = nil
	pool.cnt = nil
	pool.trial = nil
}

// Insert a new fragment into pool
func (pool *FragPool) Insert(frag *Fragment, idx common.Hash) uint16 {
	//fmt.Printf("Insertion starts here\n")
	tmp := &FragNode{
		Content: frag,
		Next:    nil,
	}
	insPos := idx
	flag := true
	// create new line
	if _, flag := pool.load[insPos]; !flag {
		pool.load[insPos] = NewFragLine(tmp)
		pool.cnt[insPos] = 0
		pool.trial[insPos] = 0
	} else {
		pool.load[insPos].Lock()
		defer pool.load[insPos].Unlock()
		p := pool.load[insPos].head
		if tmp.Content.pos < p.Content.pos {
			pool.load[insPos].head = tmp
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
	if flag { pool.cnt[insPos]++ }
	return pool.cnt[insPos]
}

func (pool *FragPool) Clean(pos common.Hash) {
	delete(pool.load, pos)
	delete(pool.cnt, pos)
	delete(pool.trial, pos)
}

func (pool *FragPool) TryDecode(pos common.Hash, rs *RSCodec) ([]byte, bool) {

	data := make([]*Fragment, 0)
	pool.load[pos].Lock()
	p := pool.load[pos].head
	for ; p != nil; p = p.Next {
		data = append(data, p.Content)
	}
	pool.load[pos].Unlock()
	res, flag := rs.SpliceAndDecode(data)
	return res, flag
}
