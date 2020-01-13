package reedsolomon

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type FragNode struct {
	Content *Fragment
	Next    *FragNode
}

type FragPool struct {
	sync.Mutex
	queue map[common.Hash]*FragNode
	cnt   map[common.Hash]uint16
}

func NewFragPool() *FragPool {
	return &FragPool{
		Mutex: sync.Mutex{},
		queue:   make(map[common.Hash]*FragNode, 0),
		cnt:     make(map[common.Hash]uint16, 0),
	}
}

// urge GC to collect garbage
func (pool *FragPool) Stop() {
	pool.queue = make(map[common.Hash]*FragNode, 0)
	pool.cnt = make(map[common.Hash]uint16, 0)
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
	pool.Lock()
	defer pool.Unlock()
	// first frag in the queue
	if _, flag := pool.queue[insPos]; !flag {
		pool.queue[insPos] = tmp
	} else {
		p := pool.queue[insPos]
		if tmp.Content.pos < p.Content.pos {
			pool.queue[insPos] = tmp
			tmp.Next = p
		} else {
			//fmt.Printf("Try to walk list\n")
			for ; p.Next != nil; p = p.Next {
				//already has this block
				if tmp.Content.pos == p.Next.Content.pos{
					flag = false
					break
				}
				if tmp.Content.pos > p.Next.Content.pos {
					break
				}
			}
			if flag{
				tmp.Next = p.Next
				p.Next = tmp
			}
		}
	}
	//fmt.Printf("Insertion ends here\n")
	if flag{
		pool.cnt[insPos]++
	}
	return pool.cnt[insPos]
}

func (pool *FragPool) Clean(pos common.Hash) {
	pool.cnt[pos] = 0
	pool.queue[pos] = nil
}

func (pool *FragPool) TryDecode(pos common.Hash) ([]byte, int) {
	rs := RSCodec{
		Primitive:  Primitive,
		EccSymbols: EccSymbol,
		NumSymbols: NumSymbol,
	}
	rs.InitLookupTables()

	data := make([]*Fragment, 0)
	pool.Lock()
	p := pool.queue[pos]
	for ; p != nil; p = p.Next {
		data = append(data, p.Content)
	}
	pool.Unlock()
	res, flag := rs.SpliceAndDecode(data)
	return res, flag
}
