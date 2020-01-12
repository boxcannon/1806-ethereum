package reedsolomon

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type FragNode struct{
	Content Fragment
	Next *FragNode
}

type FragPool struct {
	sync.RWMutex
	queue map[common.Hash]*FragNode
	cnt map[common.Hash]uint16
}

func NewFragPool() *FragPool{
	return &FragPool{
		RWMutex: sync.RWMutex{},
		queue:   make(map[common.Hash]*FragNode, 0),
		cnt:     make(map[common.Hash]uint16, 0),
	}
}

// urge GC to collect garbage
func (pool *FragPool)Stop(){
	pool.queue = make(map[common.Hash]*FragNode, 0)
	pool.cnt = make(map[common.Hash]uint16, 0)
}

// Insert a new fragment into pool
func (pool *FragPool)Insert(frag Fragment, idx common.Hash) uint16{
	tmp := &FragNode{
		Content:frag,
		Next:	nil,
	}
	insPos := idx
	// first frag in the queue
	pool.RLock()
	if _, flag := pool.queue[insPos]; flag == false{
		pool.Lock()
		pool.queue[insPos] = tmp
		pool.Unlock()
	} else {
		p := pool.queue[insPos]
		if tmp.Content.pos < p.Content.pos{
			pool.Lock()
			pool.queue[insPos] = tmp
			tmp.Next = p
			pool.Unlock()
		} else {
			for ; p.Next!=nil; p=p.Next{
				if tmp.Content.pos > p.Next.Content.pos{break}
			}
			pool.Lock()
			tmp.Next = p.Next
			p.Next = tmp
			pool.Unlock()
		}
	}
	pool.RUnlock()
	pool.cnt[insPos] += 1
	return pool.cnt[insPos]
}

func (pool *FragPool)Clean(pos common.Hash){
	pool.cnt[pos] = 0
	pool.queue[pos] = nil
}

func (pool *FragPool)TryDecode(pos common.Hash)([]byte, int){
	rs := RSCodec{
		Primitive:  0x11d,
		EccSymbols: 160,
		NumSymbols: 40,
	}
	rs.InitLookupTables()

	data := make([]Fragment, 0)
	pool.RLock()
	p := pool.queue[pos]
	for ; p!=nil;p=p.Next{
		data = append(data, p.Content)
	}
	pool.Unlock()
	res, flag := rs.SpliceAndDecode(data)
	return res, flag
}
