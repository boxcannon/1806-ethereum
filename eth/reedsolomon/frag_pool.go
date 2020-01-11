package reedsolomon

//import (
//	"github.com/ethereum/go-ethereum/common"
//)

const (
	length = 2 << 8
)

type FragNode struct{
	Content Fragment
	Next *FragNode
}

type FragPool struct {
	queue [length]*FragNode
	cnt [length]uint32
}

func NewFragPool() *FragPool{
	tmp := &FragPool{}
	for i:=0; i<length;i++{
		tmp.queue[i] = nil
		tmp.cnt[i] = 0
	}
	return tmp
}

// Insert a new fragment into pool
func (pool *FragPool)Insert(frag Fragment) uint32{
	tmp := &FragNode{}
	insPos := frag.fingerprint
	tmp.Next = nil
	tmp.Content = frag
	// first frag in the queue
	if pool.queue[insPos] == nil{
		pool.queue[insPos] = tmp
	} else {
		p := pool.queue[insPos]
		if tmp.Content.pos < p.Content.pos{
			pool.queue[insPos] = tmp
			tmp.Next = p
		} else {
			for ; p.Next!=nil; p=p.Next{
				if tmp.Content.pos > p.Next.Content.pos{break}
			}
			tmp.Next = p.Next
			p.Next = tmp
		}
	}
	pool.cnt[insPos] += 1
	return pool.cnt[insPos]
}

func (pool *FragPool)clean(pos int){
	pool.cnt[pos] = 0
	pool.queue[pos] = nil
}
