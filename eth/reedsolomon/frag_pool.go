package reedsolomon

import (
	"github.com/willf/bitset"
	"math/big"
	"sync"
	"sync/atomic"
)
const (
	TxFrag = 0x11
	BlockFrag = 0x12
)

type FragNode struct {
	Content *Fragment
	Next    *FragNode
}

type ReqNode struct {
	Bit 	*bitset.BitSet
	PeerID 	string
	Next	*ReqNode
}

type FragLine struct {
	mutex 			sync.Mutex
	head			*FragNode
	Bit				*bitset.BitSet
	MinHop 			uint32
	MinHopPeer 		string
	TotalFrag 		uint64
	Cnt				uint64
	Trial			uint8
	Type 			uint64
	IsDecoded  		uint32
	TD      		*big.Int
	IsReqing 		uint32
	ReqHead			*ReqNode
}

type FragPool struct {
	BigMutex	sync.Mutex
	Load		map[FragHash]*FragLine
}

func NewFragLine(newNode *FragNode, fragType uint64, minHop uint32, minHopPeer string) *FragLine{
	return &FragLine {
		head:	newNode,
		Bit:	bitset.New(EccSymbol+NumSymbol),
		MinHop: minHop,
		MinHopPeer: minHopPeer,
		TotalFrag: 0,
		Cnt:	0,
		Trial:  0,
		IsDecoded: 0,
		Type: fragType,
		TD:		new(big.Int),
		IsReqing: 0,
		ReqHead: nil,
	}
}

func NewFragPool() *FragPool {
	return &FragPool{
		Load:  make(map[FragHash]*FragLine, 0),
	}
}

func NewReqNode(bit *bitset.BitSet, peerID string) *ReqNode {
	return &ReqNode{
		Bit:	bit.Clone(),
		PeerID:	peerID,
		Next:	nil,
	}
}

// Urge GC to collect garbage
func (pool *FragPool) Stop() {
	pool.Load = nil
}

// Insert a new fragment into pool
func (pool *FragPool) Insert(frag *Fragment, idx FragHash, hopCnt uint32, peerID string, td *big.Int, fragType uint64) (uint64, uint64, uint32) {
	tmp := &FragNode {
		Content: frag,
		Next:    nil,
	}
	insPos := idx
	flag := true
	var line *FragLine
	pool.BigMutex.Lock()
	defer pool.BigMutex.Unlock()
	// create new line, first insertion should store TD
	if _, ok := pool.Load[insPos]; !ok {
		pool.Load[insPos] = NewFragLine(tmp, fragType, hopCnt, peerID)
		pool.Load[insPos].Bit.Set(uint(frag.pos))
		// first insertion decides TD
		line = pool.Load[insPos]
		pool.Load[insPos].TD = td
	} else {
		p := pool.Load[insPos].head
		line = pool.Load[insPos]
		line.mutex.Lock()
		defer line.mutex.Unlock()
		if tmp.Content.pos < p.Content.pos {
			line.head = tmp
			tmp.Next = p
		} else {
			if tmp.Content.pos == p.Content.pos {
				flag = false
			}
			for ; p.Next != nil; p = p.Next {
				// already has this block, ignore
				if tmp.Content.pos == p.Next.Content.pos {
					flag = false
					break
				}
				if tmp.Content.pos < p.Next.Content.pos {
					break
				}
			}
			if flag {
				tmp.Next = p.Next
				p.Next = tmp
			}
		}
	}
	atomic.AddUint64(&line.TotalFrag,1)
	if line.MinHop > hopCnt {
		line.MinHopPeer = peerID
		line.MinHop = hopCnt
	}
	if flag {
		atomic.AddUint64(&line.Cnt,1)
		line.Bit.Set(uint(tmp.Content.pos))
	}
	return pool.Load[insPos].Cnt, pool.Load[insPos].TotalFrag, pool.Load[insPos].IsDecoded
}

// Delete maybe unused frags
func (pool *FragPool) Clean(pos FragHash) {
	pool.BigMutex.Lock()
	delete(pool.Load, pos)
	pool.BigMutex.Unlock()
}

// Try to use fragments to decode, return res and whether succeeds
func (pool *FragPool) TryDecode(pos FragHash, rs *RSCodec) ([]byte, bool) {
	data := make([]*Fragment, 0)
	pool.BigMutex.Lock()
	p := pool.Load[pos].head
	line := pool.Load[pos]
	line.mutex.Lock()
	defer line.mutex.Unlock()
	defer pool.BigMutex.Unlock()
	for ; p != nil; p = p.Next {
		data = append(data, p.Content)
	}
	line.Trial++
	res, flag := rs.SpliceAndDecode(data)
	if flag {
		atomic.StoreUint32(&pool.Load[pos].IsDecoded,1)
	}
	return res, flag
}

// Based on peer's request, provide all useful fragments
func (pool *FragPool) Prepare(req *Request) *Fragments {
	var flag bool
	tmp := NewFragments(0)
	tmp.ID = req.ID
	// the message is mean to answer a request
	tmp.IsResp = 1

	pool.BigMutex.Lock()
	line := pool.Load[req.ID]
	line.mutex.Lock()
	defer line.mutex.Unlock()
	defer pool.BigMutex.Unlock()
	bits := line.Bit.Difference(req.Load)
	for p := line.head; p!= nil; p = p.Next {
		flag = bits.Test(uint(p.Content.pos))
		if flag {
			tmp.Frags = append(tmp.Frags, p.Content)
		}
	}
	return tmp
}

// Insert a request that should response later
func (line *FragLine) InsertReq(bit *bitset.BitSet, peerID string) uint32 {
	line.mutex.Lock()
	defer line.mutex.Unlock()

	oldReqing := line.IsReqing
	newNode := NewReqNode(bit, peerID)
	newNode.Next = line.ReqHead
	line.ReqHead = newNode
	line.IsReqing = 1

	return oldReqing
}

// Insert a request that should response later
func (line *FragLine) SetIsReqing() uint32 {
	line.mutex.Lock()
	defer line.mutex.Unlock()

	oldReqing := line.IsReqing
	line.IsReqing = 1

	return oldReqing
}

// Clear waiting list
func (line *FragLine) ClearReq() *ReqNode {
	line.mutex.Lock()
	defer line.mutex.Unlock()

	oldHead := line.ReqHead
	line.ReqHead = nil
	line.IsReqing = 0

	return oldHead
}

