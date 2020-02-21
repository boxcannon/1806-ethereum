package reedsolomon

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/willf/bitset"
	"golang.org/x/crypto/sha3"
	"io"
	"sync/atomic"
)

// Fragment of Block or Transactions
type Fragment struct {
	pos  uint8
	code []byte

	//caches
	hash atomic.Value
	size atomic.Value
}

type Request struct {
	Load *bitset.BitSet
	ID   common.Hash

	//caches
	hash atomic.Value
	size atomic.Value
}

func NewFragment(size int) *Fragment {
	return &Fragment{
		code: make([]byte, size),
	}
}

func NewRequest(ID common.Hash, s *bitset.BitSet) *Request{
	return &Request{
		Load: s,
		ID:   ID,
	}
}

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

/*func NewFragment() *Fragment{
	return &Fragment{
		code:	make([]byte, 0),
		pos:	0,
	}
}*/

type FragmentList []*Fragment

type Fragments struct {
	Frags 		FragmentList
	ID    		common.Hash
	HopCnt 		uint32
	IsResp 		uint32

	//caches
	hash atomic.Value
	size atomic.Value
}

func NewFragments(size int) *Fragments {
	return &Fragments{
		Frags: make([]*Fragment, size),
		HopCnt: 0,
		IsResp: 0,
	}
}

type extFragments struct {
	Frags 		[]*Fragment
	ID    		common.Hash
	HopCnt 		uint32
	IsResp 		uint32
}

type extFragment struct {
	Pos  uint8
	Code []byte
}

type extRequest struct {
	Load  *bitset.BitSet
	ID    common.Hash
}

func (frags *Fragments) DecodeRLP(s *rlp.Stream) error {
	var ef extFragments
	_, size, _ := s.Kind()
	if err := s.Decode(&ef); err != nil {
		return err
	}
	frags.Frags, frags.ID, frags.HopCnt, frags.IsResp = ef.Frags, ef.ID, ef.HopCnt, ef.IsResp
	frags.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (frags *Fragments) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extFragments{
		Frags: frags.Frags,
		ID:    frags.ID,
		HopCnt: frags.HopCnt,
		IsResp: frags.IsResp,
	})
}

func (frag *Fragment) DecodeRLP(s *rlp.Stream) error {
	var ef extFragment
	_, size, _ := s.Kind()
	if err := s.Decode(&ef); err != nil {
		return err
	}
	frag.code, frag.pos = ef.Code, ef.Pos
	frag.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (frag *Fragment) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extFragment{
		Code: frag.code,
		Pos:  frag.pos,
	})
}

func (req *Request) DecodeRLP(s *rlp.Stream) error {
	var er extRequest
	_, size, _ := s.Kind()
	if err := s.Decode(&er); err != nil {
		return err
	}
	req.Load, req.ID = er.Load, er.ID
	req.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (req *Request) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extRequest{
		Load:	req.Load,
		ID:		req.ID,
	})
}

func (frags *Fragments) Size() common.StorageSize {
	if size := frags.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, frags)
	frags.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (fragment *Fragment) Size() common.StorageSize {
	if size := fragment.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, fragment)
	fragment.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (req *Request) Size() common.StorageSize {
	if size := req.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, req)
	req.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

func (frags *Fragments) Hash() common.Hash {
	if hash := frags.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	v := rlpHash(frags)
	frags.hash.Store(v)
	return v
}

func (fragment *Fragment) Hash() common.Hash {
	if hash := fragment.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	v := rlpHash(fragment)
	fragment.hash.Store(v)
	return v
}

func (req *Request) Hash() common.Hash {
	if hash := req.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	v := rlpHash(req)
	req.hash.Store(v)
	return v
}

func (frag *Fragment) Pos() uint8 {
	return frag.pos
}

func PrintFrags(frags *Fragments) {
	for _, frag := range frags.Frags {
		fmt.Printf("code: %x,pos: %d\n", frag.code, frag.pos)
	}
	fmt.Printf("ID: %x\n", frags.ID)
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}
