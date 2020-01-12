package reedsolomon

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
	"io"
	"sync/atomic"
)

// Fragment of Block or Transactions
type Fragment struct {
	pos  uint8
	code []byte
}

type writeCounter common.StorageSize

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
}

func NewFragment(size int) *Fragment {
	return &Fragment{
		code: make([]byte, size),
	}
}

/*func NewFragment() *Fragment{
	return &Fragment{
		code:	make([]byte, 0),
		pos:	0,
	}
}*/

type Fragments struct {
	Frags []*Fragment
	ID    common.Hash

	//caches
	hash atomic.Value
	size atomic.Value
}

func NewFragments(size int) *Fragments {
	return &Fragments{
		Frags: make([]*Fragment, size),
	}
}

type extfragments struct {
	ID    common.Hash
	Frags []*Fragment
}

func (frags *Fragments) DecodeRLP(s *rlp.Stream) error {
	var ef extfragments
	_, size, _ := s.Kind()
	if err := s.Decode(&ef); err != nil {
		return err
	}
	frags.Frags, frags.ID = ef.Frags, ef.ID
	frags.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (frags *Fragments) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extfragments{
		ID:    frags.ID,
		Frags: frags.Frags,
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

func (fragment *Fragment) Hash() common.Hash {
	v := rlpHash(fragment)
	return v
}

func PrintFrags(frags *Fragments) {
	for _, frag := range frags.Frags {
		fmt.Printf("code: %x,pos: %d\n", frag.code, frag.pos)
	}
	fmt.Printf("ID: %x", frags.ID)
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}
