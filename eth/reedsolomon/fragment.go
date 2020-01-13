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

	//caches
	hash atomic.Value
	size atomic.Value
}
type writeCounter common.StorageSize

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
	Frags FragmentList
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

type extFragments struct {
	Frags []*Fragment
	ID    common.Hash
}

type extFragment struct {
	Pos  uint8
	Code []byte
}

func (frags *Fragments) DecodeRLP(s *rlp.Stream) error {
	var ef extFragments
	_, size, _ := s.Kind()
	if err := s.Decode(&ef); err != nil {
		return err
	}
	frags.Frags, frags.ID = ef.Frags, ef.ID
	frags.size.Store(common.StorageSize(rlp.ListSize(size)))
	return nil
}

func (frags *Fragments) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extFragments{
		Frags: frags.Frags,
		ID:    frags.ID,
	})
}

func (frag *Fragment) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, extFragment{
		Code: frag.code,
		Pos:  frag.pos,
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

func (c *writeCounter) Write(b []byte) (int, error) {
	*c += writeCounter(len(b))
	return len(b), nil
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

type FragmentList []*Fragment

func (s FragmentList) Len() int { return len(s) }

func (s FragmentList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// GetRlp implements Rlpable and returns the i'th element of s in rlp.
func (s FragmentList) GetRlp(i int) []byte {
	enc, _ := rlp.EncodeToBytes(s[i])
	return enc
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
