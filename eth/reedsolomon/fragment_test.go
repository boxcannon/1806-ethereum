package reedsolomon

import (
	"fmt"
	"testing"
)

func TestNewFragments(t *testing.T) {
	frags := NewFragments(0)
	frag2 := NewFragments(10)
	fmt.Println(frags)
	frags = frag2
	fmt.Println(frags)
	fmt.Println(frag2)
	//frags.Frags = append(frags.Frags,NewFragment())
	//fmt.Println(*frags.Frags[0])
	//fmt.Println(frags.ID)
}
