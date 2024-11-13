// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"encoding/binary"
	"io"
	"math"
	"slices"

	"github.com/ava-labs/avalanchego/ids"
)

const (
	addrSize = 16
)

func childSize_disk(index byte, childEntry *child) int {
	// * index
	// * child ID
	// * child key
	// * Now need a disk address
	// * bool indicating whether the child has a value
	return uintSize(uint64(index)) + ids.IDLen + keySize(childEntry.compressedKey) + addrSize + boolLen
}

// Assumes [n] is non-nil.
func encodeDBNode_disk(n *dbNode) []byte {
	length := encodedDBNodeSize(n)
	w := codecWriter{
		b: make([]byte, 0, length),
	}

	w.MaybeBytes(n.value)

	numChildren := len(n.children)
	w.Uvarint(uint64(numChildren))

	// Avoid allocating keys entirely if the node doesn't have any children.
	if numChildren == 0 {
		return w.b
	}

	// By allocating BranchFactorLargest rather than [numChildren], this slice
	// is allocated on the stack rather than the heap. BranchFactorLargest is
	// at least [numChildren] which avoids memory allocations.
	keys := make([]byte, numChildren, BranchFactorLargest)
	i := 0
	for k := range n.children {
		keys[i] = k
		i++
	}

	// Ensure that the order of entries is correct.
	slices.Sort(keys)
	for _, index := range keys {
		entry := n.children[index]
		w.Uvarint(uint64(index))
		w.Key(entry.compressedKey)
		w.ID(entry.id)
		w.Address(entry.diskAddr) // how to know diskAddr before adding child?
		w.Bool(entry.hasValue)
	}

	return w.b
}

// TODO: remove this
func (w *codecWriter) Address(v diskAddress) {
	diskBytes := v.bytes()
	//fmt.Print(diskBytes)
	w.b = append(w.b, diskBytes[:]...)
}

// Assumes [n] is non-nil.
func decodeDBNode_disk(b []byte, n *dbNode) error {
	// make a codecReader struct with the given byte sequence
	r := codecReader{
		b:    b,
		copy: true,
	}

	// if bytes exists and no errors then continue
	var err error
	n.value, err = r.MaybeBytes()
	if err != nil {
		return err
	}

	numChildren, err := r.Uvarint()
	if err != nil {
		return err
	}
	if numChildren > uint64(BranchFactorLargest) {
		return errTooManyChildren
	}

	n.children = make(map[byte]*child, numChildren)
	var previousChild uint64
	for i := uint64(0); i < numChildren; i++ {
		index, err := r.Uvarint()
		if err != nil {
			return err
		}
		if (i != 0 && index <= previousChild) || index > math.MaxUint8 {
			return errChildIndexTooLarge
		}
		previousChild = index

		compressedKey, err := r.Key()
		if err != nil {
			return err
		}
		childID, err := r.ID()
		if err != nil {
			return err
		}
		// TODO: just do child entry address bytes instead
		// make sure 16 bytes
		addr, err := r.Address()
		if err != nil {
			return err
		}
		hasValue, err := r.Bool()
		if err != nil {
			return err
		}
		n.children[byte(index)] = &child{
			compressedKey: compressedKey,
			id:            childID,
			hasValue:      hasValue,
			diskAddr:      addr,
		}
	}
	if len(r.b) != 0 {
		return errExtraSpace
	}
	return nil
}

// TODO: remove this
func (r *codecReader) Address() (diskAddress, error) {
	// check 16 bytes

	offset := int64(binary.BigEndian.Uint64(r.b[:8]))
	//fmt.Print(offset)
	if offset <= 0 {
		return diskAddress{}, io.ErrUnexpectedEOF
	}

	r.b = r.b[8:]
	size := int64(binary.BigEndian.Uint64(r.b[:8]))
	//fmt.Print(size)
	if size <= 0 {
		return diskAddress{}, io.ErrUnexpectedEOF
	}
	r.b = r.b[8:]
	return diskAddress{offset: offset, size: size}, nil
}
