// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/ava-labs/avalanchego/ids"
)

const (
	addrSize = 16
)

var (
	errInvalidDiskAddrOffset = errors.New("Unable to get disk addr offset")
	errInvalidDiskAddrSize   = errors.New("Unable to get disk addr size")
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
func encodedDBNodeSize_disk(n *dbNode) int {
	// * number of children
	// * bool indicating whether [n] has a value
	// * the value (optional)
	// * children
	size := uintSize(uint64(len(n.children))) + boolLen
	if n.value.HasValue() {
		valueLen := len(n.value.Value())
		size += uintSize(uint64(valueLen)) + valueLen
	}
	// for each non-nil entry, we add the additional size of the child entry
	for index, entry := range n.children {
		size += childSize_disk(index, entry)
	}
	return size
}

// Assumes [n] is non-nil.
func encodeDBNode_disk(n *dbNode) []byte {
	length := encodedDBNodeSize_disk(n)
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
		// add Disk Address here
		diskBytes := entry.diskAddr.bytes()
		w.b = append(w.b, diskBytes[:]...)
		//w.Address(entry.diskAddr)
		w.Bool(entry.hasValue)
	}

	return w.b
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
		// Read Disk Address from byte stream
		offset := int64(binary.BigEndian.Uint64(r.b[:8]))
		if offset <= 0 { // change this condition?
			return errInvalidDiskAddrOffset // change error code?
		}
		r.b = r.b[8:]
		size := int64(binary.BigEndian.Uint64(r.b[:8]))
		if size <= 0 { // change this condition?
			return errInvalidDiskAddrSize // change error code?
		}
		r.b = r.b[8:]
		addr := diskAddress{offset: offset, size: size}

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
