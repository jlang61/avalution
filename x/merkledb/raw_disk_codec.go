// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

const (
	boolLen   = 1
	trueByte  = 1
	falseByte = 0
	addrSize  = 16 //looks like Aaron B did a 16 byte array for address
)

var (
	trueBytes  = []byte{trueByte}
	falseBytes = []byte{falseByte}

	errChildIndexTooLarge = errors.New("invalid child index. Must be less than branching factor")
	errLeadingZeroes      = errors.New("varint has leading zeroes")
	errInvalidBool        = errors.New("decoded bool is neither true nor false")
	errNonZeroKeyPadding  = errors.New("key partial byte should be padded with 0s")
	errExtraSpace         = errors.New("trailing buffer space")
	errIntOverflow        = errors.New("value overflows int")
	errTooManyChildren    = errors.New("too many children")
)

func childSize(index byte, childEntry *child) int {
	// * index
	// * child ID
	// * child key
	// * Now need a disk address
	// * bool indicating whether the child has a value
	return uintSize(uint64(index)) + ids.IDLen + keySize(childEntry.compressedKey) + addrSize + boolLen
}

// based on the implementation of encodeUint which uses binary.PutUvarint
func uintSize(value uint64) int {
	if value == 0 {
		return 1
	}
	return (bits.Len64(value) + 6) / 7
}

func keySize(p Key) int {
	return uintSize(uint64(p.length)) + bytesNeeded(p.length)
}

// Assumes [n] is non-nil.
func encodedDBNodeSize(n *diskNode) int {
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
		size += childSize(index, entry)
	}
	return size
}

// Assumes [n] is non-nil.
func encodeDBNode(n *diskNode) []byte {
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

func encodeKey(key Key) []byte {
	length := uintSize(uint64(key.length)) + len(key.Bytes())
	w := codecWriter{
		b: make([]byte, 0, length),
	}
	w.Key(key)
	return w.b
}

// codec Writer struct with certain functions definitions

type codecWriter struct {
	b []byte
}

// bool is ???
func (w *codecWriter) Bool(v bool) {
	if v {
		w.b = append(w.b, trueByte)
	} else {
		w.b = append(w.b, falseByte)
	}
}

// this appends the uvarint in an encoded format to the byte stream of the codec writer?
func (w *codecWriter) Uvarint(v uint64) {
	w.b = binary.AppendUvarint(w.b, v)
}

func (w *codecWriter) ID(v ids.ID) {
	w.b = append(w.b, v[:]...)
}

func (w *codecWriter) Address(v diskAddress) {
	diskBytes := v.bytes()
	fmt.Print(diskBytes)
	w.b = append(w.b, diskBytes[:]...)
}

func (w *codecWriter) Bytes(v []byte) {
	w.Uvarint(uint64(len(v)))
	w.b = append(w.b, v...)
}

func (w *codecWriter) MaybeBytes(v maybe.Maybe[[]byte]) {
	hasValue := v.HasValue()
	w.Bool(hasValue)
	if hasValue {
		w.Bytes(v.Value())
	}
}

func (w *codecWriter) Key(v Key) {
	w.Uvarint(uint64(v.length))
	w.b = append(w.b, v.Bytes()...)
}

// Assumes [n] is non-nil.
func decodeDBNode(b []byte, n *diskNode) error {
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

// decode key
func decodeKey(b []byte) (Key, error) {
	r := codecReader{
		b:    b,
		copy: true,
	}
	key, err := r.Key()
	if err != nil {
		return Key{}, err
	}
	if len(r.b) != 0 {
		return Key{}, errExtraSpace
	}
	return key, nil
}

// this is the codecReader struct that is used to decode the serialization
// it is an array of bytes
// copy references mean copying the things at references? or just references themselves?
type codecReader struct {
	b []byte
	// copy is used to flag to the reader if it is required to copy references
	// to [b].
	copy bool
}

func (r *codecReader) Bool() (bool, error) {
	if len(r.b) < boolLen {
		return false, io.ErrUnexpectedEOF
	}
	boolByte := r.b[0]
	// if its bigger than trueByte (1) it is not boolean
	if boolByte > trueByte {
		return false, errInvalidBool
	}
	// remove the byte after read
	r.b = r.b[boolLen:]
	return boolByte == trueByte, nil
}

// this is to decode a varint of varying length
func (r *codecReader) Uvarint() (uint64, error) {
	length, bytesRead := binary.Uvarint(r.b)
	if bytesRead <= 0 {
		return 0, io.ErrUnexpectedEOF
	}

	// To ensure decoding is canonical, we check for leading zeroes in the
	// varint.
	// The last byte of the varint includes the most significant bits.
	// If the last byte is 0, then the number should have been encoded more
	// efficiently by removing this leading zero.
	if bytesRead > 1 && r.b[bytesRead-1] == 0x00 {
		return 0, errLeadingZeroes
	}

	r.b = r.b[bytesRead:]
	return length, nil
}

// function to get an ID
func (r *codecReader) ID() (ids.ID, error) {
	// if not enough bytes left then no ID
	if len(r.b) < ids.IDLen {
		return ids.Empty, io.ErrUnexpectedEOF
	}
	id := ids.ID(r.b[:ids.IDLen])
	//extend the original byte array by re slicing it
	r.b = r.b[ids.IDLen:]
	return id, nil
}

func (r *codecReader) Address() (diskAddress, error) {
	offset := int64(binary.BigEndian.Uint64(r.b)) //????????
	if offset <= 0 {
		return diskAddress{}, io.ErrUnexpectedEOF
	}

	r.b = r.b[8:]
	size := int64(binary.BigEndian.Uint64(r.b))
	r.b = r.b[8:]
	return diskAddress{offset: offset, size: size}, nil
}

// based on the length read the actual value bytes
func (r *codecReader) Bytes() ([]byte, error) {
	length, err := r.Uvarint()
	if err != nil {
		return nil, err
	}

	if length > uint64(len(r.b)) {
		return nil, io.ErrUnexpectedEOF
	}
	result := r.b[:length]
	if r.copy {
		result = bytes.Clone(result)
	}

	r.b = r.b[length:]
	return result, nil
}

// based on r.Bool(), read the bytes and wrap it around an option type
func (r *codecReader) MaybeBytes() (maybe.Maybe[[]byte], error) {
	if hasValue, err := r.Bool(); err != nil || !hasValue {
		return maybe.Nothing[[]byte](), err
	}

	bytes, err := r.Bytes()
	return maybe.Some(bytes), err
}

// read and decode a Key
func (r *codecReader) Key() (Key, error) {
	bitLen, err := r.Uvarint()
	if err != nil {
		return Key{}, err
	}
	if bitLen > math.MaxInt {
		return Key{}, errIntOverflow
	}

	result := Key{
		length: int(bitLen),
	}
	byteLen := bytesNeeded(result.length)
	if byteLen > len(r.b) {
		return Key{}, io.ErrUnexpectedEOF
	}
	if result.hasPartialByte() {
		// Confirm that the padding bits in the partial byte are 0.
		// We want to only look at the bits to the right of the last token,
		// which is at index length-1.
		// Generate a mask where the (result.length % 8) left bits are 0.
		paddingMask := byte(0xFF >> (result.length % 8))
		if r.b[byteLen-1]&paddingMask != 0 {
			return Key{}, errNonZeroKeyPadding
		}
	}
	result.value = string(r.b[:byteLen])

	r.b = r.b[byteLen:]
	return result, nil
}
