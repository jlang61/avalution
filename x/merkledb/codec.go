// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
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
	// * index - size of index
	// * child ID - 32 byte hash (avalanchego/ids/id.go, type ID [IDLen]byte, const IDLen=32)
	// * child key - returns size of key len + bytes needed
	// * bool indicating whether the child has a value - const of 1
	return uintSize(uint64(index)) + ids.IDLen + keySize(childEntry.compressedKey) + boolLen
}

// based on the implementation of encodeUint which uses binary.PutUvarint
func uintSize(value uint64) int {
	if value == 0 {
		return 1
	}
	//bits.len64(value) converts 0100 to 3/removes trailing zeros, +6/7 is to include remainder
	//ex: bits.Len64(0(x56)10000000)=8. each byte is 7 bits so 8+6/7=2 bytes req
	return (bits.Len64(value) + 6) / 7 //size in bytes
}

func keySize(p Key) int {
	//bytes needed function in key.go, byte=8
	return uintSize(uint64(p.length)) + bytesNeeded(p.length)
}

// Assumes [n] is non-nil.
func encodedDBNodeSize(n *dbNode) int {
	// * number of children
	// * bool indicating whether [n] has a value
	// * the value (optional)
	// * children
	size := uintSize(uint64(len(n.children))) + boolLen //number of children+1
	if n.value.HasValue() {
		valueLen := len(n.value.Value())
		size += uintSize(uint64(valueLen)) + valueLen //add len(valuelen) + len of value
	}
	// for each non-nil entry, we add the additional size of the child entry
	for index, entry := range n.children {
		size += childSize(index, entry) //add size of all children
	}
	return size //lenB(numchildren) + 1 + lenB(lenvalue) + lenvalue +
	//(childindex + childID + keylen + lenB(keylen) + 1)
}

// Assumes [n] is non-nil.
func encodeDBNode(n *dbNode) []byte {
	length := encodedDBNodeSize(n)
	w := codecWriter{
		b: make([]byte, 0, length), //byte slice with size:0, capacity:length
	}

	w.MaybeBytes(n.value) // ***w=1(if exist) + varint(valuelen) + value***

	numChildren := len(n.children) //count size of hashmap
	w.Uvarint(uint64(numChildren)) //***w+=varint(number of children)***

	// Avoid allocating keys entirely if the node doesn't have any children.
	if numChildren == 0 {
		return w.b //done if no children
	}

	// By allocating BranchFactorLargest rather than [numChildren], this slice
	// is allocated on the stack rather than the heap. BranchFactorLargest is
	// at least [numChildren] which avoids memory allocations.
	keys := make([]byte, numChildren, BranchFactorLargest) //keys=hashmap of size:numchildren, capacity:branchfactorlargest
	i := 0
	//copies n's children into keys
	for k := range n.children {
		keys[i] = k
		i++
	}

	// Ensure that the order of entries is correct., sort children increasing
	slices.Sort(keys)
	for _, index := range keys {
		entry := n.children[index]
		w.Uvarint(uint64(index))   //***w+=varint(child index)***
		w.Key(entry.compressedKey) //***w+=varint(len compressed key) + child compressed key***
		w.ID(entry.id)             //***w+=child id(32 byte hash)***
		w.Bool(entry.hasValue)     //***w+=1 if exist(yes)***
	}
	return w.b //finished serialization(byte slice) of node n
}

func encodeKey(key Key) []byte {
	length := uintSize(uint64(key.length)) + len(key.Bytes()) //len key + len(key value)
	w := codecWriter{
		b: make([]byte, 0, length),
	}
	w.Key(key) //add varint(len key)+key
	return w.b
}

// codec Writer struct with certain functions definitions

type codecWriter struct {
	b []byte //make an empty byte slice(dynamic sized array)
}

// bool is ???
func (w *codecWriter) Bool(v bool) {
	//everything stored in bytes, so byte of '1' or '0'
	if v {
		w.b = append(w.b, trueByte)
	} else {
		w.b = append(w.b, falseByte)
	}
}


// add varint(v)
func (w *codecWriter) Uvarint(v uint64) {
	w.b = binary.AppendUvarint(w.b, v)
}


// add ID
func (w *codecWriter) ID(v ids.ID) {
	w.b = append(w.b, v[:]...)
}

// add varint(len v)+v
func (w *codecWriter) Bytes(v []byte) {
	w.Uvarint(uint64(len(v)))
	w.b = append(w.b, v...)
}

// add value existence flag, varint(value len), and value
func (w *codecWriter) MaybeBytes(v maybe.Maybe[[]byte]) {
	hasValue := v.HasValue()
	w.Bool(hasValue) //add 1 if hasvalue, else 0
	if hasValue {
		w.Bytes(v.Value()) //add len of value(varint) and value to w
	}
}

// add varint(len key)+key
func (w *codecWriter) Key(v Key) {
	w.Uvarint(uint64(v.length))
	w.b = append(w.b, v.Bytes()...)
}

// Assumes [n] is non-nil.
func decodeDBNode(b []byte, n *dbNode) error {
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
		hasValue, err := r.Bool()
		if err != nil {
			return err
		}
		n.children[byte(index)] = &child{
			compressedKey: compressedKey,
			id:            childID,
			hasValue:      hasValue,
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
	log.Printf("len and boolen %d %d", len(r.b), boolLen)
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
		log.Printf("error: %v", err)
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
