// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"slices"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

// Representation of a node stored in the database.
type dbNode struct {
	value    maybe.Maybe[[]byte]
	children map[byte]*child //hashmap of byte to child
}

type child struct {
	compressedKey Key
	id            ids.ID
	diskAddr      diskAddress
	hasValue      bool
}

// node holds additional information on top of the dbNode that makes calculations easier to do
type node struct {
	dbNode
	key         Key
	valueDigest maybe.Maybe[[]byte]
	diskAddr    diskAddress
}

// Returns a new node with the given [key] and no value.
func newNode(key Key) *node {
	return &node{
		dbNode: dbNode{
			children: make(map[byte]*child, 2),
		},
		key: key,
	}
}

// Parse [nodeBytes] to a node and set its key to [key].
func parseNode(hasher Hasher, key Key, nodeBytes []byte) (*node, error) {
	n := dbNode{}
	if err := decodeDBNode(nodeBytes, &n); err != nil {
		return nil, err
	}
	result := &node{
		dbNode: n,
		key:    key,
	}

	result.setValueDigest(hasher)
	return result, nil
}

// decodeNodeBytes decodes the given byte array into a node without a key.
func decodeNodeBytes(nodeBytes []byte) (*node, error) {
	var n dbNode
	if err := decodeDBNode(nodeBytes, &n); err != nil {
		return nil, err
	}
	return &node{
		dbNode: n,
		// The key is not set as it's unknown.
	}, nil
}

// Returns true iff this node has a value.
func (n *node) hasValue() bool {
	return !n.value.IsNothing()
}

// Returns the byte representation of this node.
func (n *node) bytes() []byte {
	return encodeDBNode(&n.dbNode)
}

// Set [n]'s value to [val].
func (n *node) setValue(hasher Hasher, val maybe.Maybe[[]byte]) {
	n.value = val
	n.setValueDigest(hasher)
}

func (n *node) setValueDigest(hasher Hasher) {
	if n.value.IsNothing() || len(n.value.Value()) < HashLength {
		n.valueDigest = n.value
	} else {
		hash := hasher.HashValue(n.value.Value())
		n.valueDigest = maybe.Some(hash[:])
	}
}

// Adds [child] as a child of [n].
// Assumes [child]'s key is valid as a child of [n].
// That is, [n.key] is a prefix of [child.key].
func (n *node) addChild(childNode *node, tokenSize int) {
	n.addChildWithID(childNode, tokenSize, ids.Empty)
}

func (n *node) addChildWithID(childNode *node, tokenSize int, childID ids.ID) {
	n.setChildEntry(
		childNode.key.Token(n.key.length, tokenSize),
		&child{
			compressedKey: childNode.key.Skip(n.key.length + tokenSize),
			id:            childID,
			hasValue:      childNode.hasValue(),
		},
	)
}

// Adds a child to [n] without a reference to the child node.
func (n *node) setChildEntry(index byte, childEntry *child) {
	n.children[index] = childEntry
}

// Removes [child] from [n]'s children.
func (n *node) removeChild(child *node, tokenSize int) {
	delete(n.children, child.key.Token(n.key.length, tokenSize))
}

// clone Returns a copy of [n].
// Note: value isn't cloned because it is never edited, only overwritten
// if this ever changes, value will need to be copied as well
// it is safe to clone all fields because they are only written/read while one or both of the db locks are held
func (n *node) clone() *node {
	result := &node{
		key: n.key,
		dbNode: dbNode{
			value:    n.value,
			children: make(map[byte]*child, len(n.children)),
		},
		valueDigest: n.valueDigest,
	}
	for key, existing := range n.children {
		result.children[key] = &child{
			compressedKey: existing.compressedKey,
			id:            existing.id,
			hasValue:      existing.hasValue,
		}
	}
	return result
}

// Returns the ProofNode representation of this node.
func (n *node) asProofNode() ProofNode {
	pn := ProofNode{
		Key:         n.key,
		Children:    make(map[byte]ids.ID, len(n.children)),
		ValueOrHash: maybe.Bind(n.valueDigest, slices.Clone[[]byte]),
	}
	for index, entry := range n.children {
		pn.Children[index] = entry.id
	}
	return pn
}

// encodeNode encodes the node's fields into bytes, including the dbNode.
func encodeNode(n *node) []byte {
	w := codecWriter{
		b: make([]byte, 0),
	}

	// Encode the key
	w.Key(n.key)

	// Encode the valueDigest
	w.MaybeBytes(n.valueDigest)

	// Encode the diskaddresss
	w.DiskAddress(n.diskAddr)

	// Encode the dbNode
	dbNodeBytes := encodeDBNode(&n.dbNode)
	w.Bytes(dbNodeBytes)

	// Note: diskAddr is excluded as it's not needed here.

	return w.b
}

// decodeNode decodes the given byte array into a node.
func decodeNode(b []byte) (*node, error) {
	r := codecReader{
		b:    b,
		copy: true,
	}

	// Decode the key
	key, err := r.Key()
	if err != nil {
		return nil, err
	}

	// Decode the valueDigest
	valueDigest, err := r.MaybeBytes()
	if err != nil {
		return nil, err
	}

	// Decode the disk address
	diskAddr, err := r.DiskAddress()
	if err != nil {
		return nil, err
	}

	// Decode the dbNode
	var dbNode dbNode
	nodeBytes, err := r.Bytes()
	if err != nil {
		return nil, err
	}
	if err := decodeDBNode(nodeBytes, &dbNode); err != nil {
		return nil, err
	}

	return &node{
		dbNode:      dbNode,
		key:         key,
		valueDigest: valueDigest,
		diskAddr:    diskAddr,
	}, nil
}
