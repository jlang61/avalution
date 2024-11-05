// translation of merkle.rs to golang: https://github.com/ava-labs/firewood/blob/5e9db421cd195740d2269aea49548e97c7104f3b/firewood/src/merkle.rs
// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.


// Note that I've made some assumptions and simplifications in the translation process:

// 1. I've used a simple `MerkleError` struct instead of the `thiserror` crate.
// 2. I've assumed the existence of `Node`, `TrieReader`, `LeafNode`, `BranchNode`, and other types that were not fully defined in the original code.
// 3. The `getHelper` function is not complete, as it depends on other types and functions not provided in the original snippet.
// 4. I've used `github.com/pkg/errors` for error handling, which is a common practice in Go.
// 5. Some Rust-specific constructs (like `Arc`) have been omitted, as Go has its own memory management.


package merkle

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type Key []byte
type Value []byte

type MerkleError struct {
	msg string
}

func (e *MerkleError) Error() string {
	return e.msg
}

var (
	ErrEmpty             = &MerkleError{msg: "can't generate proof for empty node"}
	ErrReadOnly          = &MerkleError{msg: "read only"}
	ErrNotBranchNode     = &MerkleError{msg: "node not a branch node"}
	ErrParentLeafBranch  = &MerkleError{msg: "parent should not be a leaf branch"}
	ErrUnsetInternal     = &MerkleError{msg: "removing internal node references failed"}
	ErrBinarySerdeError  = &MerkleError{msg: "merkle serde error"}
	ErrUTF8Error         = &MerkleError{msg: "invalid utf8"}
	ErrNodeNotFound      = &MerkleError{msg: "node not found"}
)

// nibblesFormatter converts a set of nibbles into a printable string
// panics if there is a non-nibble byte in the set
func nibblesFormatter(nib []byte) string {
	var sb strings.Builder
	for _, c := range nib {
		if c > 15 {
			panic("requires nibbles")
		}
		sb.WriteByte("0123456789abcdef"[c])
	}
	return sb.String()
}

func writeAttributes(writer io.Writer, node Node, value []byte) error {
	if len(node.PartialPath()) > 0 {
		if _, err := fmt.Fprintf(writer, " pp=%s", nibblesFormatter(node.PartialPath())); err != nil {
			return err
		}
	}
	if len(value) > 0 {
		if isAlphanumeric(value) {
			if _, err := fmt.Fprintf(writer, " val=%s", string(value)); err != nil {
				return err
			}
		} else {
			hexVal := hex.EncodeToString(value)
			if len(hexVal) > 6 {
				if _, err := fmt.Fprintf(writer, " val=%.6s...", hexVal); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(writer, " val=%s", hexVal); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isAlphanumeric(s []byte) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
}

// getHelper returns the value mapped to by `key` in the subtrie rooted at `node`.
func getHelper(nodestore TrieReader, node Node, key []byte) (*Node, error) {
	// 4 possibilities for the position of the `key` relative to `node`:
	// 1. The node is at `key`
	// 2. The key is above the node (i.e. its ancestor)
	// 3. The key is below the node (i.e. its descendant)
	// 4. Neither is an ancestor of the other
	pathOverlap := PrefixOverlap(key, node.PartialPath())
	uniqueKey := pathOverlap.UniqueA
	uniqueNode := pathOverlap.UniqueB

	if len(uniqueNode) > 0 {
		// Case (2) or (4)
		return nil, nil
	}

	if len(uniqueKey) == 0 {
		// 1. The node is at `key`
		return &node, nil
	}

	// 3. The key is below the node (i.e. its descendant)
	childIndex := uniqueKey[0]
	remainingKey := uniqueKey[1:]

	switch n := node.(type) {
	case *LeafNode:
		return nil, nil
	case *BranchNode:
		if int(childIndex) >= len(n.Children) {
			return nil, errors.New("index out of bounds")
		}
		child := n.Children[childIndex]
		// ... (rest of the implementation)
	default:
		return nil, errors.New("unknown node type")
	}
}
