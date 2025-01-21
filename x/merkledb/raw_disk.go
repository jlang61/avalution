// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

var _ Disk = &rawDisk{}

// diskAddress specifies a byte array stored on disk
type diskAddress struct {
	offset int64
	size   int64
}

// func (r diskAddress) end() int64 {
// 	return r.offset + r.size
// }

type diskAddressWithKey struct {
	addr *diskAddress
	key  Key
}

func (r diskAddress) bytes() [16]byte {
	var bytes [16]byte
	binary.BigEndian.PutUint64(bytes[:8], uint64(r.offset))
	binary.BigEndian.PutUint64(bytes[8:], uint64(r.size))
	return bytes
}

func (r *diskAddress) decode(diskAddressBytes []byte) (int64, int64) {

	offset := int64(binary.BigEndian.Uint64(diskAddressBytes))
	size := int64(binary.BigEndian.Uint64(diskAddressBytes[8:]))
	r.offset = offset
	r.size = size
	return offset, size
}

type rawDisk struct {
	// [0] = shutdownType
	// [1,17] = rootKey raw file offset
	// [18,] = node store
	dm *diskMgr
	//hasher Hasher
}

func newRawDisk(dir string, fileName string) (*rawDisk, error) {
	dm, err := newDiskManager(nil, dir, fileName)
	if err != nil {
		return nil, err
	}
	// correctly read rootId from the header
	return &rawDisk{dm: dm}, nil
}

func (r *rawDisk) getShutdownType() ([]byte, error) {

	var shutdownType [1]byte
	_, err := r.dm.file.ReadAt(shutdownType[:], 0)
	if err != nil {
		return nil, err
	}
	return shutdownType[:], nil
}

func (r *rawDisk) setShutdownType(shutdownType []byte) error {
	if len(shutdownType) != 1 {
		return fmt.Errorf("invalid shutdown type with length %d", len(shutdownType))
	}
	_, err := r.dm.file.WriteAt(shutdownType, 0)
	return err
}

func (r *rawDisk) clearIntermediateNodes() error {
	return errors.New("clear intermediate nodes and rebuild not supported for raw disk")
}

func (r *rawDisk) Compact(start, limit []byte) error {
	return errors.New("not implemented")
}
func (r *rawDisk) HealthCheck(ctx context.Context) (interface{}, error) {
	// 1) Ensure the disk manager and file exist
	if r.dm == nil || r.dm.file == nil {
		return nil, database.ErrClosed
	}
	// 2) Attempt a small read on metadata to confirm the file is accessible
	header := make([]byte, metaSize)
	if _, err := r.dm.file.ReadAt(header, 1); err != nil {
		return nil, fmt.Errorf("rawDisk HealthCheck: could not read metadata: %w", err)
	}
	// If all checks pass:
	return nil, nil
	//return struct{}{}, nil
}


func (r *rawDisk) closeWithRoot(root maybe.Maybe[*node]) error {
	return r.close()
}

func (r *rawDisk) getRootKey() ([]byte, error) {
	rootKeyDiskAddrBytes, err := r.dm.get(diskAddress{offset: 17, size: 16})
	if err != nil {
		return nil, err
	}
	rootKeyDiskAddr := diskAddress{}
	rootKeyDiskAddr.decode(rootKeyDiskAddrBytes)
	rootKeyBytes, err := r.dm.get(rootKeyDiskAddr)
	if err != nil {
		return nil, err
	}
	return rootKeyBytes, nil
}

func (r *rawDisk) printTree(rootDiskAddr diskAddress, changes *changeSummary) error {
	// Iterate through the tree and print out the keys and disk addresses
	var remainingNodes []diskAddressWithKey
	newRootNodeBytes, _ := r.dm.get(rootDiskAddr)
	newRootNode := &dbNode{}
	decodeDBNode_disk(newRootNodeBytes, newRootNode)
	// log.Printf("Root node %v with key {%v, %s}", rootDiskAddr, changes.rootChange.after.Value().key.length, changes.rootChange.after.Value().key.value)
	parentKey := changes.rootChange.after.Value().key
	for token, child := range newRootNode.children {
		totalKeyBytes := append(parentKey.Bytes(), token)
		totalKeyBytes = append(totalKeyBytes, child.compressedKey.Bytes()...)
		totalKey := ToKey(totalKeyBytes)

		// add the child diskaddr to the array remainingNodes
		diskAddressKey := diskAddressWithKey{addr: &child.diskAddr, key: totalKey}
		remainingNodes = append(remainingNodes, diskAddressKey)

		// log.Printf("Token of %v with child compressed key %v", token, child.compressedKey)
		// log.Printf("Child with key %v with parent key %v", totalKey, parentKey)
	}
	for _, diskAddressKey := range remainingNodes {
		// iterate through the first instance of the remainingNodes
		// and print out the key and disk address
		diskAddress := diskAddressKey.addr
		parentKey := diskAddressKey.key
		childBytes, err := r.dm.get(*diskAddress)
		if err != nil {
			return err
		}
		childNode := &dbNode{}
		decodeDBNode_disk(childBytes, childNode)
		for token, child := range childNode.children {
			totalKeyBytes := append(parentKey.Bytes(), token)
			totalKeyBytes = append(totalKeyBytes, child.compressedKey.Bytes()...)
			totalKey := ToKey(totalKeyBytes)

			diskAddressKey := diskAddressWithKey{addr: &child.diskAddr, key: totalKey}
			remainingNodes = append(remainingNodes, diskAddressKey)

			// log.Printf("Child with key %v with parent key %v", totalKey, parentKey)
		}
		// remove the node from remainingNodes array
		remainingNodes = remainingNodes[1:]

	}
	return nil

}

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	var keys []Key

	for k := range changes.nodes {
		keys = append(keys, k)
	}

	// sort the keys by length, then start at the longest keys (leaf nodes)
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].length > keys[j].length
	})

	// Create a temporary map of remainingNodes to store the disk address and compressed key of the remainingNodes
	childrenNodes := make(map[Key]diskAddress)

	// ITERATES THROUGH ALL NODES EXCEPT THE ROOT
	// STARTS WITH CHILDRENS THEN MOVES TO PARENTS
	// EACH PARENT CHECKS THEIR CHILDREN'S KEY THEN ENSURES POINTERS PROPERLY WORK
	// Iterate through the keys
	for _, k := range keys {
		// find the nodechange associated with the key
		nodeChange := changes.nodes[k]
		// filter through nodes that arent changed
		if nodeChange.after == nil {
			continue
		}

		// Ensure root is not being written twice
		if changes.rootChange.after.HasValue() {
			if nodeChange.after.key == changes.rootChange.after.Value().key {
				continue
			}
		}

		// Iterate through node's children
		for token, child := range nodeChange.after.children {
			// Create the complete key (current key + compressed key of the child)

			// PREVIOUS IMPLEMENTATION:
			// completeKeyBytes := append(k.Bytes(), token)
			// completeKeyBytes = append(completeKeyBytes, child.compressedKey.Bytes()...)
			// completeKey := ToKey(completeKeyBytes)

			// CURRENT IMPLEMENTATION
			completeKey := k.Extend(ToToken(token, 4))
			if child.compressedKey.length != 0 {
				completeKey = completeKey.Extend(child.compressedKey)
			}

			// Check whether or not there exists a value for the child in the map
			if childrenNodes[completeKey] != (diskAddress{}) {
				// log.Printf("Adding a diskaddress from map to remainingNodes")
				// If there is a value, set the disk address of the child to the value in the map
				child.diskAddr = childrenNodes[completeKey]
				// log.Printf("assigned child %v to parent %v", completeKey, k)
			}
		}
		// check to ensure that all of its children have disk addresses
		for _, child := range nodeChange.after.children {
			// Check remainingNodes actually have disk addresses
			if child.diskAddr == (diskAddress{}) {
				// log.Print("child ", child)
				return errors.New("child disk address missing")
			} else {
				// log.Printf("Child disk address is %v", child.diskAddr)
			}
		}
		nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
		// log.Printf("Wrote key %v to disk", k)
		diskAddr, err := r.dm.write(nodeBytes)
		if err != nil {
			return err
		}

		// If there is not a node with the key in the map, create a new map with the key being the ch
		if childrenNodes[k] == (diskAddress{}) {
			// If the node is a leaf node, compress the key and store the disk address
			key := Key{length: k.length, value: k.value}
			childrenNodes[key] = diskAddr
		}

	}
	if err := r.dm.file.Sync(); err != nil {
		return err
	}

	// ITERATES THROUGH THE ROOT NODE A FINAL TIME
	// ENSURES THAT ROOTNODE
	if changes.rootChange.after.HasValue() {
		// Adding remainingNodes to the root node
		k := changes.rootChange.after.Value().key
		for token, child := range changes.rootChange.after.Value().children {

			// PREVIOUS IMPLEMENTATION:
			// completeKeyBytes := append(k.Bytes(), token)
			// completeKeyBytes = append(completeKeyBytes, child.compressedKey.Bytes()...)
			// completeKey := ToKey(completeKeyBytes)

			// CURRENT IMPLEMENTATION
			completeKey := k.Extend(ToToken(token, 4))
			if child.compressedKey.length != 0 {
				completeKey = completeKey.Extend(child.compressedKey)
			}
			// Check whether or not there exists a value for the child in the map
			if childrenNodes[completeKey] != (diskAddress{}) {
				// If there is a value, set the disk address of the child to the value in the map
				child.diskAddr = childrenNodes[completeKey]
				// log.Printf("assigned child %v to root node  %v", completeKey, k)

			}
		}
		for _, child := range changes.rootChange.after.Value().children {
			// Check remainingNodes actually have disk addresses
			if child.diskAddr == (diskAddress{}) {
				return errors.New("child disk address missing")
			} else {
			}
		}
		// writing rootNode to header
		rootNode := changes.rootChange.after.Value()
		rootNodeBytes := encodeDBNode_disk(&rootNode.dbNode)
		rootDiskAddr, err := r.dm.write(rootNodeBytes)
		if err != nil {
			return err
		}
		rootDiskAddrBytes := rootDiskAddr.bytes()
		r.dm.file.WriteAt(rootDiskAddrBytes[:], 1)

		rootKey := rootNode.key
		rootKeyLen := rootKey.length
		rootKeyVal := rootKey.value

		// log.Printf("RootkeyVal %v", rootKeyVal)
		// log.Printf("root key %v ", rootKey)

		rooyKeyByteArray := []byte{}
		rooyKeyByteArray = append(rooyKeyByteArray, byte(rootKeyLen))
		rooyKeyByteArray = append(rooyKeyByteArray, rootKeyVal...)

		// log.Printf("RootkeyByteArray %v", rooyKeyByteArray)

		//.Bytes()
		rootKeyDiskAddr, err := r.dm.write(rooyKeyByteArray)
		if err != nil {
			return err
		}
		rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()
		r.dm.file.WriteAt(rootKeyDiskAddrBytes[:], 17)

		// print the tree
		err = r.printTree(rootDiskAddr, changes)
		if err != nil {
			return err
		}
	}
	if err := r.dm.file.Sync(); err != nil {
		return err
	}
	// ensuring that there are two trees, then add old one to freelist
	for _, nodeChange := range changes.nodes {
		if nodeChange.before != nil {
			r.dm.free.put(nodeChange.before.diskAddr)
		}
	}

	if err := r.dm.file.Sync(); err != nil {
		log.Fatalf("failed to sync data at the end: %v", err)
	}

	return r.dm.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.dm.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	metadata, err := r.dm.getHeader()
	if err != nil {
		return nil, err
	}

	rootAddress := diskAddress{
		offset: int64(binary.BigEndian.Uint64(metadata[0:8])),
		size:   int64(binary.BigEndian.Uint64(metadata[8:16])),
	}

	// log.Printf("Root address %v", rootAddress)
	rootBytes, err := r.dm.get(rootAddress)
	if err != nil {
		return nil, err
	}

	var (
		// all node paths start at the root
		currentDbNode = dbNode{}
		// tokenSize   = t.getTokenSize()
		tokenSize = 4
	)

	err = decodeDBNode_disk(rootBytes, &currentDbNode)
	if err != nil {
		return nil, database.ErrNotFound
	}

	rootKeyAddr := diskAddress{
		offset: int64(binary.BigEndian.Uint64(metadata[16:24])),
		size:   int64(binary.BigEndian.Uint64(metadata[24:32])),
	}

	rootKeyBytes, err := r.dm.get(rootKeyAddr)
	if err != nil {
		return nil, err
	}

	// log.Printf("Here")
	currKey, err := decodeKey(rootKeyBytes[:])
	if err != nil {
		return nil, err
	}

	if !key.HasPrefix(currKey) {
		return nil, errors.New("Key doesn't match rootkey")
	}

	keylen := currKey.length // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far

	// while the entire path hasn't been matched
	for keylen < (key.length) {
		// confirm that a child exists and grab its address before attempting to load it
		// log.Printf("Token: %v", key.Token(keylen, tokenSize))
		// log.Printf("currentDbNode value %s", currentDbNode.value.Value())
		// log.Printf("num of children %d", len(currentDbNode.children))
		// for token, child := range currentDbNode.children {
		// 	log.Printf("Token: %v for Child: %s", (token), child.compressedKey.value)
		// }
		nextChildEntry, hasChild := currentDbNode.children[key.Token(keylen, tokenSize)]

		keylen += tokenSize
		if !hasChild {
			return nil, database.ErrNotFound
		}

		if !key.iteratedHasPrefix(nextChildEntry.compressedKey, keylen, tokenSize) {
			// there was no child along the path or the child that was there doesn't match the remaining path
			return nil, errors.New("Key doesn't match an existing node")

		}

		// get the next key from the current child
		currKey = nextChildEntry.compressedKey
		keylen += currKey.length

		// grab the next node along the path
		nextBytes, err := r.dm.get(nextChildEntry.diskAddr)
		if err != nil {
			return nil, err
		}
		err = decodeDBNode_disk(nextBytes, &currentDbNode)
		if err != nil {
			return nil, err
		}
	}
	return &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
	}, nil
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}

func (r *rawDisk) NewIterator() database.Iterator {
	return nil
}

func (r *rawDisk) NewIteratorWithStart(start []byte) database.Iterator {
	return nil
}

func (r *rawDisk) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return nil
}

func (r *rawDisk) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	return nil
}

func (r *rawDisk) close() error {
	r.dm.file.WriteAt([]byte{1}, 0)
	if err := r.dm.file.Close(); err != nil {
		return err
	}

	return nil
}
