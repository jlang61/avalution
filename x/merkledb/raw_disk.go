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
	return struct{}{}, nil
}

func (r *rawDisk) closeWithRoot(root maybe.Maybe[*node]) error {
	return errors.New("not implemented")
}

func (r *rawDisk) getRootKey() ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (r *rawDisk) printTree(rootDiskAddr diskAddress, changes *changeSummary) error {
	// Iterate through the tree and print out the keys and disk addresses
	var remainingNodes []diskAddressWithKey
	newRootNodeBytes, _ := r.dm.get(rootDiskAddr)
	newRootNode := &dbNode{}
	decodeDBNode_disk(newRootNodeBytes, newRootNode)
	log.Printf("Root node %v with key {%v, %s}", rootDiskAddr, changes.rootChange.after.Value().key.length, changes.rootChange.after.Value().key.value)
	parentKey := changes.rootChange.after.Value().key
	for token, child := range newRootNode.children {
		totalKeyBytes := append(parentKey.Bytes(), token)
		totalKeyBytes = append(totalKeyBytes, child.compressedKey.Bytes()...)
		totalKey := ToKey(totalKeyBytes)

		// add the child diskaddr to the array remainingNodes
		diskAddressKey := diskAddressWithKey{addr: &child.diskAddr, key: totalKey}
		remainingNodes = append(remainingNodes, diskAddressKey)

		log.Printf("Token of %v with child compressed key %v", token, child.compressedKey)
		log.Printf("Child with key %v with parent key %v", totalKey, parentKey)
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

			log.Printf("Child with key %v with parent key %v", totalKey, parentKey)
		}
		// remove the node from remainingNodes array
		remainingNodes = remainingNodes[1:]

	}
	return nil

}

// type assertion to ensure that
// pointer to rawdisk implements disk interface
//var _ Disk = &rawDisk{}

// return new error for iterator

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	var keys []Key

	for k := range changes.nodes {
		keys = append(keys, k)
	}

	// sort the keys by length, then start at the longest keys (leaf nodes)
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i].value) > len(keys[j].value)
	})

	// Create a temporary map of remainingNodes to store the disk address and compressed key of the remainingNodes
	childrenNodes := make(map[Key]diskAddress)

	for _, k := range keys {
		nodeChange := changes.nodes[k]
		if nodeChange.after == nil {
			continue
		}

		// Ensure root is not being written twice
		if changes.rootChange.after.HasValue() {
			if nodeChange.after.key == changes.rootChange.after.Value().key {
				continue
			}
		}

		// Iterate through remainingNodes in the node
		for token, child := range nodeChange.after.children {
			// Create the complete key (current key + compressed key of the child)
			completeKeyBytes := append(k.Bytes(), token)
			completeKeyBytes = append(completeKeyBytes, child.compressedKey.Bytes()...)
			completeKey := ToKey(completeKeyBytes)
			log.Printf("Creating child's completekey %v for parent %v", completeKey, k)
			// Check whether or not there exists a value for the child in the map
			if childrenNodes[completeKey] != (diskAddress{}) {
				log.Printf("Adding a diskaddress from map to remainingNodes")
				// If there is a value, set the disk address of the child to the value in the map
				child.diskAddr = childrenNodes[completeKey]
			}
		}

		nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
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
	if changes.rootChange.after.HasValue() {
		// Adding remainingNodes to the root node
		k := changes.rootChange.after.Value().key
		for token, child := range changes.rootChange.after.Value().children {
			// tokenInt := int(token)
			// tokenSize := len(string(token))
			// // Create the complete key (current key + compressed key of the child)
			// completeKey := Key{length: k.length + tokenSize + child.compressedKey.length, value: k.value + strconv.Itoa(tokenInt) +child.compressedKey.value}
			completeKeyBytes := append(k.Bytes(), token)
			completeKeyBytes = append(completeKeyBytes, child.compressedKey.Bytes()...)
			completeKey := ToKey(completeKeyBytes)
			log.Printf("Creating root node's child's completekey %v for parent %v", completeKey, k)
			// Check whether or not there exists a value for the child in the map
			if childrenNodes[completeKey] != (diskAddress{}) {
				// If there is a value, set the disk address of the child to the value in the map
				child.diskAddr = childrenNodes[completeKey]
			}
		}
		for _, child := range changes.rootChange.after.Value().children {
			// Check remainingNodes actually have disk addresses
			if child.diskAddr == (diskAddress{}) {
				return errors.New("child disk address missing")
			} else {
				log.Printf("Child disk address is %v", child.diskAddr)
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

		// writing root key to header
		rootKey := rootNode.key.Bytes()
		rootKeyDiskAddr, err := r.dm.write(rootKey[:])
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

	// for k, d := range tempremainingNodes {
	// 	log.Printf("Key is %v", k)
	// 	log.Printf("Disk address is %v", d)
	// }

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

	rootBytes, err := r.dm.get(rootAddress)
	if err != nil {
		return nil, err
	}

	var (
		// all node paths start at the root
		currentDbNode = dbNode{}
		// tokenSize   = t.getTokenSize()
		tokenSize = 8
	)

	err = decodeDBNode_disk(rootBytes, &currentDbNode)
	if err != nil {
		return nil, err
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
	currKeyString := string(rootKeyBytes[:])
	currKey := Key{length: len(currKeyString) * 8, value: currKeyString}
	// currKey, err := decodeKey(rootKeyBytes[:])
	// log.Printf(currKey.value
	// if err != nil {
	// 	return nil, err
	// }

	if !key.HasPrefix(currKey) {
		return nil, errors.New("Key doesn't match rootkey")
	}

	keylen := currKey.length // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far
	// log.Printf("Key length %d", currKey.length)
	// log.Printf("Key length %d", keylen)
	// log.Printf("Key length %d", key.length)

	// while the entire path hasn't been matched
	for keylen < (key.length) {
		// confirm that a child exists and grab its address before attempting to load it
		log.Printf("Token: %v", key.Token(keylen, tokenSize))
		log.Printf("currentDbNode value %s", currentDbNode.value.Value())
		log.Printf("num of children %d", len(currentDbNode.children))
		for token, child := range currentDbNode.children {
			log.Printf("Token: %v for Child: %s", (token), child.compressedKey.value)
		}
		nextChildEntry, hasChild := currentDbNode.children[key.Token(keylen, tokenSize)]

		keylen += tokenSize
		if !hasChild {
			return nil, errors.New("Key not found in node's children")
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
