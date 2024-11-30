// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	// "log"
	"sort"

	"github.com/ava-labs/avalanchego/utils/maybe"
)

// diskAddress specifies a byte array stored on disk
type diskAddress struct {
	offset int64
	size   int64
}

func (r diskAddress) end() int64 { // i think its the end idx for root root might b here ? at
	return r.offset + r.size
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
		for _, child := range nodeChange.after.children {
			// Create the complete key (current key + compressed key of the child)
			completeKey := Key{length: k.length + child.compressedKey.length, value: k.value + child.compressedKey.value}

			log.Printf("Creating completekey %v for parent %v", completeKey, k)
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
		for _, child := range changes.rootChange.after.Value().children {
			// Create the complete key (current key + compressed key of the child)
			completeKey := Key{length: k.length + child.compressedKey.length, value: k.value + child.compressedKey.value}

			log.Printf("Creating completekey %v for parent %v", completeKey, k)
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
		// err = r.printTree(rootDiskAddr, changes)
		// if err != nil {
		// 	return err
		// }
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
	// set the metadata with getheader
	metadata, err := r.dm.getHeader()
	if err != nil {
		return nil, err
	}

	// instatiate the address of the root node

	rootAddress := diskAddress{
		offset: int64(binary.BigEndian.Uint64(metadata[0:8])),
		size:   int64(binary.BigEndian.Uint64(metadata[8:16])),
	}

	// get the root node
	rootBytes, err := r.dm.get(rootAddress)
	if err != nil {
		return nil, err
	}

	// log.Printf("key length %v", key.length)

	var (
		// all node paths start at the root
		currentDbNode = dbNode{}
		// tokenSize   = t.getTokenSize()
		tokenSize = 8
	)

	// decode the root node\

	err = decodeDBNode_disk(rootBytes, &currentDbNode)
	if err != nil {
		// log.Println("Error decoding root node")
		return nil, err
	}
	// log.Printf("After decode rootnode")
	

	// errrrrr?
	rootKeyAddr := diskAddress{
		offset: int64(binary.BigEndian.Uint64(metadata[16:24])),
		size:   int64(binary.BigEndian.Uint64(metadata[24:32])),
	}

	rootKeyBytes, err := r.dm.get(rootKeyAddr)
	if err != nil {
		return nil, err
	}
	// log.Printf("After get rootkey %v", rootKeyBytes)
	// log.Printf("String version of read rootkey %v", string(rootKeyBytes[:]))
	currKeyString := string(rootKeyBytes[:])
	currKey := Key{length: len(currKeyString) * 8, value: currKeyString}
	if err != nil {
		return nil, err
	}

	// log.Printf("After decode rootkey")
	if !key.HasPrefix(currKey) {
		return nil, errors.New("Key doesn't match rootkey")
	}
	// log.Printf("After check rootkey")
	// log.Printf("currkey length %v", currKey.length)
	keylen := currKey.length  // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far
	// while the entire path hasn't been matched
	// log.Printf("keylen %v", keylen)
	// log.Printf("key length %v", key.length/8)

	// // check the current node 
	// if keylen == key.length/8{
	// 	// log.Printf("key: %v", key)
	// 	// log.Printf("currkey: %v", currKey)
	// 	// if the key length is equal to the length of the key, then the current node is the node we are looking for
	// 	if hasValue && key == currKey {
	// 		return &node{
	// 			dbNode:      currentDbNode,
	// 			key:         key,
	// 			valueDigest: currentDbNode.value,
	// 		}, nil
	// 	}
	// }

	for keylen <= (key.length/8) {
		// confirm that a child exists and grab its address before attempting to load it
		nextChildEntry, hasChild := currentDbNode.children[key.Token(currKey.length, tokenSize)]

		if !hasChild || !key.iteratedHasPrefix(nextChildEntry.compressedKey, keylen, tokenSize) {
			// there was no child along the path or the child that was there doesn't match the remaining path
			return nil, errors.New("Key not found in node's children")
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
	// log.Printf("After loop")
	log.Printf("current node %v", currentDbNode)
	log.Printf("key %v", key)
	log.Printf("value digest %v", currentDbNode.value)
	return &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
	}, nil
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}
