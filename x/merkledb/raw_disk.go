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

	// "github.com/hashicorp/golang-lru"

	"github.com/dgraph-io/ristretto"

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
	dm     *diskMgr
	config Config
	hasher Hasher
	cache  *ristretto.Cache
}

func newRawDisk(dir string, fileName string, hasher Hasher, config Config) (*rawDisk, error) {
	dm, err := newDiskManager(nil, dir, fileName)
	if err != nil {
		return nil, err
	}
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,     // Number of keys to track frequency (higher = better hit rate)
		MaxCost:     1 << 30, // Maximum cost in bytes (adjust as needed)
		BufferItems: 64,      // Number of keys per eviction buffer
	})
	// correctly read rootId from the header
	return &rawDisk{dm: dm, hasher: hasher, config: config, cache: cache}, nil
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
	parentKey := Key{}
	if newRootNode.value.HasValue() {
		log.Printf("Root node %v with key {%v, %s}", rootDiskAddr, changes.rootChange.after.Value().key.length, changes.rootChange.after.Value().key.value)
		parentKey = changes.rootChange.after.Value().key
	}
	for token, child := range newRootNode.children {
		totalKeyBytes := append(parentKey.Bytes(), token)
		totalKeyBytes = append(totalKeyBytes, child.compressedKey.Bytes()...)
		totalKey := ToKey(totalKeyBytes)

		// add the child diskaddr to the array remainingNodes
		diskAddressKey := diskAddressWithKey{addr: &child.diskAddr, key: totalKey}
		remainingNodes = append(remainingNodes, diskAddressKey)

		log.Printf("Token of %v with child compressed key %v", token, child.compressedKey.length)
		log.Printf("Child with key %v with parent key %v", totalKey.length, parentKey.length)
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

			log.Printf("Child with key %v with parent key %v", totalKey.length, parentKey.length)
		}
		// remove the node from remainingNodes array
		remainingNodes = remainingNodes[1:]

	}
	return nil

}

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	// change the rootnode's diskaddress to on file diskaddress if it exists
	// iterate through the entire tree

	// rootnode diskaddress rootchange.before
	// nodes.children.diskaddress - missing
	var keys []Key

	for k := range changes.nodes {
		keys = append(keys, k)
	}

	// sort the keys by length, then start at the longest keys (leaf nodes)
	// sorting longest to shortest
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].length > keys[j].length
	})

	totalLenBytes := 0
	for _, nodes := range changes.nodes {
		if nodes.after != nil {
			totalLenBytes += len(encodeDBNode_disk(&nodes.after.dbNode))
		}
	}
	totalLenBytes += 16 * (len(changes.nodes) - 1)
	// log.Printf("Total length of bytes %d", totalLenBytes)

	// fetch the available disk address for totallenbytes
	totalDiskAddress, err := r.dm.fetch(int64(totalLenBytes))
	if err != nil {
		return err
	}

	// Start partitioning the data within the totaldiskaddress
	// Start with longest keys (children), then move up the tree
	// Start with the leaf nodes
	// Iterate through the keys
	totalOffset := 0
	childrenNodes := make(map[Key]diskAddress)
	totalBytes := make([]byte, 0)
	rootDiskAddr := diskAddress{}
	for _, k := range keys {
		// find the nodechange associated with the key
		nodeChange := changes.nodes[k]
		// filter through nodes that arent changed
		if nodeChange.after == nil {
			continue
		}

		// Ensure root is not being written twice

		// Iterate through node's children
		for token, child := range nodeChange.after.children {

			// Create the complete key (current key + compressed key of the child)
			completeKey := k.Extend(ToToken(token, BranchFactorToTokenSize[r.config.BranchFactor]))
			if child.compressedKey.length != 0 {
				completeKey = completeKey.Extend(child.compressedKey)
			}

			// CASE WHERE NODES HAVE NOT BEEN WRITTEN TO DISK
			// Check whether or not there exists a value for the child in the map
			if childrenNodes[completeKey] != (diskAddress{}) {
				// If there is a value, set the disk address of the child to the value in the map
				child.diskAddr = childrenNodes[completeKey]
			}
			// IF THE CHILDREN ARE ALREADY WRITTEN TO DISK, THEY SHOULD HAVE A DISKADDRESS ASSOCIATED WITH THEM ALREADY
			// THEREFORE WE CAN SKIP THIS STEP
		}
		// check to ensure that all of its children have disk addresses
		for _, child := range nodeChange.after.children {
			// Check remainingNodes actually have disk addresses
			if child.diskAddr == (diskAddress{}) {
				return errors.New("regular node child disk address missing")
			}
		}
		nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
		diskAddr := diskAddress{totalDiskAddress.offset + int64(totalOffset), int64(len(nodeBytes))}
		totalOffset += len(nodeBytes)
		totalBytes = append(totalBytes, nodeBytes...)
		if err != nil {
			return err
		}

		nodeChange.after.dbNode.diskAddr = diskAddr
		if nodeChange.after.hasValue() {
			compositeKey := fmt.Sprintf("%s:%d", nodeChange.after.key.value, nodeChange.after.key.length)
			r.cache.Set(compositeKey, nodeChange.after.dbNode, nodeChange.after.dbNode.diskAddr.size)
		}
		// log.Print("Setting node in cache", nodeChange.after.dbNode.diskAddr)
		// If there is not a node with the key in the map, create a new map with the key being the ch
		if childrenNodes[k] == (diskAddress{}) {
			// If the node is a leaf node, compress the key and store the disk address
			key := Key{length: k.length, value: k.value}
			childrenNodes[key] = diskAddr
		}

		if nodeChange.after.key == changes.rootChange.after.Value().key {
			// writing rootNode to header
			if changes.rootChange.after.HasValue() {
				rootNode := changes.rootChange.after.Value()
				rootNodeBytes := encodeDBNode_disk(&rootNode.dbNode)
				rootDiskAddr = diskAddress{totalDiskAddress.offset + int64(totalOffset), int64(len(rootNodeBytes))}
				totalOffset += len(rootNodeBytes)
				totalBytes = append(totalBytes, rootNodeBytes...)
				if err != nil {
					return err
				}

				// iterate through cache and delete all nodes with same key value
				// as the root node
				changes.rootChange.after.Value().dbNode.diskAddr = rootDiskAddr
				if changes.rootChange.after.HasValue() {
					compositeKey := fmt.Sprintf("%s:%d", changes.rootChange.after.Value().key.value, changes.rootChange.after.Value().key.length)
					r.cache.Set(compositeKey, changes.rootChange.after.Value().dbNode, changes.rootChange.after.Value().dbNode.diskAddr.size)
				}

				// log.Print("Setting root node in cache", changes.rootChange.after.Value().dbNode.diskAddr)
				// add function that would write the root node to the disk while also updating the disk address
				if err != nil {
					return err
				}
				rootDiskAddrBytes := rootDiskAddr.bytes()
				r.dm.file.WriteAt(rootDiskAddrBytes[:], 1)

				rootKey := rootNode.key
				rootKeyByteArray := encodeKey(rootKey)


				// need to set tthe endof file to something different - current issue is that its ovelapping with end of file
				
				size, err := r.dm.file.WriteAt(rootKeyByteArray, int64(totalDiskAddress.size + totalDiskAddress.offset))
				if err != nil {
					return err
				}
				rootKeyDiskAddr := diskAddress{int64(totalDiskAddress.size + totalDiskAddress.offset), int64(size)}
				// log.Print("wrote root key to disk", rootKeyDiskAddr)
				// log.Print("total disk address", totalDiskAddress)
				rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()
				r.dm.file.WriteAt(rootKeyDiskAddrBytes[:], 17)

				// print the tree
				changes.rootChange.after.Value().dbNode.diskAddr = rootDiskAddr

				// print out a a comprehensive report on root
				// log.Print("Root key", rootKey)
				// log.Print("Root key byte array", rootKeyByteArray)
				// log.Print("Root key ", rootKeyDiskAddrBytes)
				// log.Print("Root key", rootKeyDiskAddr)
				// decodeDBNode_disk(rootNodeBytes, &rootNode.dbNode)
				// log.Print("Root node", rootNode.dbNode)
				// key, err := decodeKey(rootKeyByteArray)
				// if err != nil {
				// 	return err
				// }

				// log.Print("Root key", key)
			}

		}
	}

	// write the total bytes to the disk
	_, err = r.dm.file.WriteAt(totalBytes, totalDiskAddress.offset)
	if err != nil {
		return err
	}
	// err = r.printTree(rootDiskAddr, changes)
	if err != nil {
		return err
	}

	if err := r.dm.file.Sync(); err != nil {
		return err
	}

	// }
	if err := r.dm.file.Sync(); err != nil {
		return err
	}
	// ensuring that there are two trees, then add old one to freelist
	for _, nodeChange := range changes.nodes {
		if nodeChange.before != nil {
			r.dm.free.put(nodeChange.before.diskAddr)

		}
		if nodeChange.before != nil && nodeChange.after == nil {
			// make a new node that is the same as the old node but with has value set to false
			tempDBNode := dbNode{}
			// remove node from cache
			if nodeChange.before.hasValue() {
				compositeKey := fmt.Sprintf("%s:%d", nodeChange.before.key.value, nodeChange.before.key.length)
				if val, _ := r.cache.Get(compositeKey); val != nil {
					r.cache.Del(compositeKey)
				}
			}
			// r.cache.Set(compositeKey, changes.rootChange.after.Value().dbNode, changes.rootChange.after.Value().dbNode.diskAddr.size)

			nextBytes, err := r.dm.get(nodeChange.before.diskAddr)
			if err != nil {
				return err
			}
			err = decodeDBNode_disk(nextBytes, &tempDBNode)
			if err != nil {
				return err
			}
			tempDBNode.value = maybe.Nothing[[]byte]()
			// write the new node to disk
			nodeBytes := encodeDBNode_disk(&tempDBNode)
			// write new node at the same disk address
			_, err = r.dm.file.WriteAt(nodeBytes, nodeChange.before.diskAddr.offset)
			if err != nil {
				return err
			}

		}
	}
	return r.dm.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.dm.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	// Add a flag to check if the cache was found

	if val, found := r.cache.Get(fmt.Sprintf("%s:%d", key.value, key.length)); found {
		if val != nil {
			// If the value is found, process normally

			// Assuming val is of type dbNode, create the return node
			returnNode := &node{
				dbNode:      val.(dbNode),
				key:         key,
				valueDigest: val.(dbNode).value,
			}

			// Set the disk address from the cache entry
			returnNode.dbNode.diskAddr = val.(dbNode).diskAddr

			// You can then return the node if you wish
			return returnNode, nil
		}
	}

	// log.Printf("Getting node for key %v", key)
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
		tokenSize = BranchFactorToTokenSize[r.config.BranchFactor]
	)

	err = decodeDBNode_disk(rootBytes, &currentDbNode)
	currentDbNode.diskAddr = rootAddress
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
		// log.Printf("key %v %v, currKey %v %v", key.length, []byte(key.value), currKey.length, []byte(currKey.value))
		return nil, database.ErrNotFound //errors.New("Key doesn't match rootKey")
	}

	keyLen := currKey.length // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far

	// tempDiskAddr := diskAddress{}
	// while the entire path hasn't been matched
	for keyLen < (key.length) {
		// confirm that a child exists and grab its address before attempting to load it
		// log.Printf("Token: %v", key.Token(keyLen, tokenSize))
		// log.Printf("currentDbNode value %s", currentDbNode.value.Value())
		// log.Printf("num of children %d", len(currentDbNode.children))
		// for token, child := range currentDbNode.children {
		// 	log.Printf("Token: %v for Child: %x", (token), child.compressedKey.value)
		// }
		// log.Printf("Checking key %x", key.Token(keyLen, tokenSize))
		nextChildEntry, hasChild := currentDbNode.children[key.Token(keyLen, tokenSize)]

		keyLen += tokenSize
		if !hasChild {
			return nil, database.ErrNotFound
		}
		// log.Printf("nextChildEntry %v", nextChildEntry)
		if !key.iteratedHasPrefix(nextChildEntry.compressedKey, keyLen, tokenSize) {
			// there was no child along the path or the child that was there doesn't match the remaining path
			// return nil, errors.New("Key doesn't match an existing node")
			return nil, database.ErrNotFound

		}

		// get the next key from the current child
		currKey := ToToken(key.Token(keyLen-tokenSize, tokenSize), tokenSize)
		// log.Printf("currKey %x", currKey)
		currKey = currKey.Extend(nextChildEntry.compressedKey)
		keyLen += currKey.length - tokenSize

		// grab the next node along the path
		nextBytes, err := r.dm.get(nextChildEntry.diskAddr)
		// tempDiskAddr = currentDbNode.diskAddr
		if err != nil {
			return nil, err
		}
		err = decodeDBNode_disk(nextBytes, &currentDbNode)
		currentDbNode.diskAddr = nextChildEntry.diskAddr
		if err != nil {
			return nil, err
		}
	}
	// log.Print("found node at disk address ", tempDiskAddr)
	returnNode := &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
		//diskAddr:    tempDiskAddr,
	}

	returnNode.dbNode.diskAddr = currentDbNode.diskAddr
	// log.Print("Found node in rawdisk", returnNode.dbNode.diskAddr, returnNode.key.value)

	returnNode.setValueDigest(r.hasher)
	return returnNode, nil
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}

func (r *rawDisk) NewIterator() database.Iterator {
	panic("NewIterator not implemented")
}

func (r *rawDisk) NewIteratorWithStart(start []byte) database.Iterator {
	panic("NewIteratorWithStart not implemented")
}

func (r *rawDisk) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	panic("NewIteratorWithPrefix not implemented")
}

func (r *rawDisk) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	panic("NewIteratorWithStartAndPrefix not implemented")
}

func (r *rawDisk) close() error {
	_, err := r.dm.file.WriteAt([]byte{1}, 0)
	if err != nil {
		return err
	}
	if err := r.dm.file.Close(); err != nil {
		return err
	}

	return nil
}
