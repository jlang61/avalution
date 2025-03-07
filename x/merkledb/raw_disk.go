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
	// "go.opentelemetry.io/otel"
	"github.com/TwiN/gocache/v2"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/trace"
	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/dgraph-io/ristretto"
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
	dm           *diskMgr
	config       Config
	hasher       Hasher
	cache        *gocache.Cache
	deletedCache *ristretto.Cache
	debugTracer  trace.Tracer
	rootNode     *node
}

// Example usage in newRawDisk.
func newRawDisk(dir string, fileName string, hasher Hasher, config Config) (*rawDisk, error) {
	dm, err := newDiskManager(nil, dir, fileName)
	if err != nil {
		return nil, err
	}
	cache := gocache.NewCache().WithMaxSize(100000000)

	deletedCache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1 << 30,
		BufferItems: 64,
	})

	// Wrap the OTEL tracer to match the required interface.
	// Return the rawDisk instance with the wrapped tracer.
	return &rawDisk{
		dm:           dm,
		hasher:       hasher,
		config:       config,
		cache:        cache,
		deletedCache: deletedCache,
		debugTracer:  getTracerIfEnabled(config.TraceLevel, DebugTrace, config.Tracer), // Compatible with the expected interface.
	}, nil
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
	// rootKeyDiskAddrBytes, err := r.dm.get(diskAddress{offset: 17, size: 16})
	// if err != nil {
	// 	return nil, err
	// }
	// rootKeyDiskAddr := diskAddress{}
	// rootKeyDiskAddr.decode(rootKeyDiskAddrBytes)
	// rootKeyBytes, err := r.dm.get(rootKeyDiskAddr)
	// if err != nil {
	// 	return nil, err
	// }
	return r.rootNode.key.Bytes(), nil
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
	// Gather all keys from changes.
	var keys []Key
	for k := range changes.nodes {
		keys = append(keys, k)
	}

	// Sort the keys by length (longest first, for leaf nodes first).
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].length > keys[j].length
	})

	// Create a span for partitioning data (calculating totalLenBytes).
	totalLenBytes := 0

	for _, nodes := range changes.nodes {
		if nodes.after != nil {
			// Increase length by one for the padding byte per node.
			totalLenBytes += len(encodeDBNode_disk(&nodes.after.dbNode)) + 1
			totalLenBytes += 16 * len(nodes.after.children)
		}
	}

	// Check how many nodes are in the cache currenlty
	currentCacheSize := r.cache.Count()
	// Add the currentcachesize with the total number of nodes to be written
	totalCacheSize := currentCacheSize + len(changes.nodes)
	// If the total cache size is greater than the max cache size, write all the nodes on cache to disk and flush
	if totalCacheSize > r.cache.MaxSize() {
		// Write all the nodes in the cache to disk
		totalLenBytes = 0
		totalBytes := make([]byte, 0)
		for _, node := range r.cache.GetAll() {
			n := node.(dbNode)
			nodeBytes := encodeDBNode_disk(&n)
			totalBytes = append(totalBytes, nodeBytes...)
			totalLenBytes += len(nodeBytes) + 1
			totalLenBytes += 16 * len(node.(dbNode).children)
		}
		// Write the total bytes to disk
		totalDiskAddress, _ := r.dm.fetch(int64(totalLenBytes))
		_, err := r.dm.file.WriteAt(totalBytes, totalDiskAddress.offset)
		if err != nil {
			return err
		}
		// Clear the cache
		r.cache.Clear()
	}

	// Fetch the available disk address for the total length of bytes.
	totalDiskAddress, err := r.dm.fetch(int64(totalLenBytes))
	if err != nil {
		return err
	}

	// Set up variables for writing data.
	totalOffset := 0
	childrenNodes := make(map[Key]diskAddress)
	totalBytes := make([]byte, 0)
	numWritten := 0
	rootDiskAddr := diskAddress{}
	totalRootBytes := make([]byte, 0)

	// Create a span for processing the keys loop.
	for _, k := range keys {
		// Get the node change for the key.
		nodeChange := changes.nodes[k]
		// Skip nodes that haven't changed.
		if nodeChange.after == nil {
			continue
		}

		// Iterate through the node's children.
		for token, child := range nodeChange.after.children {
			// Create the complete key (current key + compressed key of the child).
			completeKey := k.Extend(ToToken(token, BranchFactorToTokenSize[r.config.BranchFactor]))
			if child.compressedKey.length != 0 {
				completeKey = completeKey.Extend(child.compressedKey)
			}

			// If the child has already been written, update its disk address.
			if childrenNodes[completeKey] != (diskAddress{}) {
				child.diskAddr = childrenNodes[completeKey]
			}
		}

		// Ensure that all children have valid disk addresses.
		for _, child := range nodeChange.after.children {
			if child.diskAddr == (diskAddress{}) {
				return errors.New("regular node child disk address missing")
			}
		}

		// Process the root node separately.
		if nodeChange.after.key == changes.rootChange.after.Value().key {
			// Write the root node to header.
			if changes.rootChange.after.HasValue() {
				rootNode := changes.rootChange.after.Value()
				rootNodeBytes := encodeDBNode_disk(&rootNode.dbNode)
				rootDiskAddr = diskAddress{
					totalDiskAddress.offset + int64(totalOffset) + int64(numWritten),
					int64(len(rootNodeBytes)),
				}
				totalOffset += len(rootNodeBytes)
				numWritten++
				totalBytes = append(totalBytes, rootNodeBytes...)
				if err != nil {
					return err
				}

				// Update cache: delete nodes with the same key value as the root.
				changes.rootChange.after.Value().dbNode.diskAddr = rootDiskAddr
				if changes.rootChange.after.HasValue() {
					compositeKey := fmt.Sprintf("%s:%d", changes.rootChange.after.Value().key.value, changes.rootChange.after.Value().key.length)
					r.cache.Set(compositeKey, changes.rootChange.after.Value().dbNode)
					if val, _ := r.deletedCache.Get(compositeKey); val != nil {
						r.deletedCache.Del(compositeKey)
					}
				}

				// Update total root bytes.
				r.rootNode = changes.rootChange.after.Value()
				rootDiskAddrBytes := rootDiskAddr.bytes()
				totalRootBytes = append(totalRootBytes, rootDiskAddrBytes[:]...)

				// Write the root key to disk.
				rootKey := rootNode.key
				rootKeyByteArray := encodeKey(rootKey)
				size, err := r.dm.file.WriteAt(rootKeyByteArray, int64(totalDiskAddress.size+totalDiskAddress.offset))
				if err != nil {
					return err
				}
				rootKeyDiskAddr := diskAddress{
					int64(totalDiskAddress.size + totalDiskAddress.offset),
					int64(size),
				}
				rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()
				totalRootBytes = append(totalRootBytes, rootKeyDiskAddrBytes[:]...)
				r.dm.file.WriteAt(totalRootBytes[:], 1)

				// Print the tree by updating the disk address.
				changes.rootChange.after.Value().dbNode.diskAddr = rootDiskAddr
			}
		} else {
			// Process non-root nodes.
			nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
			diskAddr := diskAddress{
				totalDiskAddress.offset + int64(totalOffset) + int64(numWritten),
				int64(len(nodeBytes)),
			}
			totalOffset += len(nodeBytes)
			totalBytes = append(totalBytes, nodeBytes...)
			numWritten++
			if err != nil {
				return err
			}

			nodeChange.after.dbNode.diskAddr = diskAddr
			if nodeChange.after.value.HasValue() {
				compositeKey := fmt.Sprintf("%s:%d", nodeChange.after.key.value, nodeChange.after.key.length)
				r.cache.Set(compositeKey, nodeChange.after.dbNode)
				if val, _ := r.deletedCache.Get(compositeKey); val != nil {
					r.deletedCache.Del(compositeKey)
				}
			}
			// If the node is a leaf, compress the key and store the disk address.
			if childrenNodes[k] == (diskAddress{}) {
				key := Key{length: k.length, value: k.value}
				childrenNodes[key] = diskAddr
			}
		}
	}
	// Create a span for writing the total bytes to disk.
	r.dm.setEOF(totalDiskAddress.offset + int64(totalOffset))
	// change the write functionality to write all the nodes in only when teh cache is full/would be full
	// _, err = r.dm.file.WriteAt(totalBytes, totalDiskAddress.offset)
	if err != nil {
		return err
	}

	// Clean up deleted nodes: add old nodes to the free list.
	for _, nodeChange := range changes.nodes {
		if nodeChange.before != nil && nodeChange.after == nil {
			if nodeChange.before.key != (Key{}) && nodeChange.before.dbNode.diskAddr != (diskAddress{}) {
				compositeKey := fmt.Sprintf("%s:%d", nodeChange.before.key.value, nodeChange.before.key.length)
				log.Print("added node to deleted cache : ", compositeKey)
				r.deletedCache.Set(compositeKey, nodeChange.before.dbNode, nodeChange.before.dbNode.diskAddr.size)
				if val, _ := r.cache.Get(compositeKey); val != nil {
					log.Print("deleted node : ", compositeKey)
					r.cache.Delete(compositeKey)
				}
			}
		}
	}
	log.Print("cache size: ", r.cache.Count())
	log.Print("deleted cache size: ", r.deletedCache.Len())
	// Sync the file to disk.
	return r.dm.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.dm.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	// Add a flag to check if the cache was found
	for _, node := range r.cache.GetAll() {
		log.Print("node in cache: ", node.(dbNode).value)
	}
	// log.Print("number of nodes in cache: ", r.cache.Count())
	val:= r.cache.GetValue(fmt.Sprintf("%s:%d", key.value, key.length));
	if val != nil {
		log.Print("found node: ", key.value, " in regular cache")
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
	

	if val, found := r.deletedCache.Get(fmt.Sprintf("%s:%d", key.value, key.length)); found {
		if val != nil {
			log.Print("found node: ", key.value, " in deleted cache")

			// If the value is found, process normally
			returnNode := &node{
				dbNode:      val.(dbNode),
				key:         key,
				valueDigest: val.(dbNode).value,
			}

			// Set the disk address from the cache entry
			returnNode.dbNode.value = maybe.Nothing[[]byte]()
			returnNode.dbNode.diskAddr = val.(dbNode).diskAddr

			// You can then return the node if you wish
			return returnNode, nil
		}
	}

	var (
		// all node paths start at the root
		currentDbNode = dbNode{}
		// tokenSize   = t.getTokenSize()
		tokenSize = BranchFactorToTokenSize[r.config.BranchFactor]
	)
	currKey := Key{}

	if r.rootNode != nil {
		currentDbNode = r.rootNode.dbNode
		currKey = r.rootNode.key
	}

	if !key.HasPrefix(currKey) {
		// log.Printf("key %v %v, currKey %v %v", key.length, []byte(key.value), currKey.length, []byte(currKey.value))
		return nil, database.ErrNotFound //errors.New("Key doesn't match rootKey")
	}

	keyLen := currKey.length // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far

	// tempDiskAddr := diskAddress{}
	// while the entire path hasn't been matched
	for keyLen < (key.length) {
		nextChildEntry, hasChild := currentDbNode.children[key.Token(keyLen, tokenSize)]

		keyLen += tokenSize
		if !hasChild {
			return nil, database.ErrNotFound
		}
		if !key.iteratedHasPrefix(nextChildEntry.compressedKey, keyLen, tokenSize) {
			// there was no child along the path or the child that was there doesn't match the remaining path
			return nil, database.ErrNotFound

		}

		// get the next key from the current child
		currKey := ToToken(key.Token(keyLen-tokenSize, tokenSize), tokenSize)
		currKey = currKey.Extend(nextChildEntry.compressedKey)
		keyLen += currKey.length - tokenSize

		// grab the next node along the path
		// Add caching check for the next node
		if val, found := r.cache.Get(fmt.Sprintf("%s:%d", currKey.value, currKey.length)); found {
			if val != nil {
				currentDbNode = val.(dbNode)
				currentDbNode.diskAddr = val.(dbNode).diskAddr
			}
		} else {
			nextBytes, err := r.dm.get(nextChildEntry.diskAddr)
			if err != nil {
				return nil, err
			}
			err = decodeDBNode_disk(nextBytes, &currentDbNode)
			currentDbNode.diskAddr = nextChildEntry.diskAddr
			if err != nil {
				return nil, err
			}
		}
	}
	// log.Print("found node at disk address ", tempDiskAddr)
	returnNode := &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
	}

	returnNode.dbNode.diskAddr = currentDbNode.diskAddr

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
