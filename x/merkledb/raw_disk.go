// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	// "log"

	// "log"
	"sort"

	// "github.com/hashicorp/golang-lru"


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
	dm           *diskMgr
	config       Config
	hasher       Hasher
	// creates a diffLayer with before/after of nodes
	diffLayer map[Key]*change[*node]
	rootNode  *node
}

func newRawDisk(dir string, fileName string, hasher Hasher, config Config) (*rawDisk, error) {
	dm, err := newDiskManager(nil, dir, fileName)
	if err != nil {
		return nil, err
	}

	diffLayer := make(map[Key]*change[*node])
	// Check if the file is empty
	fileInfo, err := dm.file.Stat()
	if err != nil {
		return nil, err
	}
	if fileInfo.Size() > 33 {
		// If file is not empty, read the root node from the file
		metadata, err := dm.getHeader()
		if err != nil {
			return nil, err
		}

		rootAddress := diskAddress{
			offset: int64(binary.BigEndian.Uint64(metadata[0:8])),
			size:   int64(binary.BigEndian.Uint64(metadata[8:16])),
		}

		rootNodeBytes, err := dm.get(rootAddress)
		if err != nil {
			return nil, err
		}

		rootNode := &node{}
		decodeDBNode_disk(rootNodeBytes, &rootNode.dbNode)

		if err != nil {
			return nil, database.ErrNotFound
		}

		rootKeyAddr := diskAddress{
			offset: int64(binary.BigEndian.Uint64(metadata[16:24])),
			size:   int64(binary.BigEndian.Uint64(metadata[24:32])),
		}

		rootKeyBytes, err := dm.get(rootKeyAddr)
		if err != nil {
			return nil, err
		}
		rootKey, err := decodeKey(rootKeyBytes)
		if err != nil {
			return nil, err
		}

		rootNode.key = rootKey

		return &rawDisk{
			dm:           dm,
			hasher:       hasher,
			config:       config,
			diffLayer:    diffLayer,
			rootNode:     rootNode,
		}, nil
	}
	// If file is empty, create a new root node
	return &rawDisk{
		dm:           dm,
		hasher:       hasher,
		config:       config,
		diffLayer:    diffLayer,
		rootNode:     nil,
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
	if err != nil {
		return err
	// Completely write all of the data in difflayer to the file
	// and clear the diffLayer
	} else {
		// Similar logic to writeChanges -> write children first, then write parent

		var keys []Key
		for k := range r.diffLayer {
			keys = append(keys, k)
		}
		// sort the keys by length, then start at the longest keys (leaf nodes)
		// sorting longest to shortest
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].length > keys[j].length
		})

		totalLenBytes := 0
		for _, nodes := range r.diffLayer {
			if nodes.after != nil {
				// Increase length by one for the padding byte per node
				totalLenBytes += len(encodeDBNode_disk(&nodes.after.dbNode))
				totalLenBytes += 16 * len(nodes.after.children)
			}
		}

		// fetch the available disk address for totallenbytes
		totalDiskAddress, err := r.dm.fetch(int64(totalLenBytes))

		totalOffset := 0
		childrenNodes := make(map[Key]diskAddress)
		totalBytes := make([]byte, 0)
		rootDiskAddr := diskAddress{}
		totalRootBytes := make([]byte, 0)
		for _, k := range keys {
			// find the nodechange associated with the key
			nodeChange := r.diffLayer[k]
			// filter through nodes that arent changed
			if nodeChange.after == nil {
				continue
			}
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
				// IF THE CHILDREN ARE ALREADY WRITTEN TO DISK, THEY SHOULD
				// HAVE A DISKADDRESS ASSOCIATED WITH THEM ALREADY
				// THEREFORE WE CAN SKIP THIS STEP
			}
			// check to ensure that all of its children have disk addresses
			for _, child := range nodeChange.after.children {
				// Check remainingNodes actually have disk addresses
				if child.diskAddr == (diskAddress{}) {
					return errors.New("regular node child disk address missing")
				}
			}
			if nodeChange.after.key == r.rootNode.key {
				// writing rootNode to header
				if r.rootNode.hasValue() {
					rootNode := r.rootNode
					rootNodeBytes := encodeDBNode_disk(&rootNode.dbNode)
					rootDiskAddr = diskAddress{totalDiskAddress.offset + int64(totalOffset), int64(len(rootNodeBytes))}
					totalOffset += len(rootNodeBytes)
					totalBytes = append(totalBytes, rootNodeBytes...)
					if err != nil {
						return err
					}
					// Convert diskaddr of rootnode and rootkey to bytes
					// Then write the key to the file and the root information to header

					rootDiskAddrBytes := rootDiskAddr.bytes()
					totalRootBytes = append(totalRootBytes, rootDiskAddrBytes[:]...)
					rootKey := r.rootNode.key
					rootKeyByteArray := encodeKey(rootKey)
					size, _ := r.dm.file.WriteAt(rootKeyByteArray, int64(totalDiskAddress.size+totalDiskAddress.offset))
					rootKeyDiskAddr := diskAddress{int64(totalDiskAddress.size + totalDiskAddress.offset), int64(size)}
					rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()
					totalRootBytes = append(totalRootBytes, rootKeyDiskAddrBytes[:]...)
					r.dm.file.WriteAt(totalRootBytes[:], 1)
					r.rootNode.dbNode.diskAddr = rootDiskAddr
				}
			} else {
				nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
				diskAddr := diskAddress{totalDiskAddress.offset + int64(totalOffset), int64(len(nodeBytes))}
				totalOffset += len(nodeBytes)
				totalBytes = append(totalBytes, nodeBytes...)
				nodeChange.after.dbNode.diskAddr = diskAddr
				// If there is not a node with the key in the map, create a new map with the key being the ch
				if childrenNodes[k] == (diskAddress{}) {
					// If the node is a leaf node, compress the key and store the disk address
					key := Key{length: k.length, value: k.value}
					childrenNodes[key] = diskAddr
				}
			}
		}
		// write the total bytes to the disk
		_, err = r.dm.file.WriteAt(totalBytes, totalDiskAddress.offset)
		if err != nil {
			return err
		}

	}
	return r.dm.file.Sync()

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
	if r.rootNode == nil {
		return nil, database.ErrNotFound
	}
	return encodeKey(r.rootNode.key), nil

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
	// Check if there is a merkle tree on the disk already
	// if not, create a new one
	if r.rootNode != nil {
		for key, newNode := range changes.nodes {
			// Check if the node.before is found in the diff layer
			if newNode.before != nil {
				if diffLayerNode, ok := r.diffLayer[key]; ok {
					// check if the node is in the diff layer
					if diffLayerNode.before != nil {
						// if the difflayer node is found and has a before value
						// this node is adding a change on top of a change
						r.diffLayer[key] = &change[*node]{diffLayerNode.before, newNode.after}
					} else {
						// if the node is not found in the diff layer, add it to the diff layer
						r.diffLayer[key] = &change[*node]{newNode.before, newNode.after}
					}
					// Addiitonal step so that any children values with disk addresses are added to the diff laye	

				} else {
					// means that the node.before is on the raw disk
					// and the node.after should be put in the diff layer
					r.diffLayer[key] = &change[*node]{newNode.before, newNode.after}
				}
			} else {
				// if the node does not have a before, it means that it is a new node
				// and should be added to the diff layer
				r.diffLayer[key] = &change[*node]{nil, newNode.after}
			}

			// Special case for rootnode
			if newNode.after != nil {
				if newNode.after.key == changes.rootChange.after.Value().key {
					r.rootNode = newNode.after
				}
			}
		}
	} else { // sort the keys by length, then start at the longest keys (leaf nodes)
		// sorting longest to shortest
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].length > keys[j].length
		})

		totalLenBytes := 0
		for _, nodes := range changes.nodes {
			if nodes.after != nil {
				// Increase length by one for the padding byte per node
				totalLenBytes += len(encodeDBNode_disk(&nodes.after.dbNode))
				totalLenBytes += 16 * len(nodes.after.children)
			}
		}
		// every node in the tree has a diskaddress of its children except leaf nodes
		// find how many leaf nodes there are

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
		totalRootBytes := make([]byte, 0)
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
			if nodeChange.after.key == changes.rootChange.after.Value().key {
				// writing rootNode to header
				if changes.rootChange.after.HasValue() {
					rootNode := changes.rootChange.after.Value()
					// assign children disk addresses to root node
					for token, child := range rootNode.children {
						completeKey := rootNode.key.Extend(ToToken(token, BranchFactorToTokenSize[r.config.BranchFactor]))
						if child.compressedKey.length != 0 {
							completeKey = completeKey.Extend(child.compressedKey)
						}
						if childrenNodes[completeKey] != (diskAddress{}) {
							child.diskAddr = childrenNodes[completeKey]
						}
					}

					r.rootNode = rootNode
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

					// add function that would write the root node to the disk while also updating the disk address
					if err != nil {
						return err
					}
					rootDiskAddrBytes := rootDiskAddr.bytes()
					totalRootBytes = append(totalRootBytes, rootDiskAddrBytes[:]...)
					// r.dm.file.WriteAt(rootDiskAddrBytes[:], 1)

					rootKey := rootNode.key
					rootKeyByteArray := encodeKey(rootKey)

					// need to set tthe endof file to something different - current issue is that its ovelapping with end of file

					size, err := r.dm.file.WriteAt(rootKeyByteArray, int64(totalDiskAddress.size+totalDiskAddress.offset))
					if err != nil {
						return err
					}
					rootKeyDiskAddr := diskAddress{int64(totalDiskAddress.size + totalDiskAddress.offset), int64(size)}
					rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()

					totalRootBytes = append(totalRootBytes, rootKeyDiskAddrBytes[:]...)
					r.dm.file.WriteAt(totalRootBytes[:], 1)

					// print the tree
					changes.rootChange.after.Value().dbNode.diskAddr = rootDiskAddr
				}
			} else {
				nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
				diskAddr := diskAddress{totalDiskAddress.offset + int64(totalOffset), int64(len(nodeBytes))}
				totalOffset += len(nodeBytes)
				totalBytes = append(totalBytes, nodeBytes...)
				if err != nil {
					return err
				}

				nodeChange.after.dbNode.diskAddr = diskAddr
				// If there is not a node with the key in the map, create a new map with the key being the ch
				if childrenNodes[k] == (diskAddress{}) {
					// If the node is a leaf node, compress the key and store the disk address
					key := Key{length: k.length, value: k.value}
					childrenNodes[key] = diskAddr
				}

			}
		}

		// write the total bytes to the disk
		_, err = r.dm.file.WriteAt(totalBytes, totalDiskAddress.offset)
		if err != nil {
			return err
		}

		// }
		return r.dm.file.Sync()
	}
	return nil
}

func (r *rawDisk) Clear() error {
	return r.dm.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {

	// Check through the diff layer to see if the node
	// is in the diff layer
	if diffLayerNode, ok := r.diffLayer[key]; ok {
		// Check if the node has a before or after
		if diffLayerNode.after != nil {
			// If the node has an after, return the node
			return diffLayerNode.after, nil
		} else if diffLayerNode.before != nil {
			// there is no before only an after
			return nil, database.ErrNotFound
		} else {
			// there is no before or after of a node
			return nil, errors.New("Node not found in diff layer")
		}
	}
	var (
		// all node paths start at the root
		currentDbNode = dbNode{}
		tokenSize     = BranchFactorToTokenSize[r.config.BranchFactor]
	)


	if r.rootNode == nil {
		return nil, database.ErrNotFound
	}
	currentDbNode = r.rootNode.dbNode

	currKey := Key{}
	diffLayerKey := Key{}
	if r.rootNode != nil {
		currKey = r.rootNode.key
		diffLayerKey = r.rootNode.key
	}

	if !key.HasPrefix(currKey) {
		return nil, database.ErrNotFound //errors.New("Key doesn't match rootKey")
	}

	keyLen := currKey.length // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far

	// tempDiskAddr := diskAddress{}
	// while the entire path hasn't been matched
	for keyLen < (key.length) {
		// confirm that a child exists and grab its address before attempting to load it

		nextChildEntry, hasChild := currentDbNode.children[key.Token(keyLen, tokenSize)]
		token := key.Token(keyLen, tokenSize)
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

		// create the entire key by extending the current key with the child's compressed key
		diffLayerKey = diffLayerKey.Extend(ToToken(token, tokenSize))
		diffLayerKey = diffLayerKey.Extend(nextChildEntry.compressedKey)
		// currKey = currKey.Extend(ToToken(token, tokenSize))
		currKey = currKey.Extend(nextChildEntry.compressedKey)
		keyLen += currKey.length - tokenSize

		// Search first through the difflayer
		if diffLayerNode, ok := r.diffLayer[diffLayerKey]; ok {
			// Check if the node has a before or after
			if diffLayerNode.after != nil {
				// If the node has an after, return the node
				currentDbNode = diffLayerNode.after.dbNode
			} else if diffLayerNode.before != nil {
				// there is no before only an after
				return nil, database.ErrNotFound
			}
		} else {
			// grab the next node along the path
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
	returnNode := &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
	}


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
	r.setShutdownType([]byte{1})
	// _, err := r.dm.file.WriteAt([]byte{1}, 0)
	// if err != nil {
	// 	return err
	// }
	if err := r.dm.file.Close(); err != nil {
		return err
	}

	return nil
}
