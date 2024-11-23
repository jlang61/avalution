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

func (r diskAddress) end() int64 {
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

	//0x1
	//0x12
	// start longest key
	// every one after that, committed this child -here is disk address

	// Create a temporary map of children to store the disk address and compressed key of the children
	tempChildren := make(map[Key]map[Key]diskAddress)

	for _, k := range keys {
		nodeChange := changes.nodes[k]
		if nodeChange.after == nil {
			continue
		}

		// DELETE: test the key compressed key
		// log.Printf("Key is %v", nodeChange.after.key)
		// if len(nodeChange.after.children) > 0 {
		// 	log.Printf("Enter children")
		// 	for _, child := range nodeChange.after.children {
		// 		log.Printf("Compressed key is %v", child.compressedKey)
		// 	}

		// }

		// Ensure root is not being written twice
		if changes.rootChange.after.HasValue() {
			if nodeChange.after.key == changes.rootChange.after.Value().key {
				continue
			}
		}

		// Check with existing map of children to see if this node is a parent
		if tempChildren[k] != nil {
			// Iterate through the children in the node
			for _, child := range nodeChange.after.children {
				// Ensure that there is a diskaddress for the child
				if tempChildren[k][child.compressedKey] != (diskAddress{}) {
					child.diskAddr = tempChildren[k][child.compressedKey]
				} else {
					log.Fatalf("Child disk address not found")
				}
			}

		}
		nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
		diskAddr, err := r.dm.write(nodeBytes)
		if err != nil {
			return err
		}

		if tempChildren[k] == nil {
			// If the node is a leaf node, compress the key and store the disk address
			// log.Printf("Key is %v", k)
			compressedKey := Key{length: k.length, value: k.value[len(k.value)-1:]}
			// log.Printf("Compressed key is %v", compressedKey)
			// log.Printf("Disk address is %v", diskAddr)
			tempChildren[k] = make(map[Key]diskAddress)
			tempChildren[k][compressedKey] = diskAddr
		}

	}
	if err := r.dm.file.Sync(); err != nil {
		return err
	}
	if changes.rootChange.after.HasValue() {
		rootNode := changes.rootChange.after.Value()
		rootNodeBytes := encodeDBNode_disk(&rootNode.dbNode)
		rootDiskAddr, err := r.dm.write(rootNodeBytes)
		if err != nil {
			return err
		}
		// writing root to header
		rootDiskAddrBytes := rootDiskAddr.bytes()
		r.dm.file.WriteAt(rootDiskAddrBytes[:], 1)
		// writing root key to header

		rootKey := rootNode.key.Bytes()
		rootKeyDiskAddr, err := r.dm.write(rootKey[:])
		if err != nil {
			return err
		}
		// writing root key to hea
		rootKeyDiskAddrBytes := rootKeyDiskAddr.bytes()
		r.dm.file.WriteAt(rootKeyDiskAddrBytes[:], 17)

	}
	if err := r.dm.file.Sync(); err != nil {
		return err
	}
	// ensuring that there are two trees, then add old one to freelist
	for _, nodeChange := range changes.nodes {
		if nodeChange.after == nil {
			continue
		} else {
			if nodeChange.before != nil {
				r.dm.free.put(nodeChange.before.diskAddr)
			}
		}
	}

	if err := r.dm.file.Sync(); err != nil {
		log.Fatalf("failed to sync data at the end: %v", err)
	}

	// Iterate through tempChildren and print the compressed key and disk address
	// for k, v := range tempChildren {
	// 	log.Printf("Key is %v", k)
	// 	for key, diskAddr := range v {
	// 		log.Printf("Compressed key is %v", key)
	// 		log.Printf("Disk address is %v", diskAddr)
	// 	}
	// }

	return r.dm.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.dm.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	// 	rootKey, err := r.getRootKey()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	// check if its what you're looking for
	// 	// if not, either check children or return error

	// 	// compare key and if prefix matches key of

	// 	// truncate key, if we have a match, check children of current node

	// 	// check all children of current node, if we have a match, check children of current node
	return nil, errors.New("not implemented")
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}
