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
	// freelist is not initialized, need to initialize
	var keys []Key

	for k := range changes.nodes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].value < keys[j].value
	})
	for _, k := range keys {
		nodeChange := changes.nodes[k]
		if nodeChange.after == nil {
			continue
		}
		if changes.rootChange.after.HasValue() {
			if nodeChange.after.key == changes.rootChange.after.Value().key {
				continue
			}
		}
		nodeBytes := encodeDBNode_disk(&nodeChange.after.dbNode)
		r.dm.write(nodeBytes)
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

	// if err := r.file.Sync(); err != nil {
	// 	log.Fatalf("failed to sync data at the end: %v", err)
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

	currKeyString := string(rootKeyBytes[:])
	currKey := Key{length: len(currKeyString), value: currKeyString}
	if err != nil {
		return nil, err
	}

	if !key.HasPrefix(currKey) {
		return nil, errors.New("Key doesn't match rootkey")
	}

	keylen := currKey.length + tokenSize // keeps track of where to start comparing prefixes in the key i.e. the length of key iterated so far

	// while the entire path hasn't been matched
	for keylen < (key.length) {
		// confirm that a child exists and grab its address before attempting to load it
		nextChildEntry, hasChild := currentDbNode.children[key.Token(keylen, tokenSize)]
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
	return &node{
		dbNode:      currentDbNode,
		key:         key,
		valueDigest: currentDbNode.value,
	}, nil
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}
