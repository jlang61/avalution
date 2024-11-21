// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

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
	// freelist is not initialized, need to initialize
	for _, nodeChange := range changes.nodes {
		if nodeChange.after == nil {
			continue
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
		r.dm.write(rootNodeBytes)
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
