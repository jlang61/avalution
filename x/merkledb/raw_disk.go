// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/ava-labs/avalanchego/utils/perms"
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
	file *os.File
	free *freeList
}

func newRawDisk(dir string, fileName string) (*rawDisk, error) {
	file, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return nil, err
	}
	return &rawDisk{file: file}, nil
}

func (r *rawDisk) endOfFile() (int64, error) {
	fileInfo, err := r.file.Stat()
	if err != nil {
		log.Fatalf("failed to get file info: %v", err)
	}
	return fileInfo.Size(), err
}

func (r *rawDisk) getShutdownType() ([]byte, error) {
	var shutdownType [1]byte
	_, err := r.file.ReadAt(shutdownType[:], 0)
	if err != nil {
		return nil, err
	}
	return shutdownType[:], nil
}

func (r *rawDisk) setShutdownType(shutdownType []byte) error {
	if len(shutdownType) != 1 {
		return fmt.Errorf("invalid shutdown type with length %d", len(shutdownType))
	}
	_, err := r.file.WriteAt(shutdownType, 0)
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

func (n *node) raw_disk_bytes() []byte {
	encodedBytes := encodeDBNode_disk(&n.dbNode)

	// 80 bytes 128  129

	// adding offset, size, capacity
	// capacity would be 128 -> reading it would read out only 80 bytes

	// 80 bytes 00000//next node
	// 128 bytes // 80 bytes next node -> node
	// 5 12345.12345678
	// 6 123456.2345678
	// 88 bytes // 8 bytes next node

	// writing raw disk
	// append
	// 80 bytes // next node
	// 88 bytes
	// 128 bytes, append 48 bytes of padding to the end of 80 bytes

	// node1 -> node2
	// [ 00 0 00 0 ]
	// [80] -> []
	// 88 -> 88 89 90 so

	// 0 children
	// 1 children 32 bytes, 31 bytes of compressed key

	// Calculate the next power of 2 size
	currentSize := len(encodedBytes)
	nextPowerOf2Size := nextPowerOf2(currentSize)

	// Add dummy bytes to reach the next power of 2 size
	paddingSize := nextPowerOf2Size - currentSize
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		encodedBytes = append(encodedBytes, padding...)
	}
	return encodedBytes
}

// Helper function to calculate the next power of 2 for a given size
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// type assertion to ensure that
// pointer to rawdisk implements disk interface
// var _ Disk = &rawDisk{}
// return new error for iterator

// BUG CHEANGE FREELIST TO ONLY
// add freelist to constructor (ensure that parameter types and names are the same)
// add freelist to field on rawdisk
// adding to freelist should be done AFTER iterations
// diskaddress on node works probably for the best
func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	// freelist is not initialized, need to initialize
	log.Println("FreeList: ", r.free)
	if r.free == nil {
		log.Printf("Free list not initialized, creating new free list with size 1024")
		// SIZE CAN BE CHANGED
		r.free = newFreeList(1024)
	}
	r.free.load()
	for _, nodeChange := range changes.nodes {
		if nodeChange.after == nil {
			continue
		}
		nodeBytes := nodeChange.after.bytes()
		// Get a diskAddress from the freelist to write the data
		freeSpace, ok := r.free.get(int64(len(nodeBytes)))
		if !ok {
			// If there is no free space, write at the end of the file
			endOffset, err := r.endOfFile()
			if err != nil {
				log.Fatalf("failed to get end of file: %v", err)
			}
			_, err = r.file.WriteAt(nodeBytes, endOffset)
			if err != nil {
				log.Fatalf("failed to write data: %v", err)
			}
			log.Println("Data written successfully at the end of the file.")
		} else {
			// If there is free space, write at the offset
			_, err := r.file.WriteAt(nodeBytes, freeSpace.offset)
			if err != nil {
				log.Fatalf("failed to write data: %v", err)
			}
			log.Println("Data written successfully at free space.")
		}
	}

	if changes.rootChange.after.HasValue() && r.file.Sync() == nil {
		rootNode := changes.rootChange.after.Value()
		rootNodeBytes := rootNode.bytes()
		// Get a diskAddress from the freelist to write the data
		freeSpace, ok := r.free.get(int64(len(rootNodeBytes)))
		if !ok {
			// If there is no free space, write at the end of the file
			endOffset, err := r.endOfFile()
			if err != nil {
				log.Fatalf("failed to get end of file: %v", err)
			}
			_, err = r.file.WriteAt(rootNodeBytes, endOffset)
			if err != nil {
				log.Fatalf("failed to write data: %v", err)
			}
			log.Println("Root node written successfully at the end of the file.")
		} else {
			// If there is free space, write at the offset
			_, err := r.file.WriteAt(rootNodeBytes, freeSpace.offset)
			if err != nil {
				log.Fatalf("failed to write data: %v", err)
			}
			log.Println("Root node written successfully at free space.")
		}
	}

	// ensuring that there are two trees, then add old one to freelist
	if r.file.Sync() == nil {
		for _, nodeChange := range changes.nodes {
			if nodeChange.after == nil {
				continue
			} else {
				if nodeChange.before != nil {
					r.free.put(nodeChange.before.diskAddr)
				}
			}
		}
	}
	return nil //r.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.file.Truncate(0)
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
