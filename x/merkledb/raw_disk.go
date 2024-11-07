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

	"github.com/ava-labs/avalanchego/ids"
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

func (r *diskAddress) decode(diskAddressBytes []byte) {
	r.offset = int64(binary.BigEndian.Uint64(diskAddressBytes))
	r.size = int64(binary.BigEndian.Uint64(diskAddressBytes[8:]))
}

type rawDisk struct {
	// [0] = shutdownType
	// [1,17] = rootKey raw file offset
	// [18,] = node store
	file *os.File
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

// simple test function to write to end of disk
func (r *rawDisk) appendBytes(data []byte) error {
	endOffset, err := r.endOfFile()

	// Write the data at the end of the file using WriteAt.
	_, err = r.file.WriteAt(data, endOffset)
	if err != nil {
		// return err
		log.Fatalf("failed to write data: %v", err)
	}

	log.Println("Data written successfully at the end of the file.")
	return nil
}

// simple test function to write to end of disk
func (r *rawDisk) writeBytes(data []byte, offset int64) error {
	// Write the data at the offset in the file using WriteAt.
	_, err := r.file.WriteAt(data, offset)
	if err != nil {
		log.Fatalf("failed to write data: %v", err)
		return err
	}

	log.Println("Data written successfully at the end of the file.")
	return nil
}

func (r *rawDisk) writeNode(n *node, offset int64) error {
	// Write the data at the offset in the file using WriteAt.
	data := n.bytes()
	_, err := r.file.WriteAt(data, offset)
	if err != nil {
		log.Fatalf("failed to write data node: %v", err)
		return err
	}

	log.Println("Data node written successfully at the end of the file.")
	return nil
}

type diskNode struct {
	node
	diskAddr diskAddress
}

type diskChangeSummary struct {
	// The ID of the trie after these changes.
	rootID ids.ID
	// The root before/after this change.
	// Set in [applyValueChanges].
	rootChange change[maybe.Maybe[*diskNode]]
	nodes      map[Key]*change[*diskNode]
	values     map[Key]*change[maybe.Maybe[[]byte]]
}

//
func (n *diskNode) bytes() []byte {
	encodedBytes := encodeDBNode(&n.dbNode)
	diskAddrBytes := n.diskAddr.bytes()
	return append(encodedBytes, diskAddrBytes[:]...)
}

func (r *rawDisk) writeChanges(ctx context.Context, changes *diskChangeSummary, freelist *freeList) error {
	for _, nodeChange := range changes.nodes {
			// If nodes aren't changed, continue; otherwise, put the before into the freelist
			if nodeChange.after == nil {
				continue
			} else {
				if nodeChange.before != nil {
					freelist.put(nodeChange.before.diskAddr)
				}
			}

			nodeBytes := nodeChange.after.bytes()
			// Get a diskAddress from the freelist to write the data
			freeSpace, ok := freelist.get(int64(len(nodeBytes)))
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

		if changes.rootChange.after.HasValue() {
			rootNode := changes.rootChange.after.Value()
			rootNodeBytes := rootNode.bytes()
			// Get a diskAddress from the freelist to write the data
			endOffset, err := r.endOfFile()
			if err != nil {
				log.Fatalf("failed to get end of file: %v", err)
			}
			_, err = r.file.WriteAt(rootNodeBytes, endOffset)
			if err != nil {
				log.Fatalf("failed to write rootChange data: %v", err)
			}
			log.Println("Root change written successfully.")
		}
	return nil
}

func (r *rawDisk) Clear() error {
	return r.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	return nil, errors.New("not implemented")
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}