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

const fileName = "merkle.db"

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
}

func newRawDisk(dir string) (*rawDisk, error) {
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
	return encodedBytes
}

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	for _, nodeChange := range changes.nodes {
		if nodeChange.after == nil {
			continue
		}
		nodeBytes := nodeChange.after.bytes()
		endOffset, err := r.endOfFile()
		_, err = r.file.WriteAt(nodeBytes, endOffset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
		}
		log.Println("Data written successfully at the end of the file BIG DUB.")
	}
	if changes.rootChange.after.HasValue() {
		rootNode := changes.rootChange.after.Value()
		rootNodeBytes := rootNode.bytes()
		endOffset, err := r.endOfFile()
		if err != nil {
			log.Fatalf("failed to get end of file: %v", err)
		}
		_, err = r.file.WriteAt(rootNodeBytes, endOffset)
		if err != nil {
			log.Fatalf("failed to write rootChange data: %v", err)
		}
		log.Println("Root change written successfully at the end of the file.")
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
