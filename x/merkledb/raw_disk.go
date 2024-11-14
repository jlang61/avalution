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
	"slices"

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

// serializes diskAddr, first 8 bytes = size, next 8 is offset
func (r diskAddress) bytes() [16]byte {
	var bytes [16]byte
	binary.BigEndian.PutUint64(bytes[:8], uint64(r.offset))
	binary.BigEndian.PutUint64(bytes[8:], uint64(r.size))
	return bytes
}

func (r *diskAddress) decode(diskAddressBytes []byte) {
	r.offset = int64(binary.BigEndian.Uint64(diskAddressBytes))
	r.size = int64(binary.BigEndian.Uint64(diskAddressBytes[8:]))
	//return offset, size
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

func encodeRawDiskNode(n *dbNode) []byte {
	length := encodedDBNodeSize(n)
	w := codecWriter{
		b: make([]byte, 0, length), //byte slice with size:0, capacity:length
	}

	w.MaybeBytes(n.value) // ***w=1(if exist) + varint(valuelen) + value***

	numChildren := len(n.children) //count size of hashmap
	w.Uvarint(uint64(numChildren)) //***w+=varint(number of children)***

	// Avoid allocating keys entirely if the node doesn't have any children.
	if numChildren == 0 {
		return w.b //done if no children
	}

	// By allocating BranchFactorLargest rather than [numChildren], this slice
	// is allocated on the stack rather than the heap. BranchFactorLargest is
	// at least [numChildren] which avoids memory allocations.
	keys := make([]byte, numChildren, BranchFactorLargest) //keys=hashmap of size:numchildren, capacity:branchfactorlargest
	i := 0
	//copies n's children into keys
	for k := range n.children {
		keys[i] = k
		i++
	}

	// Ensure that the order of entries is correct., sort children increasing
	slices.Sort(keys)
	for _, index := range keys {
		entry := n.children[index]
		w.Uvarint(uint64(index))   //***w+=varint(child index)***
		w.Key(entry.compressedKey) //***w+=varint(len compressed key) + child compressed key***
		w.ID(entry.id)             //***w+=child id(32 byte hash)***
		serializedAddr := entry.diskAddr.bytes()
		w.b = append(w.b, serializedAddr[:]...) //***w+=diskAddress(size+offset)
		w.Bool(entry.hasValue)                  //***w+=1 if exist(yes)***
	}
	return w.b //finished serialization(byte slice) of node n
}

func (n *node) raw_disk_bytes() []byte {
	return encodeRawDiskNode(&n.dbNode)
}

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	for _, nodeChange := range changes.nodes {
		if nodeChange.after == nil {
			continue
		}
		nodeBytes := nodeChange.after.raw_disk_bytes()
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
	return r.file.Sync()
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
