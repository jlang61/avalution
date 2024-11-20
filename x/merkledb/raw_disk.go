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
	"sort"

	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/ava-labs/avalanchego/utils/perms"
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
	// [1,17] = root Db Node raw file offset
  // [18, 34] = rootKey
	// [34,] = node store
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

// simple test function to write to end of disk
func (r *rawDisk) appendBytes(data []byte) error {
	endOffset, err := r.endOfFile()
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
	log.Printf("Current size: %v\n", currentSize)
	nextPowerOf2Size := nextPowerOf2(currentSize)

	// Add dummy bytes to reach the next power of 2 size
	paddingSize := nextPowerOf2Size - currentSize
	log.Printf("Padding size: %v\n", paddingSize)
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
	if r.free == nil {
		log.Printf("Free list not initialized, creating new free list with size 1024")
		r.free = newFreeList(1024) // SIZE CAN BE CHANGED
	}
	r.free.load()
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
		nodeBytes := nodeChange.after.raw_disk_bytes()
		log.Printf("Length of Node bytes: %v\n", len(nodeBytes))
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
		if err := r.file.Sync(); err != nil {
			log.Fatalf("failed to sync data: %v", err)
		}
	}

	if changes.rootChange.after.HasValue() && r.file.Sync() == nil {
		rootNode := changes.rootChange.after.Value()
		rootNodeBytes := rootNode.raw_disk_bytes()
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
		if err := r.file.Sync(); err != nil {
			log.Fatalf("failed to sync data: %v", err)
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
	if err := r.file.Sync(); err != nil {
		log.Fatalf("failed to sync data at the end: %v", err)
	}
	return nil //r.file.Sync()
}

func (r *rawDisk) Clear() error {
	return r.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*dbNode, error) {

  var rootAddress [16]byte
  _, err := r.file.ReadAt(rootAddress[:], 1)
  if err != nil {
    return nil, err
  }

  rootBytes := make([]byte, binary.BigEndian.Uint64(rootAddress[:8]))
  offset := binary.BigEndian.Uint64(rootAddress[8:])
  _ , err = r.file.ReadAt(rootBytes[:], int64(offset))
  if err != nil {
    return nil, err
  }

  var (
    // all node paths start at the root
    currentDbNode = dbNode{}
    // tokenSize   = t.getTokenSize()
    tokenSize   = 8
  )

  err = decodeDBNode(rootBytes, &currentDbNode)
  if err != nil {
    return nil, err
  }

  // pseudo
  var rootKeyBytes [16]byte // 16 bytes is assumption of root key size
  _, err = r.file.ReadAt(rootBytes[:], 18)
  currKey, err := decodeKey(rootKeyBytes[:])
  if err != nil {
    return nil, err
  }

  if !key.HasPrefix(currKey) {
    return nil, errors.New("Key doesn't match rootkey")
  }

  // while the entire path hasn't been matched
  for currKey.length < key.length {
    // confirm that a child exists and grab its address before attempting to load it
    nextChildEntry, hasChild := currentDbNode.children[key.Token(currKey.length, tokenSize)]

    if !hasChild || !key.iteratedHasPrefix(nextChildEntry.compressedKey, currKey.length+tokenSize, tokenSize) {
      // there was no child along the path or the child that was there doesn't match the remaining path
      return nil, errors.New("Key not found in node's children")
    }

    // get the next key from the current child
    currKey = nextChildEntry.compressedKey
    // grab the next node along the path
    nextBytes := make([]byte, nextChildEntry.diskAddr.size)
    _ , err = r.file.ReadAt(nextBytes[:], nextChildEntry.diskAddr.offset)
    if err != nil {
      return nil, err
    }
    err = decodeDBNode(nextBytes, &currentDbNode)
    if err != nil {
      return nil, err
    }
  }
  return &currentDbNode, nil
}

func (r *rawDisk) cacheSize() int {
  return 0 // TODO add caching layer
}
