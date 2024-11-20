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

const fileName = "merkle.db"

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

func (r *diskAddress) decode(diskAddressBytes []byte) {
	r.offset = int64(binary.BigEndian.Uint64(diskAddressBytes))
	r.size = int64(binary.BigEndian.Uint64(diskAddressBytes[8:]))
}

type rawDisk struct {
	// [0] = shutdownType
	// [1,17] = root Db Node raw file offset
  // [18, 34] = rootKey
	// [34,] = node store
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

/*
	func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
		for _, nodeChange := range changes.nodes {
			if nodeChange.after == nil { //nodeChange.after
				continue
			}
			nodeBytes := nodeChange.after.bytes()
			//-------------------(beg)appendBytes-----------------------
			endOffset, err := r.endOfFile()
			// Write the data at the end of the file using WriteAt.
			_, err = r.file.WriteAt(nodeBytes, endOffset)
			if err != nil {
				// return err
				log.Fatalf("failed to write data: %v", err)
			}

			log.Println("Data written successfully at the end of the file BIG DUB.")
		}
		//-------------------(end)appendBytes-----------------------
		/*
				offset, err := r.file.Seek(0, os.SEEK_END)
				if err != nil {
					return err
				}

				_, err = r.file.Write(nodeBytes)
				if err != nil {
					return err
				}

				// Store disk address of the written node.
				childDiskAddr := diskAddress{
					offset: offset,
					size:   int64(len(nodeBytes)),
				}

				// Add the disk address bytes to the node's serialized form.
				diskAddrBytes := childDiskAddr.bytes()
				_, err = r.file.Write(diskAddrBytes[:])
				if err != nil {
					return err
				}
			}

			// Record the changes in the history to enable tracking of the state over time.
			trieHistory := newTrieHistory(r.cacheSize())
			trieHistory.record(changes)

		return nil
		//return errors.New("not implemented")
	}
*/
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

func (n *diskNode) bytes() []byte {
	encodedBytes := encodeDBNode(&n.dbNode)
	diskAddrBytes := n.diskAddr.bytes()
	return append(encodedBytes, diskAddrBytes[:]...)
}

func (r *rawDisk) writeChanges(ctx context.Context, changes *diskChangeSummary) error {
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
		// grab the next node along the path
		// currentNode, err = t.getNode(key.Take(currentNode.key.length+tokenSize+nextChildEntry.compressedKey.length), nextChildEntry.hasValue)
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
