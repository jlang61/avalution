// translation of nodestore.rs to golang
// https://github.com/ava-labs/firewood/blob/5e9db421cd195740d2269aea49548e97c7104f3b/storage/src/nodestore.rs


nodestore.rs translated to go:
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
)

// defines structure for metadata in the node store 
type NodeStoreHeader struct {
	version     uint32
	endianTest  uint32
	rootAddress *uint64
	size        uint64
	freeLists   []*uint64 // array of free list pointers, one for each area size
}

//  initializes nodestoreheader with default values
func NewNodeStoreHeader() NodeStoreHeader {
	return NodeStoreHeader{
		version:     1,
		endianTest:  1,
		rootAddress: nil,
		size:        0,
		freeLists:   make([]*uint64, 23),
	}
}

// represents main structure for storing nodes with metadata and storage handling
type NodeStore struct {
	header NodeStoreHeader
	kind   NodeStoreKind
	storage Storage
}

//  represents the type of node store, committed or proposal
type NodeStoreKind interface{}

// committed holds data for the committed nodes, includes deleted nodes addresses and root hash
type Committed struct {
	deleted  []uint64
	rootHash *uint64
}

// mutable proposal holds data for proposed node set
type MutableProposal struct {
	root    *Node
	deleted []uint64
	parent  NodeStoreParent
}

// holds hash for immutable proposal
type ImmutableProposal struct {
	rootHash *uint64
}

type NodeStoreParent interface{}

type NodeStoreParentCommitted struct {
	rootHash *uint64
}

type NodeStoreParentProposed struct {
	parent *ImmutableProposal
}

type Storage interface {
	StreamFrom(addr uint64) (io.Reader, error)
	Write(addr uint64, data []byte) error
	ReadCachedNode(addr uint64) (*Node, error)
	AddToFreeListCache(addr uint64, nextFreeBlock *uint64)
	FreeListCache(addr uint64) *uint64
}

type Node struct {
	// Node fields
}

type FreeArea struct {
	nextFreeBlock *uint64
}

type AreaIndex uint8

const AREA_SIZES = [23]uint64{
	16, 32, 64, 96, 128, 256, 512, 768, 1024, 1024 << 1, 1024 << 2, 1024 << 3, 1024 << 4, 1024 << 5, 1024 << 6, 1024 << 7, 1024 << 8, 1024 << 9, 1024 << 10, 1024 << 11, 1024 << 12, 1024 << 13, 1024 << 14,
}

const NUM_AREA_SIZES = len(AREA_SIZES)
const MIN_AREA_SIZE = AREA_SIZES[0]
const MAX_AREA_SIZE = AREA_SIZES[NUM_AREA_SIZES-1]

// returns the AreaIndex for given size, validates the size
func areaSizeToIndex(n uint64) (AreaIndex, error) {
	if n > MAX_AREA_SIZE {
		return 0, errors.New(fmt.Sprintf("Node size %d is too large", n))
	}

	if n <= MIN_AREA_SIZE {
		return 0, nil
	}

	for i, size := range AREA_SIZES {
		if size >= n {
			return AreaIndex(i), nil
		}
	}

	return 0, errors.New(fmt.Sprintf("Node size %d is too large", n))
}


type LinearAddress uint64
//  represents a structure with a node and freeArea that is associated
type Area[T, U any] struct {
	Node T
	Free U
}

type StoredArea[T any] struct {
	areaSizeIndex AreaIndex
	area          T
}

func (ns *NodeStore) areaIndexAndSize(addr LinearAddress) (AreaIndex, uint64, error) {
	areaStream, err := ns.storage.StreamFrom(uint64(addr))
	if err != nil {
		return 0, 0, err
	}

	var index AreaIndex
	if err := binary.Read(areaStream, binary.LittleEndian, &index); err != nil {
		return 0, 0, err
	}

	size := AREA_SIZES[index]
	return index, size, nil
}

func (ns *NodeStore) readNodeFromDisk(addr LinearAddress) (*Node, error) {
	if node, err := ns.storage.ReadCachedNode(uint64(addr)); err == nil {
		return node, nil
	}

	if addr%8 != 0 {
		return nil, errors.New("invalid address alignment")
	}

	addr += 1 // Skip the index byte

	areaStream, err := ns.storage.StreamFrom(uint64(addr))
	if err != nil {
		return nil, err
	}

	var area Area[Node, FreeArea]
	if err := gob.NewDecoder(areaStream).Decode(&area); err != nil {
		return nil, err
	}

	if area.Node != nil {
		return area.Node, nil
	}

	return nil, errors.New("attempted to read a freed area")
}

// initializes nodestore from storage by reading header and setting up root if it exists
func (ns *NodeStore) open(storage Storage) (*NodeStore, error) {
	stream, err := storage.StreamFrom(0)
	if err != nil {
		return nil, err
	}

	var header NodeStoreHeader
	if err := binary.Read(stream, binary.LittleEndian, &header); err != nil {
		return nil, err
	}

	if header.version != 1 {
		return nil, errors.New("incompatible firewood version")
	}
	if header.endianTest != 1 {
		return nil, errors.New("database cannot be opened due to difference in endianness")
	}

	nodestore := &NodeStore{
		header:  header,
		kind:    Committed{deleted: make([]uint64, 0), rootHash: nil},
		storage: storage,
	}

	if header.rootAddress != nil {
		node, err := nodestore.readNodeFromDisk(LinearAddress(*header.rootAddress))
		if err != nil {
			return nil, err
		}
		nodestore.kind.(Committed).rootHash = &node.Hash
	}

	return nodestore, nil
}

func (ns *NodeStore) newEmptyCommitted(storage Storage) (*NodeStore, error) {
	header := NewNodeStoreHeader()
	return &NodeStore{
		header:  header,
		kind:    Committed{deleted: make([]uint64, 0), rootHash: nil},
		storage: storage,
	}, nil
}

type Parentable interface {
	AsNodeStoreParent() NodeStoreParent
	RootHash() *uint64
}

func (ip *ImmutableProposal) AsNodeStoreParent() NodeStoreParent {
	return NodeStoreParentProposed{parent: ip}
}

func (ip *ImmutableProposal) RootHash() *uint64 {
	return ip.rootHash
}

func (ns *NodeStore) commitReparent(other *NodeStore) bool {
	switch other.kind.(type) {
	case NodeStoreParentProposed:
		parent := other.kind.(NodeStoreParentProposed).parent
		if ns.kind == parent {
			other.kind = NodeStoreParentCommitted{rootHash: ns.kind.(Committed).rootHash}
			return true
		}
	}
	return false
}

func (c *Committed) AsNodeStoreParent() NodeStoreParent {
	return NodeStoreParentCommitted{rootHash: c.rootHash}
}

func (c *Committed) RootHash() *uint64 {
	return c.rootHash
}

func (ns *NodeStore) newMutableProposal(parent *NodeStore) (*NodeStore, error) {
	var deleted []uint64
	var root *Node
	if parent.header.rootAddress != nil {
		deleted = append(deleted, *parent.header.rootAddress)
		node, err := parent.readNodeFromDisk(LinearAddress(*parent.header.rootAddress))
		if err != nil {
			return nil, err
		}
		root = node
	}

	kind := MutableProposal{
		root:    root,
		deleted: deleted,
		parent:  parent.kind.AsNodeStoreParent(),
	}

	return &NodeStore{
		header:  parent.header,
		kind:    kind,
		storage: parent.storage,
	}, nil
}

func (ns *NodeStore) deleteNode(addr LinearAddress) {
	ns.kind.(MutableProposal).deleted = append(ns.kind.(MutableProposal).deleted, uint64(addr))
}

func (ns *NodeStore) readForUpdate(addr LinearAddress) (*Node, error) {
	ns.deleteNode(addr)
	node, err := ns.readNodeFromDisk(addr)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (ns *NodeStore) mutRoot() *Node {
	return ns.kind.(MutableProposal).root
}

func (ns *NodeStore) newEmptyProposal(storage Storage) *NodeStore {
	header := NewNodeStoreHeader()
	headerBytes := new(bytes.Buffer)
	if err := binary.Write(headerBytes, binary.LittleEndian, &header); err != nil {
		panic(err)
	}
	if err := storage.Write(0, headerBytes.Bytes()); err != nil {
		panic(err)
	}
	return &NodeStore{
		header:  header,
		kind:    MutableProposal{root: nil, deleted: make([]uint64, 0), parent: NodeStoreParentCommitted{rootHash: nil}},
		storage: storage,
	}
}

func (ns *NodeStore) allocateFromFreed(n uint64) (*LinearAddress, AreaIndex, error) {
	indexWanted, err := areaSizeToIndex(n)
	if err != nil {
		return nil, 0, err
	}

	for i := int(indexWanted); i < NUM_AREA_SIZES; i++ {
		if ns.header.freeLists[i] != nil {
			address := *ns.header.freeLists[i]
			ns.header.freeLists[i] = nil

			freeHead := ns.storage.FreeListCache(address)
			if freeHead != nil {
				ns.header.freeLists[i] = freeHead
			} else {
				freeAreaAddr := address
				freeHeadStream, err := ns.storage.StreamFrom(freeAreaAddr)
				if err != nil {
					return nil, 0, err
				}
				var freeHead StoredArea[Area[Node, FreeArea]]
				if err := gob.NewDecoder(freeHeadStream).Decode(&freeHead); err != nil {
					return nil, 0, err
				}
				if freeHead.area.Free.nextFreeBlock != nil {
					ns.header.freeLists[i] = freeHead.area.Free.nextFreeBlock
				}
			}

			return &LinearAddress(address), AreaIndex(i), nil
		}
	}

	return nil, 0, nil
}

func (ns *NodeStore) allocateFromEnd(n uint64) (LinearAddress, AreaIndex, error) {
	index, err := areaSizeToIndex(n)
	if err != nil {
		return 0, 0, err
	}
	areaSize := AREA_SIZES[index]
	addr := LinearAddress(ns.header.size)
	ns.header.size += areaSize
	return addr, index, nil
}

func (ns *NodeStore) storedLen(node *Node) uint64 {
	area := Area[Node, FreeArea]{Node: node}
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(area); err != nil {
		panic(err)
	}
	return uint64(buffer.Len()) + 1
}