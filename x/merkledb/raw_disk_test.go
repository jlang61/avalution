package merkledb

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

func TestWriteChanges_Success(t *testing.T) {
	r, err := newRawDisk(".")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer r.file.Close()

	// Creating nodes to add to the change summary
	node1 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value1")),
			children: map[byte]*child{
				1: {
					compressedKey: Key{length: 8, value: "key1"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 0, size: 100},
				},
			},
		},
		key:         Key{length: 8, value: "key1"},
		valueDigest: maybe.Some([]byte("digest1")),
	}

	node2 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value2")),
			children: map[byte]*child{
				2: {
					compressedKey: Key{length: 8, value: "key2"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 100, size: 150},
				},
			},
		},
		key:         Key{length: 8, value: "key2"},
		valueDigest: maybe.Some([]byte("digest2")),
	}

	rootNode := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("rootValue")),
			children: map[byte]*child{
				3: {
					compressedKey: Key{length: 8, value: "key3"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 32, size: 16},
				},
			},
		},
		key:         Key{length: 8, value: "key3"},
		valueDigest: maybe.Some([]byte("digest3")),
	}

	// Creating a change summary with nodes and rootChange
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			Key{length: 8, value: "key1"}: {
				after: node1,
			},
			Key{length: 8, value: "key2"}: {
				after: node2,
			},
		},
		//rootChange: change[maybe.Maybe[*node]]{
		//	after: maybe.Some(rootNode),
		//},
	}

	// Write changes to the file
	if err := r.writeChanges(context.Background(), changeSummary); err != nil {
		t.Fatalf("write changes failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (node1, node2, and rootNode serialized bytes)
	node1Bytes := node1.bytes()
	node2Bytes := node2.bytes()
	rootNodeBytes := rootNode.bytes()
	log.Printf("Serialized node1 bytes: %v\n", node1Bytes)
	log.Printf("Serialized node2 bytes: %v\n", node2Bytes)
	log.Printf("Serialized rootNode bytes: %v\n", rootNodeBytes)

	// Create the expected content by appending node and root bytes
	expectedContent := append(node1Bytes, node2Bytes...)
	//expectedContent = append(expectedContent, rootNodeBytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}
