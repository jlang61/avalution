package merkledb

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

func TestWriteNode_Success(t *testing.T) {
	// Create an instance of rawDisk
	r, err := newRawDisk(".")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()           // Do not delete the file, simply close it

	// Initialize a node for testing
	testKey := Key{
		length: 16,
		value:  "test_key_dub",
	}

	testChild := &child{
		compressedKey: Key{
			length: 8,
			value:  "child_key_dub_again",
		},
		id:       ids.Empty,
		hasValue: true,
	}

	testDBNode := dbNode{
		value: maybe.Some([]byte("test_value_dub_dub")),
		children: map[byte]*child{
			0: testChild,
		},
	}

	testNode := &node{
		dbNode:      testDBNode,
		key:         testKey,
		valueDigest: maybe.Some([]byte("value_digest_example")),
	}

	// Convert node to bytes for writing
	nodeBytes := testNode.bytes()

	// Write node bytes to the file
	if err := r.writeNode(testNode, 0); err != nil {
		t.Fatalf("writeNode failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected
	if !bytes.Equal(content, nodeBytes) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, nodeBytes)
	}
}

/*func TestWriteChanges_Success(t *testing.T) {
	r, err := newRawDisk(".")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name())
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
				},
			},
		},
		key:         Key{length: 8, value: "key2"},
		valueDigest: maybe.Some([]byte("digest2")),
	}

	// Creating a change summary
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			Key{length: 8, value: "key1"}: {
				after: node1,
			},
			Key{length: 8, value: "key2"}: {
				after: node2,
			},
		},
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

	// Verify the content is as expected (node1 and node2 serialized bytes)
	node1Bytes := node1.bytes()
	node2Bytes := node2.bytes()
	expectedContent := append(node1Bytes, node2Bytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}*/

func TestWriteChanges_Success(t *testing.T) {
	r, err := newRawDisk(".")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer r.file.Close()

	// Creating diskNodes to add to the change summary
	diskNode1 := &diskNode{
		node: node{
			dbNode: dbNode{
				value: maybe.Some([]byte("value1")),
				children: map[byte]*child{
					1: {
						compressedKey: Key{length: 8, value: "key1"},
						id:            ids.GenerateTestID(),
						hasValue:      true,
					},
				},
			},
			key:         Key{length: 8, value: "key1"},
			valueDigest: maybe.Some([]byte("digest1")),
		},
		diskAddr: diskAddress{offset: 0, size: 100},
	}

	diskNode2 := &diskNode{
		node: node{
			dbNode: dbNode{
				value: maybe.Some([]byte("value2")),
				children: map[byte]*child{
					2: {
						compressedKey: Key{length: 8, value: "key2"},
						id:            ids.GenerateTestID(),
						hasValue:      true,
					},
				},
			},
			key:         Key{length: 8, value: "key2"},
			valueDigest: maybe.Some([]byte("digest2")),
		},
		diskAddr: diskAddress{offset: 100, size: 150},
	}
	rootNode := &diskNode{
		node: node{
			dbNode: dbNode{
				value: maybe.Some([]byte("rootValue")),
				children: map[byte]*child{
					3: {
						compressedKey: Key{length: 8, value: "key3"},
						id:            ids.GenerateTestID(),
						hasValue:      true,
					},
				},
			},
			key:         Key{length: 8, value: "key3"},
			valueDigest: maybe.Some([]byte("digest3")),
		},
		diskAddr: diskAddress{offset: 32, size: 16},
	}

	// Creating a diskChangeSummary
	changeSummary := &diskChangeSummary{
		nodes: map[Key]*change[*diskNode]{
			Key{length: 8, value: "key1"}: {
				after: diskNode1,
			},
			Key{length: 8, value: "key2"}: {
				after: diskNode2,
			},
		},
		rootChange: change[maybe.Maybe[*diskNode]]{
			after: maybe.Some(rootNode),
		},
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

	// Verify the content is as expected (diskNode1 and diskNode2 serialized bytes)
	node1Bytes := diskNode1.bytes()
	node2Bytes := diskNode2.bytes()
	rootNodeBytes := rootNode.bytes()
	//log.Printf("Serialized diskNode1 bytes: %v\n", node1Bytes)
	//log.Printf("Serialized diskNode2 bytes: %v\n", node2Bytes)
	expectedContent := append(node1Bytes, node2Bytes...)
	expectedContent = append(expectedContent, rootNodeBytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}
