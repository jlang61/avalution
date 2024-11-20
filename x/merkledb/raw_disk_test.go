package merkledb

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

func TestWriteChanges_Success(t *testing.T) {
	os.Remove("merkle.db")
	os.Remove("freelist.db")
	// make sure the file is deleted before moving on
	time.Sleep(1 * time.Second)

	tempDir, err := os.MkdirTemp("", "merkledb_test")
	if err != nil {
		//t.Fatalf("failed to create temp directory: %v", err)
	}

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		//t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer os.RemoveAll(tempDir)    // Clean up the directory and all its contents after the test
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
		//t.Fatalf("write changes failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		//t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (node1, node2, and rootNode serialized bytes)
	node1Bytes := node1.raw_disk_bytes()
	node2Bytes := node2.raw_disk_bytes()
	rootNodeBytes := rootNode.raw_disk_bytes()
	// log.Printf("Serialized node1 bytes: %v\n", node1Bsytes)
	// log.Printf("Serialized node2 bytes: %v\n", node2Bytes)
	log.Printf("Serialized rootNode bytes: %v\n", rootNodeBytes)

	// Create the expected content by appending node and root bytes
	expectedContent := append(node1Bytes, node2Bytes...)
	//expectedContent = append(expectedContent, rootNodeBytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}

func TestGetRootKey_Success(t *testing.T) {

	r, err := newRawDisk(".")
    if err != nil {
        t.Fatalf("failed to create temp file: %v", err)
    }
    defer os.Remove(r.file.Name()) 
    defer r.file.Close()          

    knownRootKey := []byte("1234567890abcdef") 
    if err := r.writeBytes(knownRootKey, 1); err != nil {
        t.Fatalf("failed to write known root key: %v", err)
    }

    rootKey, err := r.getRootKey()
    if err != nil {
        t.Fatalf("failed to get root key: %v", err)
    }

    if !bytes.Equal(rootKey, knownRootKey) {
        t.Errorf("expected root key %x, but got %x", knownRootKey, rootKey)
    }
	log.Println("oot key test success")
}

func TestFreeListWriteChanges(t *testing.T) {
	os.Remove("merkle.db")
	os.Remove("freelist.db")
	// make sure the file is deleted before moving on
	time.Sleep(1 * time.Second)
	tempDir, err := os.MkdirTemp("", "merkledb_test_")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up the directory and all its contents after the test

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()

	// Creating initial diskNodes to add to the change summary

	node1 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value1")),
			children: map[byte]*child{
				1: {
					compressedKey: Key{length: 8, value: "key1____"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 0, size: 120},
				},
			},
		},
		key:         Key{length: 8, value: "key1____"},
		valueDigest: maybe.Some([]byte("digest1")),
		diskAddr:    diskAddress{offset: 0, size: 120},
	}

	node2 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value2")),
			children: map[byte]*child{
				2: {
					compressedKey: Key{length: 8, value: "key2____"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 100, size: 150},
				},
			},
		},
		key:         Key{length: 8, value: "key2____"},
		valueDigest: maybe.Some([]byte("digest2")),
		diskAddr:    diskAddress{offset: 100, size: 150},
	}

	// Creating a diskChangeSummary for initial nodes
	initialChangeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			Key{length: 8, value: "key1____"}: {
				after: node1,
			},
			Key{length: 8, value: "key2____"}: {
				after: node2,
			},
		},
	}
	// Write initial changes to the file
	if err := r.writeChanges(context.Background(), initialChangeSummary); err != nil {
		t.Fatalf("write initial changes failed: %v", err)
	}
	// Creating a new node to replace node1
	newnode1 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("new_value1")),
			children: map[byte]*child{
				1: {
					compressedKey: Key{length: 8, value: "new_key1"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 0, size: 100},
				},
			},
		},
		key:         Key{length: 8, value: "new_key1"},
		valueDigest: maybe.Some([]byte("new_digest1")),
		diskAddr:    diskAddress{offset: 0, size: 100},
	}

	// Creating a diskChangeSummary for the new changes
	newChangeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			Key{length: 8, value: "key1____"}: {
				before: node1,
				after:  newnode1,
			},
		},
	}

	// Write new changes to the file
	if err := r.writeChanges(context.Background(), newChangeSummary); err != nil {
		t.Fatalf("write new changes failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	} else {
		log.Println("Read back file contents successfully.")
	}
	// Verify the content is as expected (newDiskNode1 and diskNode2 serialized bytes)
	node1Bytes := node1.raw_disk_bytes()
	node2Bytes := node2.raw_disk_bytes()
	newNode1Bytes := newnode1.raw_disk_bytes()
	expectedContent := append(node1Bytes, node2Bytes...)
	expectedContent = append(expectedContent, newNode1Bytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
	newnode2 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("new_value2")),
			children: map[byte]*child{
				1: {
					compressedKey: Key{length: 8, value: "new_key2"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 0, size: 100},
				},
			},
		},
		key:         Key{length: 8, value: "new_key2"},
		valueDigest: maybe.Some([]byte("new_digest2")),
		diskAddr:    diskAddress{offset: 0, size: 100},
	}

	newChangeSummary2 := &changeSummary{
		nodes: map[Key]*change[*node]{
			Key{length: 8, value: "key2___"}: {
				after: newnode2,
			},
		},
	}

	// Write new changes to the file
	if err := r.writeChanges(context.Background(), newChangeSummary2); err != nil {
		t.Fatalf("write new changes failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err = os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	} else {
		log.Println("Read back file contents successfully.")
	}
	// Verify the content is as expected (newDiskNode1 and diskNode2 serialized bytes)
	// The write should overwrite node 1, and then put in new value 2
	newNode2Bytes := newnode2.raw_disk_bytes()
	expectedContent = append(newNode2Bytes, node2Bytes...)
	expectedContent = append(expectedContent, newNode1Bytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	} else {
		log.Printf("Got: \n%s", expectedContent)
	}

	// Verify that the freelist contains the expected diskAddresses
	// expectedFreeList := []diskAddress{diskNode1.diskAddr}
	// for _, expectedAddr := range expectedFreeList {
	// 	retrievedAddr, ok := freelist.get(expectedAddr.size)
	// 	if !ok {
	// 		t.Fatalf("failed to get address of size %d from freelist", expectedAddr.size)
	// 	}
	// 	if retrievedAddr != expectedAddr {
	// 		t.Errorf("expected %v, got %v", expectedAddr, retrievedAddr)
	// 	}
	// }
}
