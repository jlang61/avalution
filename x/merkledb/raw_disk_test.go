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

func TestAppendBytes(t *testing.T) {
	// Create a temporary file
	r, err := newRawDisk(".", "merkle.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()

	// Data to be written
	testData := []byte("Hello, World!\n")
	//testData := []byte("Bye, World!\n")

	// First write
	if err := r.appendBytes(testData); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write to append more data to the file
	if err := r.appendBytes(testData); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (testData twice in succession)
	expectedContent := bytes.Repeat(testData, 2)
	// log.Println("Content:")
	// log.Println(content)
	// log.Println("Expected Content:")
	// log.Println(expectedContent)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}

func TestAppendBytes2(t *testing.T) {
	// Create a temporary file
	r, err := newRawDisk(".", "merkle.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()

	// Data to be written
	testData1 := []byte("Hello, World!\n")
	testData2 := []byte("Bye, World!\n")

	// First write
	if err := r.appendBytes(testData1); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write to append more data to the file
	if err := r.appendBytes(testData1); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}
	log.Println("Content right now:\n", string(content))

	// Third write
	if err := r.appendBytes(testData2); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Fourth write
	if err := r.appendBytes(testData2); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	content, err = os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}
	log.Println("Content after:\n", string(content))

	// Verify the content is as expected (testData twice in succession)
	expectedContent := bytes.Repeat(testData1, 2)
	expectedContent = append(expectedContent, bytes.Repeat(testData2, 2)...)

	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}

func TestWriteBytes_Success(t *testing.T) {
	r, err := newRawDisk(".", "merkle.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()

	// Populate the file with data
	testData := []byte("Hello, World!\n")

	if err := r.appendBytes(testData); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := r.appendBytes(testData); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := r.appendBytes(testData); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	test2 := []byte("Bye, world!!!\n")

	// write Bye World after one Hello World
	// (replace the second hello world with bye world)
	if err := r.writeBytes(test2, int64(len(testData))); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (testData twice in succession)
	expectedContent := append(testData, test2...)
	expectedContent = append(expectedContent, testData...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}

}

func TestWriteNode_Success(t *testing.T) {
	// Create an instance of rawDisk
	r, err := newRawDisk(".", "merkle.db")
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

func TestFreeListWriteChanges(t *testing.T) {
	os.Remove("merkle.db")
	r, err := newRawDisk(".", "merkle.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer r.file.Close()

	// Creating initial diskNodes to add to the change summary
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

	// Creating a diskChangeSummary for initial nodes
	initialChangeSummary := &diskChangeSummary{
		nodes: map[Key]*change[*diskNode]{
			Key{length: 8, value: "key1"}: {
				after: diskNode1,
			},
			Key{length: 8, value: "key2"}: {
				after: diskNode2,
			},
		},
	}
	// Write initial changes to the file
	if err := r.writeChanges(context.Background(), initialChangeSummary); err != nil {
		t.Fatalf("write initial changes failed: %v", err)
	}

	// Creating a new node to replace diskNode1
	newDiskNode1 := &diskNode{
		node: node{
			dbNode: dbNode{
				value: maybe.Some([]byte("new_value1")),
				children: map[byte]*child{
					1: {
						compressedKey: Key{length: 8, value: "new_key1"},
						id:            ids.GenerateTestID(),
						hasValue:      true,
					},
				},
			},
			key:         Key{length: 8, value: "new_key1"},
			valueDigest: maybe.Some([]byte("new_digest1")),
		},
		diskAddr: diskAddress{offset: 0, size: 100}, // Reuse the same address
	}

	// Creating a diskChangeSummary for the new changes
	newChangeSummary := &diskChangeSummary{
		nodes: map[Key]*change[*diskNode]{
			Key{length: 8, value: "key1"}: {
				before: diskNode1,
				after:  newDiskNode1,
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
	node1Bytes := diskNode1.bytes()
	node2Bytes := diskNode2.bytes()
	newNode1Bytes := newDiskNode1.bytes()
	expectedContent := append(node1Bytes, node2Bytes...)
	expectedContent = append(expectedContent, newNode1Bytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}

	newDiskNode2 := &diskNode{
		node: node{
			dbNode: dbNode{
				value: maybe.Some([]byte("new_value2")),
				children: map[byte]*child{
					1: {
						compressedKey: Key{length: 8, value: "new_key2"},
						id:            ids.GenerateTestID(),
						hasValue:      true,
					},
				},
			},
			key:         Key{length: 8, value: "new_key2"},
			valueDigest: maybe.Some([]byte("new_digest2")),
		},
		diskAddr: diskAddress{offset: 0, size: 100}, // Reuse the same address
	}

	newChangeSummary2 := &diskChangeSummary{
		nodes: map[Key]*change[*diskNode]{
			Key{length: 8, value: "key2"}: {
				after:  newDiskNode2,
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
	newNode2Bytes := newDiskNode2.bytes()
	expectedContent = append(newNode2Bytes, node2Bytes...)
	expectedContent = append(expectedContent, newNode1Bytes...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
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