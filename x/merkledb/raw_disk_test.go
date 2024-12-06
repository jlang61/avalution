package merkledb

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
	"time"
  "errors"

	// "github.com/ava-labs/avalanchego/app"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
	// "golang.org/x/tools/go/expect"
)

const testMetaSize = metaSize + 1

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
	// log.Printf("Current size: %v\n", currentSize)
	nextPowerOf2Size := nextPowerOf2(currentSize)

	// Add dummy bytes to reach the next power of 2 size
	paddingSize := nextPowerOf2Size - currentSize
	// log.Printf("Padding size: %v\n", paddingSize)
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		encodedBytes = append(encodedBytes, padding...)
	}
	return encodedBytes
}
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
	defer os.Remove(r.dm.file.Name()) // clean up the file after the test
	defer os.RemoveAll(tempDir)       // Clean up the directory and all its contents after the test
	defer r.dm.file.Close()

	
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

	// rootNode := &node{
	// 	dbNode: dbNode{
	// 		value: maybe.Some([]byte("rootValue")),
	// 		children: map[byte]*child{
	// 			3: {
	// 				compressedKey: Key{length: 8, value: "key3"},
	// 				id:            ids.GenerateTestID(),
	// 				hasValue:      true,
	// 				diskAddr:      diskAddress{offset: 32, size: 16},
	// 			},
	// 		},
	// 	},
	// 	key:         Key{length: 8, value: "key3"},
	// 	valueDigest: maybe.Some([]byte("digest3")),
	// }

	// Creating a change summary with nodes and rootChange
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			{length: 8, value: "key1"}: {
				after: node1,
			},
			{length: 8, value: "key2"}: {
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
	content, err := os.ReadFile(r.dm.file.Name())
	if err != nil {
		//t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (node1, node2, and rootNode serialized bytes)
	node1Bytes := node1.raw_disk_bytes()
	node2Bytes := node2.raw_disk_bytes()
	// rootNodeBytes := rootNode.raw_disk_bytes()
	// log.Printf("Serialized node1 bytes: %v\n", node1Bytes)
	// log.Printf("Serialized node2 bytes: %v\n", node2Bytes)
	// log.Printf("Serialized rootNode bytes: %v\n", rootNodeBytes)

	// Create the expected content by appending node and root bytes
	expectedContent := append(make([]byte, testMetaSize), node1Bytes...)

	expectedContent = append(expectedContent, node2Bytes...)
	otherExpectedContent := append(make([]byte, testMetaSize), node2Bytes...)
	otherExpectedContent = append(otherExpectedContent, node1Bytes...)
	// log.Printf("file content bytes: %v\n", content)
	//expectedContent = append(expectedContent, rootNodeBytes...)
	if !bytes.Equal(content, expectedContent) && !bytes.Equal(content, otherExpectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
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
	defer os.Remove(r.dm.file.Name()) // clean up the file after the test
	defer r.dm.file.Close()

	// Creating initial diskNodes to add to the change summary


	boss := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("rootValue")),
			children: map[byte]*child{
				'2' : {
					compressedKey: ToKey([]byte("0")),
					id: ids.GenerateTestID(),
					hasValue: true,
				},
				'3' : {
					compressedKey: ToKey([]byte("0")),
					id: ids.GenerateTestID(),
					hasValue: true,
				},
			},
		},
		key: ToKey([] byte("1")),
		valueDigest: maybe.Some([]byte("digest")),
	}

	henchMan1 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value1")),
			children: map[byte]*child{},
		},
		key: ToKey([] byte ("120")),
		valueDigest: maybe.Some([]byte("digest1")),
	}

	henchMan2 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value2")),
			children: map[byte]*child{},
		},
		key: ToKey([] byte ("130")),
		valueDigest: maybe.Some([]byte("digest2")),
	}

	initialChangeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			boss.key: {
				after: boss,
			},
			henchMan1.key: {
				after: henchMan1,
			},
			henchMan2.key: {
				after: henchMan2,
			},
		},
		rootChange: change[maybe.Maybe[*node]]{
			after: maybe.Some(boss),
		},
	}
	// Write initial changes to the file
	if err := r.writeChanges(context.Background(), initialChangeSummary); err != nil {
		t.Fatalf("write initial changes failed: %v", err)
	}

	removed := &changeSummary{	
		nodes: map[Key]*change[*node]{
			henchMan1.key: {
				before: henchMan1,
			},
		},
	}

	if err := r.writeChanges(context.Background(), removed); err != nil {
		t.Fatalf("write removed changes failed: %v", err)
	}

	// Creating a new node to replace henchman
	secretAgent := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("new_value1")),
			children: map[byte]*child{},
		},
		key:         ToKey([] byte ("140")),
		valueDigest: maybe.Some([]byte("new_digest1")),
	}

	// Creating a diskChangeSummary for the new changes
	replacement := &changeSummary{
		nodes: map[Key]*change[*node]{
			secretAgent.key: {
				after: secretAgent,
			},
		},
	}

	if err := r.writeChanges(context.Background(), replacement); err != nil {
		t.Fatalf("write replacement changes failed: %v", err)
	}


	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.dm.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	} else {
		log.Println("Read back file contents successfully.")
	}	
	// Verify the content is as expected (newDiskNode1 and diskNode2 serialized bytes)
	bossBytes := boss.raw_disk_bytes()
	
	henchMan2Bytes := henchMan2.raw_disk_bytes()
	henchMan1Bytes := henchMan1.raw_disk_bytes()


	secretAgentBytes := secretAgent.raw_disk_bytes()

	rootNodeAddrBytes := diskAddress{offset: 65, size: 116}.bytes()
	rootKeyAddrBytes := diskAddress{offset: 193, size: 1}.bytes() 
	metaData := append(rootNodeAddrBytes[:], rootKeyAddrBytes[:]...)
	rootKeyBytes, err := r.getRootKey()
	log.Printf("root key bytes: %v\n", rootKeyBytes)
	expectedContent := append(make([]byte, 1), metaData...)
	expectedContent = append(expectedContent, secretAgentBytes...)
	expectedContent = append(expectedContent, henchMan2Bytes...)
	expectedContent = append(expectedContent, bossBytes...)
	expectedContent = append(expectedContent, rootKeyBytes...)
	otherExpectedContent := append(make([]byte, testMetaSize), henchMan2Bytes...)
	otherExpectedContent = append(otherExpectedContent, henchMan1Bytes...)
	otherExpectedContent = append(otherExpectedContent, bossBytes...)
	otherExpectedContent = append(otherExpectedContent, rootKeyBytes...)

	log.Printf("secret agent bytes: %v\n", secretAgentBytes)
	if !bytes.Equal(content, expectedContent) && !bytes.Equal(content, otherExpectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%v\nExpected:\n%v", content, expectedContent)
	}
	
	
}

func TestWriteChanges_WithRootNode(t *testing.T) {
	// Set up temporary directory and rawDisk
	tempDir, err := os.MkdirTemp("", "merkledb_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		t.Fatalf("failed to create rawDisk: %v", err)
	}
	defer os.Remove(r.dm.file.Name())
	defer r.dm.file.Close()

	// Create root node
	rootNode := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("rootValue")),
			children: map[byte]*child{
				3: {
					compressedKey: Key{length: 8, value: "key3"},
					id:            ids.GenerateTestID(),
					hasValue:      true,
					diskAddr:      diskAddress{offset: 33, size: 16},
				},
			},
		},
		key:         Key{length: 8, value: "key3"},
		valueDigest: maybe.Some([]byte("digest3")),
		diskAddr:    diskAddress{offset: testMetaSize, size: 67},
	}

	// Create changeSummary with rootChange
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			{length: 8, value: "key1____"}: {
				after: rootNode,
			},
		},
		rootChange: change[maybe.Maybe[*node]]{
			after: maybe.Some(rootNode),
		},
	}

	// Write changes to the file
	if err := r.writeChanges(context.Background(), changeSummary); err != nil {
		t.Fatalf("write changes failed: %v", err)
	}

	// Read back the contents of the file
	content, err := os.ReadFile(r.dm.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}
	// Verify the content includes the serialized root node
	diskAddrBytes := diskAddress{offset: testMetaSize, size: 67}.bytes()
	rootAddrBytes := diskAddress{offset: 161, size: 4}.bytes()
	expectedContent := append(append(append(make([]byte, 1), diskAddrBytes[:]...), rootAddrBytes[:]...), rootNode.raw_disk_bytes()...)
	expectedContent = append(expectedContent, rootNode.key.Bytes()...)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%v\nExpected:\n%v", content, expectedContent)
	}
}

func TestWriteChanges_MultipleNodes(t *testing.T) {
	// .
// ..existing code...

	tempDir, err := os.MkdirTemp("", "merkledb_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		t.Fatalf("failed to create rawDisk: %v", err)
	}
	defer os.Remove(r.dm.file.Name())
	defer r.dm.file.Close()

	childNode1 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value1")),
		},
    key:     ToKey([]byte{1,2,5}),
		valueDigest: maybe.Some([]byte("digest5")),
	}

	// Create three child nodes
	node1 := &node{
		dbNode: dbNode{
			children : map[byte]*child{
				byte(5): {
					compressedKey: ToKey(make([]byte,0)),
					id:            ids.GenerateTestID(),
					hasValue:      true,
					// ...existing code...
				},
			},
			// ...existing code...
		},
    key:         ToKey([]byte{1,2}),
		valueDigest: maybe.Some([]byte("digest1")),
		// ...existing code...
	}

	node2 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value2")),
			// ...existing code...
		},
    key:         ToKey([]byte{1,3}),
		valueDigest: maybe.Some([]byte("digest2")),
		// ...existing code...
	}

	node3 := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("value3")),
			// ...existing code...
		},
    key:         ToKey([]byte{1,4}),
		valueDigest: maybe.Some([]byte("digest3")),
		// ...existing code...
	}

	rootNode := &node{
		dbNode: dbNode{
			children: map[byte]*child{
				byte(2): {
					compressedKey: ToKey(make([]byte,0)),
					id:            ids.GenerateTestID(),
					hasValue:      true,
					// ...existing code...
				},
				byte(3): {
					compressedKey: ToKey(make([]byte,0)),
					id:            ids.GenerateTestID(),
					hasValue:      true,
					// ...existing code...
				},
				byte(4): {
					compressedKey: ToKey(make([]byte,0)),
					id:            ids.GenerateTestID(),
					hasValue:      true,
					// ...existing code...
				},
			},
			// ...existing code...
		},
    key: ToKey([]byte{1}),
		// ...existing code...
	}

	// Build changeSummary with rootChange and nodes
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			rootNode.key: {after: rootNode},
			node1.key: {after: node1},
			node2.key: {after: node2},
			node3.key: {after: node3},
			childNode1.key: {after: childNode1},
		},
		rootChange: change[maybe.Maybe[*node]]{
			after: maybe.Some(rootNode),
		},
	}

	// Write changes to disk
	if err := r.writeChanges(context.Background(), changeSummary); err != nil {
		t.Fatalf("write changes failed: %v", err)
	}

	// WriteChanges should write the children first, then their parent nodes
	diskAddrBytes := diskAddress{offset: 145, size: 155}.bytes()
	rootAddrBytes := diskAddress{offset: 401, size: 1}.bytes()
	expectedContent := append(make([]byte, 1), diskAddrBytes[:]...)
	expectedContent = append(expectedContent, rootAddrBytes[:]...)
	startExpectedContent := append(expectedContent, childNode1.raw_disk_bytes()...)
	expectedContent = append(startExpectedContent, node1.raw_disk_bytes()...)
	expectedContent = append(expectedContent, node2.raw_disk_bytes()...)
	expectedContent = append(expectedContent, node3.raw_disk_bytes()...)
	expectedContent = append(expectedContent, rootNode.raw_disk_bytes()...)
	expectedContent = append(expectedContent, rootNode.key.Bytes()...)

	otherExpectedContent3 := append(startExpectedContent, node1.raw_disk_bytes()...)
	otherExpectedContent3 = append(otherExpectedContent3, node3.raw_disk_bytes()...)
	otherExpectedContent3 = append(otherExpectedContent3, node2.raw_disk_bytes()...)
	otherExpectedContent3 = append(otherExpectedContent3, rootNode.raw_disk_bytes()...)
	otherExpectedContent3 = append(otherExpectedContent3, rootNode.key.Bytes()...)

	otherExpectedContent1 := append(startExpectedContent, node2.raw_disk_bytes()...)
	otherExpectedContent1 = append(otherExpectedContent1, node1.raw_disk_bytes()...)
	otherExpectedContent1 = append(otherExpectedContent1, node3.raw_disk_bytes()...)
	otherExpectedContent1 = append(otherExpectedContent1, rootNode.raw_disk_bytes()...)
	otherExpectedContent1 = append(otherExpectedContent1, rootNode.key.Bytes()...)

	otherExpectedContent4 := append(startExpectedContent, node2.raw_disk_bytes()...)
	otherExpectedContent4 = append(otherExpectedContent4, node3.raw_disk_bytes()...)
	otherExpectedContent4 = append(otherExpectedContent4, node1.raw_disk_bytes()...)
	otherExpectedContent4 = append(otherExpectedContent4, rootNode.raw_disk_bytes()...)
	otherExpectedContent4 = append(otherExpectedContent4, rootNode.key.Bytes()...)

	otherExpectedContent2 := append(startExpectedContent, node3.raw_disk_bytes()...)
	otherExpectedContent2 = append(otherExpectedContent2, node1.raw_disk_bytes()...)
	otherExpectedContent2 = append(otherExpectedContent2, node2.raw_disk_bytes()...)
	otherExpectedContent2 = append(otherExpectedContent2, rootNode.raw_disk_bytes()...)
	otherExpectedContent2 = append(otherExpectedContent2, rootNode.key.Bytes()...)

	otherExpectedContent5 := append(startExpectedContent, node3.raw_disk_bytes()...)
	otherExpectedContent5 = append(otherExpectedContent5, node2.raw_disk_bytes()...)
	otherExpectedContent5 = append(otherExpectedContent5, node1.raw_disk_bytes()...)
	otherExpectedContent5 = append(otherExpectedContent5, rootNode.raw_disk_bytes()...)
	otherExpectedContent5 = append(otherExpectedContent5, rootNode.key.Bytes()...)

	// Read back the contents of the file
	content, err := os.ReadFile(r.dm.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected
	// log.Printf("file content bytes: %v\n", content)
	// log.Printf("expected content bytes: %v\n", expectedContent)
	if !bytes.Equal(content, expectedContent) && !bytes.Equal(content, otherExpectedContent1) && !bytes.Equal(content, otherExpectedContent2) && !bytes.Equal(content, otherExpectedContent3) && !bytes.Equal(content, otherExpectedContent4) && !bytes.Equal(content, otherExpectedContent5) { 
		t.Errorf("file content does not match expected content.\nGot:\n%v\nExpected:\n%v", content, expectedContent)
	}

}

func Test_GetNode(t *testing.T) {
	// Set up temporary directory and rawDisk
	tempDir, err := os.MkdirTemp("", "merkledb_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		t.Fatalf("failed to create rawDisk: %v", err)
	}
	defer os.Remove(r.dm.file.Name())
	defer r.dm.file.Close()

	// Create child node
	childNode := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("1234")),
		},
		key:        ToKey([]byte("1234")), 
		valueDigest: maybe.Some([]byte("1234")),
	}

	// Create parent node with the address of the child node
	parentNode := &node{
		dbNode: dbNode{
			value: maybe.Some([]byte("12")),
			children: map[byte]*child{
				'3': {
					compressedKey: ToKey([]byte("4")),
					id:            ids.GenerateTestID(),
					hasValue:      true,
				},
			},
		},
		key:         ToKey([]byte("12")),
		valueDigest: maybe.Some([]byte("12")),
	}

	// write changes
	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			parentNode.key: {
				after: parentNode,
			},
			childNode.key: {
				after: childNode,
			},
		},
		rootChange: change[maybe.Maybe[*node]]{
			after: maybe.Some(parentNode),
		},
	}
	if err := r.writeChanges(context.Background(), changeSummary); err != nil {
		t.Fatalf("write changes failed: %v", err)
	}

	// Test getNode function
	retrievedNode, err := r.getNode(parentNode.key, true)
	if err != nil {
		t.Fatalf("failed to get root node: %v", err)
	}
	retrievedChildNode, err := r.getNode(childNode.key, true)
	if err != nil {
		t.Fatalf("failed to get child node: %v", err)
	}

	// Verify the retrieved node matches the parent node
	if !bytes.Equal(retrievedNode.raw_disk_bytes(), parentNode.raw_disk_bytes()) {
		t.Errorf("retrieved node does not match parent node.\nGot:\n%v\nExpected:\n%v", retrievedNode, parentNode)
	}

	if !bytes.Equal(retrievedChildNode.raw_disk_bytes(), childNode.raw_disk_bytes()) {
		t.Errorf("retrieved node does not match child node.\nGot:\n%v\nExpected:\n%v", retrievedChildNode, childNode)
	}

}

func Test_GetNode_Failure(t *testing.T) {
	// Set up temporary directory and rawDisk
	tempDir, err := os.MkdirTemp("", "merkledb_test")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	r, err := newRawDisk(tempDir, "merkle.db")
	if err != nil {
		t.Fatalf("failed to create rawDisk: %v", err)
	}
	defer os.Remove(r.dm.file.Name())
	defer r.dm.file.Close()

	// Create root node
	rootNode := &node{
		dbNode: dbNode{
			children: map[byte]*child{},
			value:    maybe.Some([]byte("rootNode")),
		},
		// length shoudl be bits of the value
		key:         Key{length: 56, value: "rootKey"},
		valueDigest: maybe.Some([]byte("rootNode")),
	}

	changeSummary := &changeSummary{
		nodes: map[Key]*change[*node]{
			{length: 56, value: "rootKey"}: {
				after: rootNode,
			},
		},
		rootChange: change[maybe.Maybe[*node]]{
			after: maybe.Some(rootNode),
		},
	}

	// Write changes to the file
	if err := r.writeChanges(context.Background(), changeSummary); err != nil {
		t.Fatalf("write changes failed: %v", err)
	}
	// Attempt to retrieve the correct node
	// currNode, err := r.getNode(rootNode.key, true)
	// if err != nil {
	// 	t.Fatalf("failed to get node: %v", err)
	// }
	// log.Printf("Current node: %v\n", currNode.dbNode.value)
	// Attempt to retrieve a node with a different key
	wrongKey := Key{length: 64, value: "wrongKey"}
	_, err = r.getNode(wrongKey, true)
	if errors.Is(err, errors.New("Key doesn't match rootkey")){
		t.Fatalf("failed to fail when getting node with wrong key: %v", err)
	}
}
