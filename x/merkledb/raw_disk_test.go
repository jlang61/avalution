package merkledb

import (
	"bytes"
	"os"
	"testing"
)

func TestWriteBytes(t *testing.T) {
	// Create a temporary file
	r, err := newRawDisk(".")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(r.file.Name()) // clean up the file after the test
	defer r.file.Close()

	// Data to be written
	testData := []byte("Hello, World!\n")

	// First write
	if err := r.writeBytes(testData); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	// Second write to append more data to the file
	if err := r.writeBytes(testData); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	// Read back the contents of the file to verify
	content, err := os.ReadFile(r.file.Name())
	if err != nil {
		t.Fatalf("failed to read back file contents: %v", err)
	}

	// Verify the content is as expected (testData twice in succession)
	expectedContent := bytes.Repeat(testData, 2)
	if !bytes.Equal(content, expectedContent) {
		t.Errorf("file content does not match expected content.\nGot:\n%s\nExpected:\n%s", content, expectedContent)
	}
}