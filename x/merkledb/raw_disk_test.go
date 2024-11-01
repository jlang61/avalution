package merkledb

import (
	"bytes"
	"log"
	"os"
	"testing"
)

func TestAppendBytes(t *testing.T) {
	// Create a temporary file
	r, err := newRawDisk(".")
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
	r, err := newRawDisk(".")
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
	r, err := newRawDisk(".")
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