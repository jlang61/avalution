package merkledb

import (
	"os"
	"reflect"
	"testing"
)



// TestWrite tests the write function of diskMgr
func TestDiskMgrWrite(t *testing.T) {
	fileName := "testfile.db"
	dm, err := newDiskManager(nil, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer os.Remove(dm.file.Name())
	defer dm.file.Close()

	// Test Case 1: Write data with no free space available (end of file)
	writeData1 := []byte("First data block")

	addr1, err := dm.write(writeData1)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}
	// log.Println(addr1)

	// Verify the data is written correctly by reading it back
	readData1, err := dm.get(addr1)
	// log.Println(string(readData1))
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	if string(readData1) != string(writeData1) {
		t.Errorf("data mismatch: expected %s, got %s", writeData1, readData1)
	}

	// Test Case 2: Write data that fits into the previous bucket space
	dm.putBack(addr1)

	writeData2 := []byte("Xxx data block") // 14 bytes
	addr2, err := dm.write(writeData2)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// log.Println(addr2)

	// Verify the data is correctly written at the reused free space location
	readData2, err := dm.get(addr2)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	// log.Println(string(readData2))

	if string(readData2) != string(writeData2) {
		t.Errorf("data mismatch: expected %s, got %s", writeData2, readData2)
	}

	// Test Case 3: Now write to the end of file again and see if padding persists
	writeData3 := []byte("Third data block") // 16 bytes
	addr3, err := dm.write(writeData3)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// log.Println(addr3)

	// Verify the data is correctly written at the end
	readData3, err := dm.get(addr3)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	// log.Println(string(readData3))

	if string(readData3) != string(writeData3) {
		t.Errorf("data mismatch: expected %s, got %s", writeData3, readData3)
	}

	// Test to see that there is indeed a padding between 2 and 3
	readData4 := make([]byte, 32)
	_, err = dm.file.ReadAt(readData4, 0)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	pad := []byte{0, 0}

	// log.Println(readData4[14:20])
	if !reflect.DeepEqual(readData4[14:16], pad) {
		t.Errorf("Padding does not exist")
	}
	// log.Println(readData4)
}

// TestGet tests the get method of diskMgr
func TestDiskMgrGet(t *testing.T) {
	fileName := "testfile.db"
	dm, err := newDiskManager(nil, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer os.Remove(dm.file.Name())
	defer dm.file.Close()

	// Write test data at a specific offset
	writeData := []byte("Hello, disk manager!")
	addr := diskAddress{offset: 0, size: int64(len(writeData))}
	_, err = dm.file.WriteAt(writeData, addr.offset)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Use get method to retrieve the data
	readData, err := dm.get(addr)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	// log.Printf("Read bytes 1: %v\n", string(readData))
	// Verify the read data matches what was written
	if string(readData) != string(writeData) {
		t.Errorf("data mismatch: expected %s, got %s", writeData, readData)
	}

	// write at offset
	writeData = []byte("Hello again, disk manager!")
	addr = diskAddress{offset: 20, size: int64(len(writeData))}
	_, err = dm.file.WriteAt(writeData, addr.offset)
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	// Use get method to retrieve the data
	readData, err = dm.get(addr)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}
	// log.Printf("Read bytes 2: %v\n", string(readData))
	// Verify the read data matches what was written
	if string(readData) != string(writeData) {
		t.Errorf("data mismatch: expected %s, got %s", writeData, readData)
	}
}

func TestGetHeader_Success(t *testing.T) {
	fileName := "testfile.db"
	metaData := make([]byte, metaSize) // Creating metadata with the fixed size of 16 bytes
	for i := 0; i < metaSize; i++ {
		metaData[i] = byte(i) // Fill metadata with distinct values for testing
	}
	// Create a new Disk Manager
	dm, err := newDiskManager(metaData, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer os.Remove(dm.file.Name()) // Clean up the file after the test
	defer dm.file.Close()

	// Retrieve the header (metadata) using getHeader()
	header, err := dm.getHeader()
	if err != nil {
		t.Fatalf("failed to get metadata header: %v", err)
	}

	// Check that the header matches the initial metadata
	if len(header) != metaSize {
		t.Fatalf("expected metadata size of 16 bytes, got %d bytes", len(header))
	}

	for i, b := range header {
		if b != byte(i) {
			t.Errorf("expected header byte %d to be %d, but got %d", i, byte(i), b)
		}
	}

	// Log the retrieved metadata for verification purposes
	// log.Printf("Metadata successfully written and verified: %v", header)
}

func TestGetHeader_EmptyMetadata(t *testing.T) {
	fileName := "testfile.db"
	dm, err := newDiskManager(nil, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	defer os.Remove(dm.file.Name()) // Clean up the file after the test
	defer dm.file.Close()
	// Retrieve the header (metadata) using getHeader()
	header, err := dm.getHeader()
	if err != nil {
		t.Fatalf("failed to get metadata header: %v", err)
	}

	// Check that the header matches the initial empty metadata
	if len(header) != metaSize {
		t.Fatalf("expected metadata size of 16 bytes, got %d bytes", len(header))
	}

	for i, b := range header {
		if b != 0 {
			t.Errorf("expected header byte %d to be 0, but got %d", i, b)
		}
	}

	// Log the retrieved metadata for verification purposes
	// log.Printf("Empty metadata successfully written and verified: %v", header)
}

func TestNewDiskManager_WithExistingMetadata(t *testing.T) {
	fileName := "testfile.db"
	metaData := make([]byte, metaSize) // 16-byte metadata
	for i := range metaData {
		metaData[i] = byte(i)
	}

	// Step 1: Create the disk manager with metadata
	dm, err := newDiskManager(metaData, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	dm.file.Close()

	// Step 2: Re-open the disk manager, expecting it to read the existing metadata
	dm, err = newDiskManager(nil, ".", fileName)
	if err != nil {
		t.Fatalf("failed to create disk manager with existing metadata: %v", err)
	}
	defer os.Remove(dm.file.Name()) // Clean up the file after the test
	defer dm.file.Close()

	// Step 3: Retrieve the header (metadata) from the disk manager
	header, err := dm.getHeader()
	if err != nil {
		t.Fatalf("failed to get header: %v", err)
	}

	// Step 4: Verify that the header matches the original metadata
	if len(header) != len(metaData) {
		t.Errorf("header size mismatch: expected %d bytes, got %d bytes", len(metaData), len(header))
	}

	if len(header) != len(metaData) {
		t.Errorf("metadata mismatch: expected %v, got %v", metaData, header)
	}
	for i := range header {
		if header[i] != metaData[i] {
			t.Errorf("metadata mismatch: expected %v, got %v", metaData, header)
		}
	}
	// log.Printf("Metadata successfully verified: %v", header)
}
