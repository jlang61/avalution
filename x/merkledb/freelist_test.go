package merkledb

import (
	"bytes"
	"log"
	"os"
	"testing"
)

func TestFreelist(t *testing.T) {
	maxSize := 1024
	f := newFreelist(maxSize)

	// Create diskAddresses of different sizes
	addresses := []diskAddress{
		{offset: 0, size: 1},
		{offset: 1, size: 2},
		{offset: 2, size: 4},
		{offset: 3, size: 8},
		{offset: 4, size: 16},
		{offset: 5, size: 32},
		{offset: 6, size: 64},
		{offset: 7, size: 128},
		{offset: 8, size: 256},
		{offset: 9, size: 512},
		{offset: 10, size: 1024},
	}

	// Put addresses into the freelist
	for _, addr := range addresses {
		f.put(addr)
	}

	// Get addresses from the freelist and ensure they come from the right bucket
	for _, addr := range addresses {
		retrievedAddr, ok := f.get(addr.size)
		log.Println(retrievedAddr)
		if !ok {
			t.Fatalf("failed to get address of size %d", addr.size)
		}
		if retrievedAddr.size != addr.size {
			t.Errorf("expected size %d, got %d", addr.size, retrievedAddr.size)
		}
	}
}

func TestFreelistClose(t *testing.T) {
	// Create a new freelist
	f := newFreelist(1024)

	// Add some diskAddresses to the freelist
	for i := int64(1); i <= 10; i++ {
		f.put(diskAddress{offset: i, size: 1 << i})
	}

	// Close the freelist and write to file
	f.close()

	// Open the file to read back the data
	file, err := os.Open("freelist.db")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Read back the data and verify it
	var offset int64 = 0
	for i := int64(1); i <= 10; i++ {
		expected := diskAddress{offset: i, size: 1 << i}
		expectedBytes := expected.bytes()

		readBytes := make([]byte, 16)
		log.Println("Expected: ", expected.bytes())
		log.Println("Read: ", readBytes)
		n, err := file.ReadAt(readBytes, offset)
		if err != nil {
			t.Fatalf("failed to read data: %v", err)
		}
		if n != 16 {
			t.Fatalf("expected to read 16 bytes, read %d bytes", n)
		}

		if !bytes.Equal(readBytes, expectedBytes[:]) {
			t.Fatalf("expected %v, got %v", expectedBytes, readBytes)
		}

		offset += 16
	}
}

func TestFreelistCloseWritesToFile(t *testing.T) {
	// Create a new freelist
	f := newFreelist(1024)

	// Add some diskAddresses to the freelist
	for i := int64(1); i <= 10; i++ {
		f.put(diskAddress{offset: i, size: 1 << i})
	}

	// Close the freelist and write to file
	f.close()

	// Open the file to check if something is written
	file, err := os.Open("freelist.db")
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Check the file size
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("failed to get file info: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Fatalf("expected file size to be greater than 0, got %d", fileInfo.Size())
	}
}

