package merkledb

import (
	"log"
	"math"
	"os"
)

// power of 2 implementation managing single file

// be able to write metadata - might need to modify
// arbitray node can write to arbitrary location in disk store
// be able to read arbitrary location in disk store - what is the root node
// store disk address - first 16 bytes in file
// can traverse through the entire tree with the root and children
// abstract the tree - getnode should nto work
// fixed size at the front - get fixed data - return byte array

// write byte array - keep diskaddres
type freeList struct {
	buckets [][]diskAddress
}

// newFreeList creates a new freeList with the specified maximum size.
func newFreeList(maxSize int) *freeList {
	numBuckets := int(math.Log2(float64(maxSize))) + 1
	// 11 if 1024 is passed in as maxSize
	buckets := make([][]diskAddress, numBuckets)
	return &freeList{
		buckets: buckets,
	}
}

// Buckets should be initialized as follows:
// max size of buckets[0] = 1 -> 1 byte
// max size of buckets[1] = 2 -> 2 byes
// max size of buckets[2] = 4 -> 4 bytes,

// get retrieves a diskAddress from the freeList that can accommodate the specified size.
// It returns the diskAddress and a boolean indicating whether a suitable address was found.
func (f *freeList) get(size int64) (diskAddress, bool) {
	bucket := f.bucketIndex(size)
	for i := bucket; i < len(f.buckets); i++ {
		if len(f.buckets[i]) > 0 {
			space := f.buckets[i][len(f.buckets[i])-1]
			f.buckets[i] = f.buckets[i][:len(f.buckets[i])-1]
			return space, true
		}
	}
	// No suitable free block available
	return diskAddress{}, false
}

// put adds a diskAddress to the freeList.
func (f *freeList) put(space diskAddress) {
	bucket := f.bucketIndex(space.size)
	f.buckets[bucket] = append(f.buckets[bucket], space)
}

// bucketIndex returns the index of the bucket that the size belongs to.
func (f *freeList) bucketIndex(size int64) int {
	return int(math.Ceil(math.Log2(float64(size))))
}

// close writes the remaining diskAddresses in the freeList to a file and closes the file.
func (f *freeList) close() error {
	r, err := newRawDisk(".", "freeList.db")
	if err != nil {
		log.Fatalf("failed to create temp file: %v", err)
	}
	defer r.file.Close()

	var offset int64 = 0

	// Iterate over each pool to write remaining diskAddresses to file
	for _, pool := range f.buckets {
		// Write each diskAddress to the file
		for _, space := range pool {
			// Encode the diskAddress to bytes
			data := space.bytes()
			// log.Print(space.bytes())

			// Write the bytes at the current offset, returns number of bytes written
			n, err := r.file.WriteAt(data[:], offset)
			if err != nil {
				panic(err)
			}
			// if r.file.Sync() == nil {
			// 	log.Println("Data written successfully at the end of the file BIG DUB.")
			// }
			// Increment the offset by the number of bytes written
			offset += int64(n)
		}
	}
	// ensures that the file is written to disk
	// log.Println(os.ReadFile("freeList.db"))
	err = r.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

// freelist should always be running
// merkle.db, freelist

// load reads the diskAddresses from a file and populates the freeList.
func (f *freeList) load() error {
	file, err := os.Open("freelist.db")
	if err != nil {
		return err
	}
	// check size of stuff inside freelist

	defer file.Close()

	var offset int64 = 0

	// Read the file and populate the freeList
	for {
		readBytes := make([]byte, 16)
		n, err := file.ReadAt(readBytes, offset)
		if err != nil {
			break
		}
		if n != 16 {
			break
		}

		var space diskAddress
		space.decode(readBytes)

		// Put the diskAddress in the appropriate pool
		f.put(space)

		offset += 16
	}
	return nil
}
