package merkledb

import (
	"log"
	"math"
	"os"
)

type freeList struct {
	buckets [][]diskAddress
}

func newFreeList(maxSize int) *freeList {
	numBuckets := int(math.Log2(float64(maxSize))) + 1
	buckets := make([][]diskAddress, numBuckets)
	return &freeList{
		buckets: buckets,
	}
}

func (f *freeList) get(size int64) (diskAddress, bool) {
	bucket := f.bucketIndex(size)
	for i := bucket; i < len(f.buckets); i++ {
		if len(f.buckets[i]) > 0 {
			space := f.buckets[i][len(f.buckets[i])-1]
			f.buckets[i] = f.buckets[i][:len(f.buckets[i])-1]
			return space, true
		}
	}
	return diskAddress{}, false
}

func (f *freeList) put(space diskAddress) {
	bucket := f.bucketIndex(space.size)
	f.buckets[bucket] = append(f.buckets[bucket], space)
}

// returns the index of the bucket that the size belongs to
func (f *freeList) bucketIndex(size int64) int {
	return int(math.Log2(float64(size)))
}

func (f *freeList) close() {
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
			log.Println(offset)
			// Encode the diskAddress to bytes
			data := space.bytes()
			// log.Print(space.bytes())
		
			// Write the bytes at the current offset, returns number of bytes written
			n, err := r.file.WriteAt(data[:], offset)
			log.Println("Data written: ", data[:])
			if err != nil {
				panic(err)
			}
			if r.file.Sync() == nil {
				log.Println("Data written successfully at the end of the file BIG DUB.")
			}
			// Increment the offset by the number of bytes written
			offset += int64(n)
		}
	}
	if r.file.Sync() == nil {
		log.Println(os.ReadFile("freeList.db"))
	}
}

func (f *freeList) load() {
	file, err := os.Open("freeList.db")
	if err != nil {
		panic(err)
	}
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
}