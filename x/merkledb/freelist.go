package merkledb

import (
	"log"
	"math"
	"os"
)

type freelist struct {
	buckets [][]diskAddress
}

func newFreelist(maxSize int) *freelist {
	numBuckets := int(math.Log2(float64(maxSize))) + 1
	buckets := make([][]diskAddress, numBuckets)
	return &freelist{
		buckets: buckets,
	}
}

func (f *freelist) get(size int64) (diskAddress, bool) {
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

func (f *freelist) put(space diskAddress) {
	bucket := f.bucketIndex(space.size)
	f.buckets[bucket] = append(f.buckets[bucket], space)
}

// returns the index of the bucket that the size belongs to
func (f *freelist) bucketIndex(size int64) int {
	return int(math.Log2(float64(size)))
}

func (f *freelist) close() {
	r, err := newRawDisk(".", "freelist.db")
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
		log.Println(os.ReadFile("freelist.db"))
	}

}

// func (f *freelist) load() {
// 	file, err := os.Open("freelist.db")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer file.Close()

// 	var offset int64 = 0

// 	// Read the file and populate the freelist
// 	for {
// 		// Read the diskAddress from the file
// 		var space diskAddress
// 		dec := gob.NewDecoder(file)
// 		if err := dec.Decode(&space); err != nil {
// 			break
// 		}

// 		// Put the diskAddress in the appropriate pool
// 		f.put(space)
// 	}
// }