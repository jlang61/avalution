package merkledb

import (
	"log"
	"os"
	"path/filepath"

	"github.com/ava-labs/avalanchego/utils/perms"
)

// Metadata size constant
const metaSize = 16

type DiskManager interface {
	write([]byte) (diskAddress, error) // malloc()
	putBack(diskAddress) error         // done working, should put a disk address back free()
	get(diskAddress) ([]byte, error)   // read()
	getHeader() ([]byte, error)        // get root node
}

// Type assertion to check that diskMgr is implementing DiskManager
var _ DiskManager = &diskMgr{}

type diskMgr struct {
	file *os.File
	free *freeList
}

// TODO pointer and nil instead of diskMgr{}?
func newDiskManager(metaData []byte, dir string, fileName string) (diskMgr, error) {
	// create file on-disk
	file, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return diskMgr{}, err
	}

	// start freelist
	maxSize := 1024
	f := newFreeList(maxSize)
	f.load()

	// metaData is fixed size of the header
	if len(metaData) != metaSize {
		return diskMgr{}, log.Output(2, "Metadata size is incorrect")
	}

	// Write metadata at the start of the file
	_, err = file.WriteAt(metaData, 0)
	if err != nil {
		log.Fatalf("failed to write metadata: %v", err)
		return diskMgr{}, err
	}

	// if metadata always fixed in length, return error if not fixed

	// create new file, new diskmanager
	// with a certain size in the constructor, this is the size of the metadata
	return diskMgr{file: file, free: f}, err
}

func (dm *diskMgr) getHeader() ([]byte, error) {
	// Read the metadata from the reserved header space
	headerBytes := make([]byte, metaSize)
	_, err := dm.file.ReadAt(headerBytes, 0)
	if err != nil {
		return nil, err
	}
	return headerBytes, nil
}

func (dm *diskMgr) get(addr diskAddress) ([]byte, error) {
	readBytes := make([]byte, addr.size)
	_, err := dm.file.ReadAt(readBytes, addr.offset)
	if err != nil {
		return nil, err
	}
	return readBytes, nil
}

func (dm *diskMgr) putBack(addr diskAddress) error {
	dm.free.put(addr)
	return nil
}

// returning diskaddress that it wrote to
// if we write to freelist: diskaddress would be the size of freespace
// if we dont write to freelist: append bytes to end, return endoffset and size
func (dm *diskMgr) write(bytes []byte) (diskAddress, error) {
	freeSpace, ok := dm.free.get(int64(len(bytes)))

	// Calculate and add padding
	prevSize := len(bytes)
	nextPowerOf2Size := nextPowerOf2(prevSize)
	// Add dummy bytes to reach the next power of 2 size
	paddingSize := nextPowerOf2Size - prevSize
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		bytes = append(bytes, padding...)
	}

	// log.Println("Initial Get: ", freeSpace)
	if !ok {
		// If there is no free space, write at the end of the file
		endOffset, err := dm.endOfFile()
		if err != nil {
			log.Fatalf("failed to get end of file: %v", err)
			return diskAddress{}, err
		}
		_, err = dm.file.WriteAt(bytes, endOffset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
			return diskAddress{}, err
		}
		log.Println("Data written successfully at the end of the file.")
		freeSpace = diskAddress{offset: endOffset, size: int64(prevSize)}
	} else {
		// If there is free space, write at the offset
		_, err := dm.file.WriteAt(bytes, freeSpace.offset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
			return diskAddress{}, err
		}
		log.Println("Data written successfully at free space.")
		freeSpace = diskAddress{offset: freeSpace.offset, size: int64(prevSize)}
	}
	// log.Println("Freespace: ", freeSpace)
	return freeSpace, nil
}

// Helper function for Disk Manager
func (dm *diskMgr) endOfFile() (int64, error) {
	fileInfo, err := dm.file.Stat()
	if err != nil {
		log.Fatalf("failed to get file info: %v", err)
	}
	return fileInfo.Size(), err
}
