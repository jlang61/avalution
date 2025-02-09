package merkledb

import (
	"log"
	"os"
	"path/filepath"

	"github.com/ava-labs/avalanchego/utils/perms"
)

// Metadata size constant
const metaSize = 32

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
// creates disk manager with metadata size, if exists, should fetch metadata?
// this is fixed size of header, create new file, metadata takes this amount of space
// if already exist but metadata not fetched, crash, otherwise load into memory
// if metadata ever not correct size, return error
func newDiskManager(metaData []byte, dir string, fileName string) (*diskMgr, error) {
	if metaData == nil {
		metaData = make([]byte, metaSize) // Initialize to 16 bytes of zeros
	}

	// create file on-disk
	file, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return nil, err
	}

	// if metadata always fixed in length, return error if not fixed
	if len(metaData) != metaSize {
		log.Fatalf("invalid metadata size; expected %d bytes, got %d bytes; error: %v", metaSize, len(metaData), err)
		return nil, err
	}

	// Check if the file already exists and has data
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return &diskMgr{}, err
	}

	if fileInfo.Size() > 0 {
		// The file already exists; attempt to read the existing metadata
		existingMeta := make([]byte, metaSize)
		_, err := file.ReadAt(existingMeta, 1)
		if err != nil {
			file.Close()
			log.Fatalf("failed to read metadata %v", err)
			return nil, err
		}

		if len(existingMeta) != metaSize {
			file.Close()
			log.Fatalf("invalid metadata size; expected %d bytes, got %d bytes; error: %v", metaSize, len(existingMeta), err)
			return nil, err
		}

		// If metadata is found and is correct, we assume it is loaded successfully.
		// log.Printf("Existing metadata loaded successfully.")
	} else {
		// The file is new; write the provided metadata
		_, err := file.WriteAt(metaData, 1)
		if err != nil {
			file.Close()
			log.Fatalf("failed to write metadata %v", err)
			return nil, err
		}
		// log.Printf("Metadata written successfully to new file.")
	}

	// start freelist
	maxSize := 4096 * 16 * 16
	f := newFreeList(maxSize)
	f.load(dir)

	// create new file, new diskmanager
	// with a certain size in the constructor, this is the size of the metadata
	return &diskMgr{file: file, free: f}, err
}

func (dm *diskMgr) getHeader() ([]byte, error) {
	// Read the metadata from the reserved header space
	headerBytes := make([]byte, metaSize)
	_, err := dm.file.ReadAt(headerBytes, 1)
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

func (dm *diskMgr) writeRoot(rootNode dbNode) (diskAddress, error) {
	// first check the size of rootNode without the disk address
	bytes := encodeDBNode_disk(&rootNode)
	freeSpace, ok := dm.free.get(int64(len(bytes)) + 16)
	// Calculate and add padding
	prevSize := len(bytes)
	nextPowerOf2Size := nextPowerOf2(prevSize)
	// Add dummy bytes to reach the next power of 2 size
	paddingSize := nextPowerOf2Size - prevSize
	if paddingSize > 0 {
		padding := make([]byte, paddingSize)
		bytes = append(bytes, padding...)
	}
	rootNodeSize := len(bytes) + 16
	if !ok {
		// If there is no free space, write at the end of the file
		endOffset, err := dm.endOfFile()
		if err != nil {
			log.Fatalf("failed to get end of file: %v", err)
			return diskAddress{}, err
		}
		// We know the offset for the root node and the size
		rootDiskAddr := diskAddress{offset: endOffset, size: int64(rootNodeSize)}
		// Attach the diskAddress to the rootnode
		rootNode.diskAddr = rootDiskAddr
		// Encode the root node with the disk address
		bytes = encodeDBNode_disk(&rootNode)
		// Write the root node to the end of the file
		_, err = dm.file.WriteAt(bytes, endOffset)
		if err != nil {
			log.Fatalf("failed to get end of file: %v", err)
			return diskAddress{}, err
		}
		_, err = dm.file.WriteAt(bytes, endOffset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
			return diskAddress{}, err
		}
		freeSpace = diskAddress{offset: endOffset, size: int64(prevSize)}
	} else {
		// If there is free space, we need to write at the offset
		rootDiskAddr := diskAddress{offset: freeSpace.offset, size: int64(rootNodeSize)}
		rootNode.diskAddr = rootDiskAddr
		bytes = encodeDBNode_disk(&rootNode)
		_, err := dm.file.WriteAt(bytes, freeSpace.offset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
			return diskAddress{}, err
		}
		freeSpace = diskAddress{offset: freeSpace.offset, size: int64(prevSize)}
	}
	// log.Println("Freespace: ", freeSpace)
	return freeSpace, nil

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
		// log.Println("Data written successfully at the end of the file.")
		freeSpace = diskAddress{offset: endOffset, size: int64(prevSize)}
	} else {
		// If there is free space, write at the offset
		_, err := dm.file.WriteAt(bytes, freeSpace.offset)
		if err != nil {
			log.Fatalf("failed to write data: %v", err)
			return diskAddress{}, err
		}
		// log.Println("Data written successfully at free space.")
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

// Helper function to calculate the next power of 2 for a given size
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
