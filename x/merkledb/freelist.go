package merkledb

type freelist struct {
	pool chan diskAddress
}

func newFreelist(size int) *freelist {
	return &freelist{
		pool: make(chan diskAddress, size),
	}
}

func (f *freelist) get(size int64) (diskAddress, bool) {
	select {
	case space := <-f.pool:
		if space.size >= size {
			return space, true
		}
		// If the space is not large enough, discard it and return false
		return diskAddress{}, false
	default:
		return diskAddress{}, false
	}
}

func (f *freelist) put(space diskAddress) {
	select {
	case f.pool <- space:
	default:
		// Pool is full, discard the space
	}
}

func (f *freelist) close() {
	// iterate through the pool and output the information to a db file 
	
	close(f.pool)
}
