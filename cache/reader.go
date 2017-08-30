package cache

import (
	"io"

	"sync"
	"time"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
	"os"
)

// Manager is managing the chunk retrieval from both source and cache for a single object
type Reader struct {
	CachedObject    *Object
	m								*Manager
	offset					struct{ sync.RWMutex; o int64 }
	memory        	ChunkStorage
	preloadQueue		chan int64
	preloadOffset		int64
	//recentChunks		map[int64][]byte
}

func NewReader(o *Object) *Reader {
	r := &Reader {
		CachedObject: o,
		offset: struct{ sync.RWMutex; o int64 }{o: 0},
		preloadQueue: make(chan int64),
		preloadOffset: 0,
	}

	m := NewManager(o, r)
	r.m = m
	r.memory = NewMemory(o.CacheFs.chunkCleanAge)
	//r.recentChunks = make(map[int64][]byte)

	return r
}

// CacheFs is a convenience method to get the parent cache FS of the object's manager
func (r *Reader) CacheFs() *Fs {
	return r.CachedObject.CacheFs
}

// Storage is a convenience method to get the persistent storage of the object's manager
func (r *Reader) Storage() Storage {
	return r.CacheFs().Cache()
}

// Memory is a convenience method to get the transient storage of the object's manager
func (r *Reader) Memory() ChunkStorage {
	return r.memory
}

func (r *Reader) String() string {
	return r.CachedObject.Abs()
}

func (r *Reader) Offset() int64 {
	//r.offset.RLock()
	//defer r.offset.RUnlock()
	return r.offset.o
}

// GetChunk is called by the FS to retrieve a specific chunk of known start and size from where it can find it
// it can be from transient or persistent cache
// it will also build the chunk from the cache's specific chunk boundaries and build the final desired chunk in a buffer
func (r *Reader) GetChunk(chunkStart int64) ([]byte, error) {
	var data []byte
	var err error

	// we reached the end of the file
	if chunkStart >= r.CachedObject.Size() {
		fs.Debugf(r, "reached EOF %v", chunkStart)
		return nil, io.EOF
	}

	// we calculate the modulus of the requested offset with the size of a chunk
	offset := chunkStart % r.CacheFs().chunkSize

	// we align the start offset of the first chunk to a likely chunk in the storage
	// if it's 0, even better, nothing changes
	// if it's >0, we'll read from the start of the chunk and we'll need to trim the offsetMod bytes from it after EOF
	chunkStart = chunkStart - offset

	reqSize := r.CacheFs().chunkSize - offset
	buffer := make([]byte, 0, reqSize)

	if chunkStart != r.preloadOffset {
		r.preloadOffset = chunkStart
		select {
		case r.preloadQueue <- chunkStart:
			fs.Infof(r, "sent %v to manager", chunkStart)
		default:
			fs.Errorf(r, "missed %v for manager", chunkStart)
		}
	}

	// delete old chunks from RAM
	//go r.Memory().CleanChunksByNeed(chunkStart)

	found := false
	data, err = r.Memory().GetChunk(r.CachedObject, chunkStart)
	if err == nil {
		found = true
		//fs.Infof(r, "info: chunk read from ram cache: %v", chunkStart)
	}

	start := time.Now()
	if !found {
		// we're gonna give the workers a chance to pickup the chunk
		// and retry a couple of times
		for i := 0; i < r.m.ReadRetries; i++ {
			data, err = r.Storage().GetChunk(r.CachedObject, chunkStart)

			if err == nil {
				fs.Infof(r, "%v: chunk read from storage cache: %v", chunkStart, chunkStart+int64(len(data)))
				found = true
				break
			}

			fs.Debugf(r, "%v: chunk retry storage: %v", chunkStart, i)
			time.Sleep(time.Second)
		}
	}

	elapsed := time.Since(start)
	if elapsed > time.Second {
		fs.Debugf(r, "%v: chunk search storage: %s", chunkStart, elapsed)
	}

	// not found in ram or
	// the worker didn't managed to download the chunk in time so we abort and close the stream
	if err != nil || len(data) == 0 || !found {
		return nil, errors.Errorf("chunk not found %v", chunkStart)
	}

	// TODO: maybe a more efficient way to do this?
	// first chunk will be aligned with the start
	if offset > 0 {
		fs.Debugf(r, "chunk start align %v->%v", chunkStart, chunkStart+offset)
		data = data[int(offset):]
	}

	//fs.Errorf(r, "buffer: %v; data: %v", cap(buffer), len(data))
	buffer = append(buffer, data...)

	return buffer, nil
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.offset.Lock()
	defer r.offset.Unlock()

	var buf []byte
	currentOffset := r.Offset()

	buf, err = r.GetChunk(currentOffset)
	readSize := copy(p, buf)

	newOffset := currentOffset + int64(readSize)
	fs.Infof(r, "read %v: %v", currentOffset, fs.SizeSuffix(readSize))
	r.offset.o = newOffset
	if r.offset.o >= r.CachedObject.Size() {
		return readSize, io.EOF
	}
	//atomic.CompareAndSwapInt64(&r.offset.o, currentOffset, newOffset)
	return readSize, err
}

func (r *Reader) Close() error {
	r.offset.RLock()
	defer r.offset.RUnlock()
	r.m.Stop()

	fs.Infof(r, "reader closed %v", r.Offset())
	return nil
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var err error

	r.offset.Lock()
	defer r.offset.Unlock()

	fs.Infof(r, "reader seek obj (%v) from %v to %v", r.CachedObject.Size(), r.offset.o, offset)

	switch whence {
	case os.SEEK_SET:
		fs.Debugf(r, "moving offset set from %v to %v", r.offset.o, offset)
		r.offset.o = offset
	case os.SEEK_CUR:
		fs.Debugf(r, "moving offset cur from %v to %v", r.offset.o, r.offset.o + offset)
		r.offset.o += offset
	case os.SEEK_END:
		fs.Debugf(r, "moving offset end (%v) from %v to %v", r.CachedObject.Size(), r.offset.o, r.CachedObject.Size() + offset)
		r.offset.o = r.CachedObject.Size() + offset
	default:
		err = errors.Errorf("cache: unimplemented seek whence %v", whence)
	}

	return r.offset.o, err
}

// Check the interfaces are satisfied
var (
	_ io.ReadCloser             = (*Reader)(nil)
	_ io.Seeker             		= (*Reader)(nil)
)