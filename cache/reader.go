package cache

import (
	"io"

	"sync"
	"time"

	"fmt"
	"os"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

// Reader is managing the read operations on an open handle
type Reader struct {
	CachedObject  *Object
	memory        ChunkStorage
	preloadQueue  chan int64
	preloadOffset int64
	offset        int64
	mu            sync.RWMutex

	ReadRetries     int
	DownloadRetries int
	TotalWorkers    int
	UseMemory       bool
	workers         []*worker
}

// NewReader returns a new Reader
func NewReader(o *Object) *Reader {
	r := &Reader{
		CachedObject:  o,
		offset:        0,
		preloadOffset: -1, // -1 to trigger the first preload

		ReadRetries:     o.CacheFs.readRetries,
		DownloadRetries: o.CacheFs.downloadRetries,
		TotalWorkers:    o.CacheFs.totalWorkers,
		UseMemory:       o.CacheFs.chunkMemory,
	}

	if r.UseMemory {
		r.memory = NewMemory(o.CacheFs.chunkAge)
	}

	// create a larger buffer to queue up requests
	r.preloadQueue = make(chan int64, r.TotalWorkers*10)
	for i := 0; i < r.TotalWorkers; i++ {
		w := &worker{
			r:  r,
			ch: r.preloadQueue,
			id: i + 1,
		}
		go w.run()

		r.workers = append(r.workers, w)
	}
	r.QueueOffset(0)

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

// String representation of this reader
func (r *Reader) String() string {
	return r.CachedObject.Abs()
}

// QueueOffset will send an offset to the workers if it's different from the last one
func (r *Reader) QueueOffset(offset int64) {
	if offset != r.preloadOffset {
		r.preloadOffset = offset

		for i := 0; i < r.TotalWorkers; i++ {
			o := r.preloadOffset + r.CacheFs().chunkSize*int64(i)
			if o < 0 || o >= r.CachedObject.Size() {
				continue
			}

			r.preloadQueue <- o
		}
	}
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
	r.QueueOffset(chunkStart)
	found := false

	// delete old chunks from memory
	if r.UseMemory {
		go r.Memory().CleanChunksByNeed(chunkStart)

		data, err = r.Memory().GetChunk(r.CachedObject, chunkStart)
		if err == nil {
			found = true
		}
	}

	if !found {
		// we're gonna give the workers a chance to pickup the chunk
		// and retry a couple of times
		for i := 0; i < r.ReadRetries; i++ {
			data, err = r.Storage().GetChunk(r.CachedObject, chunkStart)

			if err == nil {
				found = true
				break
			}

			fs.Debugf(r, "%v: chunk retry storage: %v", chunkStart, i)
			time.Sleep(time.Second)
		}
	}

	// not found in ram or
	// the worker didn't managed to download the chunk in time so we abort and close the stream
	if err != nil || len(data) == 0 || !found {
		return nil, errors.Errorf("chunk not found %v", chunkStart)
	}

	// first chunk will be aligned with the start
	if offset > 0 {
		data = data[int(offset):]
	}

	return data, nil
}

// Read a chunk from storage or len(p)
func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var buf []byte
	currentOffset := r.offset

	buf, err = r.GetChunk(currentOffset)
	readSize := copy(p, buf)

	newOffset := currentOffset + int64(readSize)
	fs.Infof(r, "read %v: %v", currentOffset, fs.SizeSuffix(readSize))

	r.offset = newOffset
	if r.offset >= r.CachedObject.Size() {
		return readSize, io.EOF
	}

	return readSize, err
}

// Close will tell the workers to stop
func (r *Reader) Close() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for i := 0; i < r.TotalWorkers; i++ {
		r.preloadQueue <- -1
	}

	fs.Infof(r, "reader closed %v", r.offset)
	return nil
}

// Seek will move the current offset based on whence and instruct the workers to move there too
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var err error

	r.mu.Lock()
	defer r.mu.Unlock()

	fs.Infof(r, "reader seek obj (%v) from %v to %v", r.CachedObject.Size(), r.offset, offset)

	switch whence {
	case os.SEEK_SET:
		fs.Debugf(r, "moving offset set from %v to %v", r.offset, offset)
		r.offset = offset
	case os.SEEK_CUR:
		fs.Debugf(r, "moving offset cur from %v to %v", r.offset, r.offset+offset)
		r.offset += offset
	case os.SEEK_END:
		fs.Debugf(r, "moving offset end (%v) from %v to %v", r.CachedObject.Size(), r.offset, r.CachedObject.Size()+offset)
		r.offset = r.CachedObject.Size() + offset
	default:
		err = errors.Errorf("cache: unimplemented seek whence %v", whence)
	}

	chunkStart := r.offset - (r.offset % r.CacheFs().chunkSize)
	if chunkStart >= r.CacheFs().chunkSize {
		chunkStart = chunkStart - r.CacheFs().chunkSize
	}
	r.QueueOffset(chunkStart)

	return r.offset, err
}

type worker struct {
	r  *Reader
	ch <-chan int64
	rc io.ReadCloser
	id int
}

// String is a representation of this worker
func (w *worker) String() string {
	return fmt.Sprintf("worker-%v <%v>", w.id, w.r.CachedObject.Name)
}

// Reader will return a reader depending on the capabilities of the source reader:
//   - if it supports seeking it will seek to the desired offset and return the same reader
//   - if it doesn't support seeking it will close a possible existing one and open at the desired offset
//   - if there's no reader associated with this worker, it will create one
func (w *worker) Reader(offset int64) (io.ReadCloser, error) {
	var err error

	r := w.rc
	if w.rc == nil {
		r, err = w.r.CachedObject.Object.Open(&fs.SeekOption{Offset: offset})
	}
	if err != nil {
		return nil, err
	}

	seekerObj, ok := r.(io.Seeker)
	if ok {
		_, err := seekerObj.Seek(offset, os.SEEK_SET)
		return r, err
	}
	if w.rc != nil {
		_ = w.rc.Close()
		r, err = w.r.CachedObject.Object.Open(&fs.SeekOption{Offset: offset})
		return r, err
	}

	return r, nil
}

// run is the main loop for the worker which receives offsets to preload
func (w *worker) run() {
	var err error
	var data []byte
	w.rc, err = w.r.CachedObject.Object.Open()
	if err != nil {
		fs.Errorf(w, "worker error: %v", err)
		w.rc = nil
	}

	fs.Infof(w, "worker started. Reader Open: %v", w.rc != nil)

	for {
		chunkStart := <-w.ch
		if chunkStart < 0 {
			if w.rc != nil {
				_ = w.rc.Close()
			}
			break
		}

		// skip if it exists
		if w.r.UseMemory {
			if w.r.Memory().HasChunk(w.r.CachedObject, chunkStart) {
				continue
			}

			// add it in ram if it's in the persistent storage
			data, err = w.r.Storage().GetChunk(w.r.CachedObject, chunkStart)
			if err == nil {
				err = w.r.Memory().AddChunk(w.r.CachedObject, data, chunkStart)
				if err != nil {
					fs.Errorf(w, "failed caching chunk in ram %v: %v", chunkStart, err)
				} else {
					continue
				}
			}
			err = nil
		} else {
			if w.r.Storage().HasChunk(w.r.CachedObject, chunkStart) {
				continue
			}
		}

		chunkEnd := chunkStart + w.r.CacheFs().chunkSize
		if chunkEnd > chunkStart+w.r.CachedObject.Size() {
			chunkEnd = w.r.CachedObject.Size()
		}
		for retry := 0; retry < w.r.DownloadRetries; retry++ {
			w.rc, err = w.Reader(chunkStart)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			break
		}

		// we seem to be getting only errors so we abort
		if err != nil {
			fs.Errorf(w, "object open failed %v: %v", chunkStart, err)
			continue
		}

		data = make([]byte, chunkEnd-chunkStart)

		sourceRead := 0
		sourceRead, err = io.ReadFull(w.rc, data)
		if err != nil && err != io.EOF {
			fs.Errorf(w, "failed to read chunk %v: %v", chunkStart, err)
		}
		data = data[:sourceRead] // reslice to remove extra garbage

		if w.r.UseMemory {
			err = w.r.Memory().AddChunk(w.r.CachedObject, data, chunkStart)
			if err != nil {
				fs.Errorf(w, "failed caching chunk in ram %v: %v", chunkStart, err)
			}
		}

		err = w.r.Storage().AddChunk(w.r.CachedObject, data, chunkStart)
		if err != nil {
			fs.Errorf(w, "failed caching chunk in storage %v: %v", chunkStart, err)
		}
	}
}

// Check the interfaces are satisfied
var (
	_ io.ReadCloser = (*Reader)(nil)
	_ io.Seeker     = (*Reader)(nil)
)
