package cache

import (
	"io"
	"path"

	"sync"
	"time"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

// Manager is managing the chunk retrieval from both source and cache for a single object
type Manager struct {
	CachedObject    *Object
	FileName        string
	ReadRetries     int
	DownloadRetries int
	TotalWorkers    int
	workerGroup     sync.WaitGroup
}

// NewManager returns a simple manager
func NewManager(o *Object) *Manager {
	return &Manager{
		CachedObject:    o,
		FileName:        path.Base(o.Remote()),
		ReadRetries:     5,
		DownloadRetries: 3,
		TotalWorkers:    4,
	}
}

// CacheFs is a convenience method to get the parent cache FS of the object's manager
func (m *Manager) CacheFs() *Fs {
	return m.CachedObject.CacheFs
}

// Storage is a convenience method to get the persistent storage of the object's manager
func (m *Manager) Storage() Storage {
	return m.CacheFs().Cache()
}

// Memory is a convenience method to get the transient storage of the object's manager
func (m *Manager) Memory() ChunkStorage {
	return m.CacheFs().Memory()
}

// DownloadWorker is a single routine that downloads a single chunk and stores in both transient and persistent storage
func (m *Manager) DownloadWorker(chunkStart int64) {
	var err error
	var reader io.ReadCloser
	chunkEnd := chunkStart + m.CacheFs().chunkSize

	defer m.workerGroup.Done()

	// align the EOF
	if chunkEnd > m.CachedObject.Size() {
		chunkEnd = m.CachedObject.Size()
	}

	for retry := 0; retry < m.DownloadRetries; retry++ {
		reader, err = m.CachedObject.Open(&fs.SeekOption{Offset: chunkStart}, &fs.RangeOption{Start: chunkStart, End: chunkEnd})

		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		break
	}

	// we seem to be getting only errors so we abort
	if err != nil {
		fs.Errorf(m.FileName, "object open failed %v: %v", chunkStart, err)
		return
	}

	data := make([]byte, chunkEnd-chunkStart)
	_, err = reader.Read(data)
	if err != nil {
		fs.Errorf(m.FileName, "failed to read chunk %v: %v", chunkStart, err)
	}
	err = reader.Close()
	if err != nil {
		fs.Errorf(m.FileName, "failed to close reader for %v: %v", chunkStart, err)
	}

	err = m.Memory().AddChunk(m.CachedObject, data, chunkStart)
	if err != nil {
		fs.Errorf(m.FileName, "failed caching chunk in ram %v: %v", chunkStart, err)
	}
	err = m.Storage().AddChunk(m.CachedObject, data, chunkStart)
	if err != nil {
		fs.Errorf(m.FileName, "failed caching chunk in storage %v: %v", chunkStart, err)
	}
}

// StartWorkers will start TotalWorkers which will download chunks and store them in cache
func (m *Manager) StartWorkers(chunkStart int64) {
	m.workerGroup.Wait()

	workers := 0
	offset := chunkStart % m.CacheFs().chunkSize
	chunkStart = chunkStart - offset

	for {
		if workers >= m.TotalWorkers {
			break
		}

		newOffset := chunkStart + (m.CacheFs().chunkSize * int64(workers))

		// we reached the end of the file
		if newOffset >= m.CachedObject.Size() {
			break
		}

		// search the storage queue
		if m.Memory().HasChunk(m.CachedObject, newOffset) || m.Storage().HasChunk(m.CachedObject, newOffset) {
			workers++
			continue
		}

		// fetch from source
		m.workerGroup.Add(1)
		go m.DownloadWorker(newOffset)
		workers++
	}
}

// GetChunk is called by the FS to retrieve a specific chunk of known start and size from where it can find it
// it can be from transient or persistent cache
// it will also build the chunk from the cache's specific chunk boundaries and build the final desired chunk in a buffer
func (m *Manager) GetChunk(chunkStart, chunkEnd int64) ([]byte, error) {
	fs.Infof(m.FileName, "reading chunk %v-%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(chunkEnd))

	reqSize := chunkEnd - chunkStart
	buffer := make([]byte, 0, reqSize)
	var data []byte
	var err error

	// we calculate the modulus of the requested offset with the size of a chunk
	offset := chunkStart % m.CacheFs().chunkSize

	// we align the start offset of the first chunk to a likely chunk in the storage
	// if it's 0, even better, nothing changes
	// if it's >0, we'll read from the start of the chunk and we'll need to trim the offsetMod bytes from it after EOF
	chunkStart = chunkStart - offset

	// delete old chunks from RAM
	go m.Memory().CleanChunksByNeed(chunkStart)

	for {
		// we reached the end of the file
		if chunkStart >= m.CachedObject.Size() {
			fs.Debugf(m.FileName, "reached EOF %v", chunkStart)
			break
		}

		found := false
		data, err = m.Memory().GetChunk(m.CachedObject, chunkStart)
		if err == nil {
			found = true
			fs.Infof(m.FileName, "chunk read from ram cache %v-%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(len(data)))
		}

		start := time.Now()
		if !found {
			// we're gonna give the workers a chance to pickup the chunk
			// and retry a couple of times
			for i := 0; i < m.ReadRetries; i++ {
				data, err = m.Storage().GetChunk(m.CachedObject, chunkStart)

				if err == nil {
					fs.Infof(m.FileName, "%v: chunk read from storage cache: %v", chunkStart, fs.SizeSuffix(len(data)))
					found = true
					break
				}

				fs.Debugf(m.FileName, "%v: chunk retry storage: %v", chunkStart, i)
				time.Sleep(time.Second)
			}
		}

		elapsed := time.Since(start)
		if elapsed > time.Second {
			fs.Debugf(m.FileName, "%v: chunk search storage: %s", chunkStart, elapsed)
		}

		// not found in ram or
		// the worker didn't managed to download the chunk in time so we abort and close the stream
		if err != nil || len(data) == 0 || !found {
			return nil, errors.Errorf("chunk not found %v", chunkStart)
		}

		// TODO: maybe a more efficient way to do this?
		// first chunk will be aligned with the start
		if offset > 0 {
			fs.Debugf(m.FileName, "chunk start align %v->%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(chunkStart+offset))
			data = data[int(offset):]
		}

		// every chunk will be checked for the end
		dataSize := int64(len(data))
		totalBufferSize := int64(len(buffer))
		if totalBufferSize+dataSize > reqSize {
			dataSize = reqSize - totalBufferSize
			fs.Debugf(m.FileName, "chunk end align %v->%v", fs.SizeSuffix(chunkStart+offset+int64(len(data))), fs.SizeSuffix(chunkStart+offset+dataSize))
			data = data[0:dataSize]
		}

		buffer = append(buffer, data...)
		totalBufferSize = int64(len(buffer))

		if totalBufferSize >= reqSize {
			fs.Infof(m.FileName, "chunk wrote to stream %v: %v", chunkStart, fs.SizeSuffix(totalBufferSize).String())
			break
		}

		// we move forward for the next chunk
		chunkStart = chunkStart + m.CacheFs().chunkSize
		offset = 0
	}

	return buffer, nil
}
