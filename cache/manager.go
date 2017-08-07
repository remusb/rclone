package cache

import (
	"io"
	"path"

	"github.com/ncw/rclone/fs"
	"time"
	"sync"
	"github.com/pkg/errors"
)

//type SeekInSource func(chunkStart int64, chunkEnd int64) ([]byte, error)

type Manager struct {
	CachedObject		*CachedObject
	FileName				string
	ReadRetries			int
	DownloadRetries	int
	TotalWorkers 		int
	workerGroup			sync.WaitGroup
}

func NewManager(o *CachedObject) *Manager {
	return &Manager{
		CachedObject: o,
		FileName: path.Base(o.Remote()),
		ReadRetries: 5,
		DownloadRetries: 3,
		TotalWorkers: 4,
	}
}

func NewBufferedManager(o *CachedObject, offset int64, end int64) *Manager {
	return &Manager{
		CachedObject: o,
		FileName: path.Base(o.Remote()),
		ReadRetries: 5,
		DownloadRetries: 3,
		TotalWorkers: 4,
	}
}

func (m *Manager) CacheFs() *Fs {
	return m.CachedObject.CacheFs
}

func (m *Manager) Storage() *Bolt {
	return m.CacheFs().Cache()
}

func (m *Manager) Memory() *Memory {
	return m.CacheFs().Memory()
}

func (m *Manager) DownloadWorker(chunkStart int64) {
	var err error
	var reader io.ReadCloser
	chunkEnd := chunkStart + m.CacheFs().chunkSize

	defer m.workerGroup.Done()

	// align the EOF
	if (chunkEnd > m.CachedObject.Size()) {
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
		fs.Errorf(m.FileName, "info: object open failed %v: %v", chunkStart, err)
		return
	}

	data := make([]byte, chunkEnd-chunkStart)
	reader.Read(data)
	reader.Close()

	//m.Memory().ObjectDataPut(m.CachedObject, data, chunkStart)
	m.Storage().ObjectDataPut(m.CachedObject, data, chunkStart)
}

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
		if m.Memory().ObjectDataExists(m.CachedObject, newOffset) || m.Storage().ObjectDataExists(m.CachedObject, newOffset) {
			workers++
			continue
		}

		// fetch from source
		m.workerGroup.Add(1)
		go m.DownloadWorker(newOffset)
		workers++
	}
}

func (m *Manager) GetChunk(chunkStart, chunkEnd int64) ([]byte, error) {
	fs.Errorf(m.FileName, "info: reading chunk %v-%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(chunkEnd))

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

	for {
		// we reached the end of the file
		if chunkStart >= m.CachedObject.Size() {
			fs.Errorf(m.FileName, "info: reached EOF %v", chunkStart)
			break
		}

		found := false
		data, err = m.Memory().ObjectDataGet(m.CachedObject, chunkStart)
		if err == nil {
			found = true
			fs.Errorf(m.FileName, "info: chunk read from ram cache %v-%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(len(data)))
		}

		start := time.Now()
		if !found {
			// we're gonna give the workers a chance to pickup the chunk
			// and retry a couple of times
			for i := 0; i < m.ReadRetries; i++ {
				data, err = m.Storage().ObjectDataGet(m.CachedObject, chunkStart)

				if err == nil {
					// store in RAM in case we need it again
					//go m.Memory().ObjectDataPut(m.CachedObject, data, chunkStart)

					fs.Errorf(m.FileName, "info: chunk read from storage cache %v: %v", chunkStart, fs.SizeSuffix(len(data)))
					found = true
					break
				}

				fs.Errorf(m.FileName, "info: chunk retry storage %v: %v", chunkStart, i)
				time.Sleep(time.Second)
			}
		}

		elapsed := time.Since(start)
		if elapsed > time.Second {
			fs.Errorf(m.FileName, "%v: chunk search storage: %s", chunkStart, elapsed)
		}

		// not found in ram or
		// the worker didn't managed to download the chunk in time so we abort and close the stream
		if err != nil || len(data) == 0 || !found {
			return nil, errors.Errorf("info: chunk not found %v", chunkStart)
		}

		// TODO: maybe a more efficient way to do this?
		// first chunk will be aligned with the start
		if offset > 0 {
			//fs.Errorf(m.FileName, "info: chunk start align %v->%v", fs.SizeSuffix(chunkStart), fs.SizeSuffix(chunkStart+offset))
			data = data[int(offset):]
		}

		// every chunk will be checked for the end
		dataSize := int64(len(data))
		totalBufferSize := int64(len(buffer))
		if totalBufferSize + dataSize > reqSize {
			dataSize = reqSize - totalBufferSize
			//fs.Errorf(m.FileName, "info: chunk end align %v->%v", fs.SizeSuffix(chunkStart+offset+int64(len(data))), fs.SizeSuffix(chunkStart+offset+dataSize))
			data = data[0:dataSize]
		}

		buffer = append(buffer, data...)
		totalBufferSize = int64(len(buffer))

		if totalBufferSize >= reqSize {
			fs.Errorf(m.FileName, "info: chunk wrote to stream %v: %v", chunkStart, fs.SizeSuffix(totalBufferSize).String())
			break
		}

		// we move forward for the next chunk
		chunkStart = chunkStart + m.CacheFs().chunkSize
		offset = 0
	}

	return buffer, nil
}
