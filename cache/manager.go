package cache

import (
	"io"
	"time"
	"github.com/ncw/rclone/fs"
	"path"
	"sync"
	"os"
	"fmt"
)

// Manager is managing the chunk retrieval from both source and cache for a single object
type Manager struct {
	CachedObject    *Object
	CachedReader		*Reader
	ReadRetries     int
	DownloadRetries int
	TotalWorkers    int

	offsetQueue 		chan int64
	workers					[]*worker
	offset					int64
	running					struct{ sync.Mutex; r bool }
}

// NewManager returns a simple manager
func NewManager(o *Object, r *Reader) *Manager {
	m := &Manager{
		CachedObject:    o,
		CachedReader:		 r,
		ReadRetries:     5,
		DownloadRetries: 3,
		TotalWorkers:    8,
		running:				 struct{ sync.Mutex; r bool }{r: true},
		offset:					 0,
	}

	m.offsetQueue = make(chan int64, m.TotalWorkers)
	for i := 0; i < m.TotalWorkers; i++ {
		w := &worker{
			m:	m,
			ch: m.offsetQueue,
			id: i+1,
		}
		go w.run()

		m.workers = append(m.workers, w)
	}
	go m.Start()

	return m
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
	return m.CachedReader.Memory()
}

// Memory is a convenience method to get the transient storage of the object's manager
func (m *Manager) String() string {
	return path.Base(m.CachedObject.Abs())
}

func (m *Manager) syncWithReader() {
	readerOffset := <-m.CachedReader.preloadQueue
	m.offset = readerOffset
	//readerOffset := m.CachedReader.Offset()
	//readerOffset = readerOffset - (readerOffset % m.CacheFs().chunkSize)
	//limitOffset := m.CacheFs().chunkSize
	//
	//if readerOffset >= limitOffset && (m.offset <= readerOffset - limitOffset || m.offset > readerOffset + limitOffset) {
	//	fs.Debugf(m, "preloader aligned %v -> %v", m.offset, readerOffset - m.CacheFs().chunkSize)
	//	m.offset = readerOffset - m.CacheFs().chunkSize
	//}
}

func (m *Manager) run(offset int64) bool {
	//offset = offset - (offset % m.CacheFs().chunkSize)

	// we reached the end of the file
	if offset < 0 || offset >= m.CachedObject.Size() {
		//fs.Debugf(m, "preloader reached EOF %v", offset)
		return false
	}

	// search the storage queue
	if !m.Memory().HasChunk(m.CachedObject, offset) {
		fs.Debugf(m, "preloader needs %v", offset)
		m.offsetQueue <- offset
	}

	//m.offset += m.CacheFs().chunkSize
	return true
}

// StartWorkers will start TotalWorkers which will download chunks and store them in cache
func (m *Manager) Start() {
	//fs.Infof(m, "starting %v workers", m.TotalWorkers)

	for i := 0; i < m.TotalWorkers; i++ {
		m.run(m.offset + m.CacheFs().chunkSize*int64(i))
	}

	for {
		m.syncWithReader()
		if !m.Running() {
			break
		}

		for i := 0; i < m.TotalWorkers; i++ {
			m.run(m.offset + m.CacheFs().chunkSize*int64(i-1))
		}
	}

	m.Stop()
}

func (m *Manager) Running() bool {
	m.running.Lock()
	defer m.running.Unlock()
	return m.running.r
}

func (m *Manager) Stop() {
	if !m.Running() {
		return
	}

	//debug.PrintStack()
	fs.Infof(m, "stopping workers")

	m.running.Lock()
	m.running.r = false
	m.running.Unlock()

	for i := 0; i < m.TotalWorkers; i++ {
		m.offsetQueue <- -1
	}
}

type worker struct {
	m		*Manager
	ch <-chan int64
	rc	io.ReadCloser
	id	int
}

func (w *worker) String() string {
	return fmt.Sprintf("worker-%v <%v>", w.id, w.m.CachedObject.Name)
}

func (w *worker) Reader(offset int64) (io.ReadCloser, error) {
	var err error

	r := w.rc
	if w.rc == nil {
		r, err = w.m.CachedObject.Object.Open(&fs.SeekOption{Offset: offset})
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
		r, err = w.m.CachedObject.Object.Open(&fs.SeekOption{Offset: offset})
		return r, err
	}

	return r, nil
}

// DownloadWorker is a single routine that downloads a single chunk and stores in both transient and persistent storage
func (w *worker) run() {
	var err error
	var data []byte
	//seeker := false
	w.rc, err = w.m.CachedObject.Object.Open()
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

		data, err = w.m.Storage().GetChunk(w.m.CachedObject, chunkStart)
		if err == nil {
			err = w.m.Memory().AddChunk(w.m.CachedObject, data, chunkStart)
			if err != nil {
				fs.Errorf(w, "failed caching chunk in ram %v: %v", chunkStart, err)
			} else {
				continue
			}
		}
		err = nil

		chunkEnd := chunkStart+w.m.CacheFs().chunkSize
		if chunkEnd > chunkStart+w.m.CachedObject.Size() {
			chunkEnd = w.m.CachedObject.Size()
		}
		for retry := 0; retry < w.m.DownloadRetries; retry++ {
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

		data = make([]byte, chunkEnd - chunkStart)

		sourceRead := 0
		sourceRead, err = io.ReadFull(w.rc, data)
		if err != nil && err != io.EOF  {
			fs.Errorf(w, "failed to read chunk %v: %v", chunkStart, err)
		}
		data = data[:sourceRead] // reslice to remove extra garbage

		err = w.m.Memory().AddChunk(w.m.CachedObject, data, chunkStart)
		if err != nil {
			fs.Errorf(w, "failed caching chunk in ram %v: %v", chunkStart, err)
		}

		err = w.m.Storage().AddChunk(w.m.CachedObject, data, chunkStart)
		if err != nil {
			fs.Errorf(w, "failed caching chunk in storage %v: %v", chunkStart, err)
		}
	}
}
