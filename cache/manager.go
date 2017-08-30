package cache

import (
	"github.com/ncw/rclone/fs"
	"path"
	"sync"
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
			//m:	m,
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
