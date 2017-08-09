package cache

import (
	"github.com/patrickmn/go-cache"
	"time"
	"strconv"
	"github.com/pkg/errors"
	"github.com/ncw/rclone/fs"
	"strings"
)

type Memory struct {
	ChunkStorage

	db	*cache.Cache
}

func NewMemory(defaultExpiration time.Duration) *Memory {
	mem := &Memory{}
	err := mem.Connect(defaultExpiration)
	if err != nil {
		fs.Errorf("cache", "can't open ram connection: %v", err)
	}

	return mem
}

func (m *Memory) Connect(defaultExpiration time.Duration) error {
	m.db = cache.New(5*time.Minute, -1)

	return nil
}

func (m *Memory) HasChunk(cachedObject *CachedObject, offset int64) bool {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)

	_, found := m.db.Get(key)

	return found
}

func (m *Memory) GetChunk(cachedObject *CachedObject, offset int64) ([]byte, error) {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)
	var data []byte

	if x, found := m.db.Get(key); found {
		data = x.([]byte)

		return data, nil
	}

	return nil, errors.Errorf("couldn't get cached object data at offset %v", offset)
}

func (m *Memory) AddChunk(cachedObject *CachedObject, data []byte, offset int64) error {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)

	m.db.Set(key, data, cache.DefaultExpiration)

	return nil
}

func (m *Memory) CleanChunksByAge(chunkAge time.Duration) {
	m.db.DeleteExpired()
}

func (m *Memory) CleanChunksByNeed(offset int64) {
	var items map[string]cache.Item

	items = m.db.Items()
	for key, _ := range items {
		sepIdx := strings.LastIndex(key, "-")
		keyOffset, err := strconv.ParseInt(key[sepIdx+1:], 10, 64)
		if err != nil {
			fs.Errorf("cache", "couldn't parse offset entry %v", key)
			continue
		}

		if keyOffset < offset {
			m.db.Delete(key)
		}
	}
}
