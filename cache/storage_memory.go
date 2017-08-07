package cache

import (
	"github.com/patrickmn/go-cache"
	"time"
	"strconv"
	"github.com/pkg/errors"
)

type Memory struct {
	Storage

	db	*cache.Cache
}

func NewMemory() *Memory {
	mem := &Memory{}
	mem.Connect()

	return mem
}

func (m *Memory) Connect() error {
	m.db = cache.New(5*time.Minute, 10*time.Minute)

	return nil
}

func (m *Memory) ObjectDataExists(cachedObject *CachedObject, offset int64) bool {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)

	_, found := m.db.Get(key)

	return found
}

func (m *Memory) ObjectDataGet(cachedObject *CachedObject, offset int64) ([]byte, error) {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)
	var data []byte

	if x, found := m.db.Get(key); found {
		data = x.([]byte)

		return data, nil
	}

	return nil, errors.Errorf("couldn't get cached object data at offset %v", offset)
}

func (m *Memory) ObjectDataPut(cachedObject *CachedObject, data []byte, offset int64) error {
	key := cachedObject.Remote() + "-" + strconv.FormatInt(offset, 10)

	m.db.Set(key, data, cache.DefaultExpiration)

	return nil
}
