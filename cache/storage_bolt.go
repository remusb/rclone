package cache

import (
	"time"

	"github.com/pkg/errors"
	"github.com/boltdb/bolt"
	"github.com/ncw/rclone/fs"
	"encoding/json"
	"os"
	"encoding/binary"
	"bytes"
	"strconv"
	"strings"
	"sync"
)

// Constants
const (
	ListBucket = "ls"
	InfoBucket = "info"
	DataBucket = "data"
	StatsBucket = "stats"
	TsBucket = "ts"
)

// Fs represents a wrapped fs.Fs
type Bolt struct {
	Storage

	dbPath 				string
	db   					*bolt.DB
	cleanupMux 		sync.Mutex
}

// NewFs contstructs an Fs from the path, container:path
func NewBolt(dbPath string) *Bolt {
	return &Bolt{
		dbPath: dbPath,
	}
}

func (b *Bolt) Connect(refreshDb bool) error {
	if refreshDb {
		os.Remove(b.dbPath)
	}

	db, err := bolt.Open(b.dbPath, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return errors.Wrapf(err, "failed to open a cache connection to %q", b.dbPath)
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte(ListBucket))
		_, _ = tx.CreateBucketIfNotExists([]byte(InfoBucket))
		_, _ = tx.CreateBucketIfNotExists([]byte(DataBucket))
		_, _ = tx.CreateBucketIfNotExists([]byte(TsBucket))

		return nil
	})

	b.db = db

	return nil
}

func (b *Bolt) Purge() {
	if b.db != nil {
		b.db.Close()
	}

	b.Connect(true)
}

func (b *Bolt) ListGet(dir string, cachedEntries *CachedDirEntries) error {
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(ListBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open bucket (%v)", ListBucket)
		}

		val := bucket.Get([]byte(dir))
		if val != nil {
			err := json.Unmarshal(val, &cachedEntries)

			if err != nil {
				return errors.Errorf("Couldn't unmarshal directory (%v) entries: %v", dir, err)
			}
		}

		return nil
	})

	return err
}

func (b *Bolt) ListPut(dir string, entries *CachedDirEntries) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(ListBucket))
		if err != nil {
			return errors.Errorf("Couldn't open or create bucket (%v): %v", ListBucket, err)
		}

		encoded, err := json.Marshal(entries)
		if err != nil {
			return errors.Errorf("Couldn't marshal directory (%v) entries: %v", dir, err)
		}

		err = bucket.Put([]byte(dir), encoded)
		if err != nil {
			return errors.Errorf("Couldn't store directory (%v) entries : %v", dir, err)
		}

		return nil
	})

	return err
}

func (b *Bolt) ListRemove(dir string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		prefix := []byte(dir)
		bucket, err := tx.CreateBucketIfNotExists([]byte(ListBucket))
		if err != nil {
			return errors.Errorf("Couldn't open or create bucket (%v): %v", ListBucket, err)
		}

		c := bucket.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			c.Delete()
		}

		bucket = tx.Bucket([]byte(InfoBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", InfoBucket)
		}

		c = bucket.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			c.Delete()
		}

		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", DataBucket)
		}

		c = bucket.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			c.Delete()
		}

		return nil
	})

	return err
}

func (b *Bolt) ObjectGet(path string) (cachedObject *CachedObject, err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(InfoBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", InfoBucket)
		}

		val := bucket.Get([]byte(path))
		fs.Errorf("bolt", "Raw cached object (%v): %+v", path, string(val[:]))

		if val != nil {
			return json.Unmarshal(val, &cachedObject)
		}

		return errors.Errorf("Couldn't find object (%v)", path)
	})

	if err != nil {
		return nil, err
	}

	return cachedObject, err
}

func (b *Bolt) ObjectPut(cachedObject *CachedObject) error {
	path := cachedObject.Remote()

	err := b.db.Batch(func(tx *bolt.Tx) error {
		// create object bucket
		bucket, err := tx.CreateBucketIfNotExists([]byte(InfoBucket))
		if err != nil {
			return errors.Errorf("Couldn't create (%v) bucket: %v", InfoBucket, err)
		}

		// cache Object Info
		encoded, err := json.Marshal(cachedObject)
		if err != nil {
			return errors.Errorf("Couldn't marshal object (%v) info: %v", path, err)
		}

		err = bucket.Put([]byte(path), []byte(encoded))
		if err != nil {
			return errors.Errorf("Couldn't cache object (%v) info: %v", path, err)
		}

		return nil
	})

	return err
}

// TODO invalidate cached listing that contains the deleted entry
func (b *Bolt) ObjectRemove(cachedObject *CachedObject) error {
	path := cachedObject.Remote()

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(InfoBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", InfoBucket)
		}

		err := bucket.Delete([]byte(path))
		if err != nil {
			fs.Errorf("bolt", "Couldn't delete file info (%v): %+v", path, err)
		}

		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", DataBucket)
		}

		_ = bucket.DeleteBucket([]byte(path))
		fs.Errorf("bolt", "info: deleted object (%v)", path)

		return nil
	})

	return err
}

func (b *Bolt) objectDataTs(tx *bolt.Tx, path string, offset int64) {
	tsBucket := tx.Bucket([]byte(TsBucket))
	tsVal := path + "-" + strconv.FormatInt(offset, 10)

	// delete previous timestamps for the same object
	c := tsBucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if bytes.Equal(v, []byte(tsVal)) {
			c.Delete()
		}
	}

	ts := itob(time.Now().UnixNano())
	err := tsBucket.Put([]byte(ts), []byte(tsVal))
	if err != nil {
		// TODO: Ignore ts update?
		fs.Errorf("bolt", "Couldn't update ts of cache object (%v): %v", path, err)
	}
}

func (b *Bolt) ObjectDataGet(cachedObject *CachedObject, offset int64) (data []byte, err error) {
	path := cachedObject.Remote()

	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DataBucket)).Bucket([]byte(path))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v.%v) bucket", DataBucket, path)
		}

		val := bucket.Get(itob(offset))
		if val == nil {
			return errors.Errorf("Couldn't get cached object data (%v) at offset %v", path, offset)
		}
		data = cloneBytes(val)

		return nil
	})

	if err != nil {
		return nil, err
	}

	go b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.objectDataTs(tx, path, offset)
		return nil
	})

	return data, err
}

func (b *Bolt) ObjectDataPut(cachedObject *CachedObject, data []byte, offset int64) error {
	path := cachedObject.Remote()

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.Bucket([]byte(DataBucket)).CreateBucketIfNotExists([]byte(path))
		if err != nil || bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket: %+v", path, err)
		}

		err = bucket.Put(itob(offset), data)
		if err != nil {
			return errors.Errorf("Couldn't cache object data (%v) at offset %v", path, offset)
		}

		return nil
	})

	go b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.objectDataTs(tx, path, offset)
		return nil
	})

	return err
}

func (b *Bolt) ObjectDataClean(chunkAge time.Duration) {
	go b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		dataBucket := tx.Bucket([]byte(DataBucket))
		if dataBucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", DataBucket)
		}

		bucket := tx.Bucket([]byte(TsBucket))
		if bucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", TsBucket)
		}

		cnt := 0
		min := itob(0)
		max := itob(time.Now().Truncate(chunkAge).UnixNano())

		// iterate through ts
		c := bucket.Cursor()
		for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
			if v == nil {
				continue
			}

			val := string(v[:])
			sepIdx := strings.LastIndex(val, "-")
			path := val[:sepIdx]
			offset, err := strconv.ParseInt(val[sepIdx+1:], 10, 64)
			if err != nil {
				fs.Errorf("bolt", "couldn't parse ts entry %v", val)
				continue
			}

			objBucket := dataBucket.Bucket([]byte(path))
			if objBucket == nil {
				fs.Errorf("bolt", "Couldn't open (%v.%v) bucket", DataBucket, path)
				continue
			}

			err = objBucket.Delete(itob(offset))
			if err != nil {
				fs.Errorf("bolt", "Couldn't delete chunk during cleanup (%v-%v): %v", path, offset, err)
				continue
			}

			err = c.Delete()
			if err != nil {
				fs.Errorf("bolt", "Couldn't delete chunk ts during cleanup (%v-%v): %v", path, offset, err)
				continue
			}

			cnt = cnt + 1
		}
		fs.Errorf("bolt", "info: deleted (%v) chunks", cnt)

		return nil
	})
}

func (b *Bolt) Stats() {
	go b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		statsBucket, err := tx.CreateBucketIfNotExists([]byte(StatsBucket))
		if err != nil {
			return errors.Errorf("Couldn't open (%v) bucket", StatsBucket)
		}

		dataBucket := tx.Bucket([]byte(DataBucket))
		if dataBucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", DataBucket)
		}
		//dataStats := dataBucket.Stats()
		listBucket := tx.Bucket([]byte(ListBucket))
		if listBucket == nil {
			return errors.Errorf("Couldn't open (%v) bucket", ListBucket)
		}
		listStats := listBucket.Stats()

		totalChunks := 0
		totalFiles := 0
		var totalBytes int64 = 0
		c := dataBucket.Cursor()
		// iterate through objects (buckets)
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				objBucket := dataBucket.Bucket(k)
				objStats := objBucket.Stats()

				totalFiles = totalFiles + 1
				totalChunks = totalChunks + objStats.KeyN
				totalBytes = totalBytes + int64(objStats.LeafInuse)
			}
		}

		stats := &CacheStats{
			TotalDirLists: listStats.KeyN,
			TotalBytes: fs.SizeSuffix(totalBytes).String(),
			TotalChunks: totalChunks,
			TotalFiles: totalFiles,
		}

		encoded, err := json.Marshal(stats)
		if err != nil {
			return errors.Errorf("Couldn't marshal stats (%+v): %v", stats, err)
		}

		ts := time.Now().Format(time.RFC3339)
		err = statsBucket.Put([]byte(ts), encoded)
		if err != nil {
			// TODO: Ignore stats update?
			fs.Errorf("bolt", "Couldn't add db stats (%v): %v", ts, err)
		}

		fs.Errorf("bolt", "info: DB STATS %v:\n%+v", ts, stats)

		return nil
	})
}

// itob returns an 8-byte big endian representation of v.
func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func boit(v []byte) int64 {
	return int64(binary.BigEndian.Uint64(v))
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}