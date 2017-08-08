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
	"path"
)

// Constants
const (
	ListBucket = "ls"
	DataBucket = "data"
	StatsBucket = "stats"
	TsBucket = "ts"
)

type Bolt struct {
	Storage

	dbPath 				string
	db   					*bolt.DB
	fs						*Fs
	cleanupMux 		sync.Mutex
}

// NewFs contstructs an Fs from the path, container:path
func NewBolt(dbPath string, fs *Fs) *Bolt {
	return &Bolt{
		dbPath: dbPath,
		fs: fs,
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
		//_, _ = tx.CreateBucketIfNotExists([]byte(ListBucket))
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

func (b *Bolt) GetBucket(dir string, createIfMissing bool, tx *bolt.Tx) *bolt.Bucket {
	if dir == "." { // empty cleaned paths in Go mean .
		dir = ""
	}

	entries := strings.FieldsFunc(dir, func(c rune) bool {
		return os.PathSeparator == c
	})

	fs.Errorf("cache", "Split %v dir in %v", dir, entries)

	var bucket *bolt.Bucket
	if createIfMissing {
		bucket, _ = tx.CreateBucketIfNotExists([]byte(ListBucket))
	} else {
		bucket = tx.Bucket([]byte(ListBucket))
	}
	if bucket == nil {
		return nil
	}

	for _, entry := range entries {
		if createIfMissing {
			bucket, _ = bucket.CreateBucketIfNotExists([]byte(entry))
		} else {
			bucket = bucket.Bucket([]byte(entry))
		}

		if bucket == nil {
			return nil
		}
	}

	return bucket
}

func (b *Bolt) GetDir(dir string) (fs.DirEntries, error) {
	var dirEntries fs.DirEntries

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := b.GetBucket(path.Clean(dir), false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", dir)
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil { // directory
				var d CachedDirectory
				d.CacheFs = b.fs

				dirEntries = append(dirEntries, &d)
			} else { // object
				var o CachedObject
				err := json.Unmarshal(v, &o)
				if err != nil {
					fs.Errorf(b.fs, "error during unmarshalling obj (%v)", dir)
				}
				o.CacheFs = b.fs

				dirEntries = append(dirEntries, &o)
			}
		}

		return nil
	})

	return dirEntries, err
}

func (b *Bolt) AddDir(dir string, entries fs.DirEntries) (fs.DirEntries, error) {
	var cachedEntries fs.DirEntries

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.GetBucket(path.Clean(dir), true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", dir)
		}

		for _, entry := range entries {
			switch o := entry.(type) {
			case fs.Object:
				co := NewCachedObject(b.fs, o)

				encoded, err := json.Marshal(co)
				if err != nil {
					return errors.Errorf("couldn't marshal object (%v): %v", co, err)
				}

				err = bucket.Put([]byte(path.Base(co.Remote())), encoded)
				if err != nil {
					return errors.Errorf("couldn't store object (%v) : %v", co, err)
				}

				cachedEntries = append(cachedEntries, co)
			case fs.Directory:
				cd := NewCachedDirectory(b.fs, o)
				bucket.CreateBucketIfNotExists([]byte(path.Base(cd.Remote())))
				cachedEntries = append(cachedEntries, cd)
			default:
				return errors.Errorf("Unknown object type %T", entry)
			}
		}

		return nil
	})

	return cachedEntries, err
}

func (b *Bolt) RemoveDir(p string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		parent, dirName := path.Split(p)

		bucket := b.GetBucket(parent, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", p)
		}

		bucket.DeleteBucket([]byte(dirName))

		// remove all cached chunks from objects in that dir
		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		c := bucket.Cursor()
		for k, _ := c.Seek([]byte(path.Clean(p))); k != nil && bytes.HasPrefix(k, []byte(path.Clean(p))); k, _ = c.Next() {
			c.Delete()
		}

		return nil
	})

	return err
}

func (b *Bolt) GetObject(p string) (cachedObject *CachedObject, err error) {
	parent, obj := path.Split(p)

	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := b.GetBucket(parent, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", p)
		}

		val := bucket.Get([]byte(obj))
		if val != nil {
			return json.Unmarshal(val, &cachedObject)
		}

		return errors.Errorf("couldn't find object (%v)", p)
	})

	if err != nil {
		return nil, err
	}

	return cachedObject, err
}

func (b *Bolt) AddObject(cachedObject *CachedObject) error {
	parent, obj := path.Split(cachedObject.Remote())

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.GetBucket(parent, true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", parent)
		}

		// cache Object Info
		encoded, err := json.Marshal(cachedObject)
		if err != nil {
			return errors.Errorf("couldn't marshal object (%v) info: %v", cachedObject, err)
		}

		err = bucket.Put([]byte(obj), []byte(encoded))
		if err != nil {
			return errors.Errorf("couldn't cache object (%v) info: %v", cachedObject, err)
		}

		return nil
	})

	return err
}

func (b *Bolt) RemoveObject(cachedObject *CachedObject) error {
	parent, obj := path.Split(cachedObject.Remote())

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.GetBucket(parent, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", parent)
		}

		bucket.Delete([]byte(obj))

		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		bucket.DeleteBucket([]byte(parent))

		return nil
	})

	return err
}

func (b *Bolt) updateChunkTs(tx *bolt.Tx, path string, offset int64) {
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
	tsBucket.Put([]byte(ts), []byte(tsVal))
}

func (b *Bolt) HasChunk(cachedObject *CachedObject, offset int64) bool {
	path := path.Clean(cachedObject.Remote())

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DataBucket)).Bucket([]byte(path))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v.%v) bucket", DataBucket, path)
		}

		val := bucket.Get(itob(offset))
		if val == nil {
			return errors.Errorf("couldn't get cached object data (%v) at offset %v", path, offset)
		}

		return nil
	})

	if err != nil {
		return false
	}

	return true
}

func (b *Bolt) GetChunk(cachedObject *CachedObject, offset int64) ([]byte, error) {
	path := path.Clean(cachedObject.Remote())
	var data []byte

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(DataBucket)).Bucket([]byte(path))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v.%v) bucket", DataBucket, path)
		}

		data = bucket.Get(itob(offset))
		if data == nil {
			return errors.Errorf("couldn't get cached object data (%v) at offset %v", path, offset)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.updateChunkTs(tx, path, offset)
		return nil
	})

	return data, nil
}

func (b *Bolt) AddChunk(cachedObject *CachedObject, data []byte, offset int64) error {
	path := path.Clean(cachedObject.Remote())

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.Bucket([]byte(DataBucket)).CreateBucketIfNotExists([]byte(path))
		if err != nil || bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket: %+v", path, err)
		}

		err = bucket.Put(itob(offset), data)
		if err != nil {
			return errors.Errorf("couldn't cache object data (%v) at offset %v", path, offset)
		}

		return nil
	})

	if err != nil {
		return err
	}

	b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.updateChunkTs(tx, path, offset)
		return nil
	})

	return err
}

func (b *Bolt) CleanChunks(chunkAge time.Duration) {
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
			return errors.Errorf("couldn't open (%v) bucket", StatsBucket)
		}

		dataBucket := tx.Bucket([]byte(DataBucket))
		if dataBucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}
		//dataStats := dataBucket.Stats()
		listBucket := tx.Bucket([]byte(ListBucket))
		if listBucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", ListBucket)
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
			return errors.Errorf("couldn't marshal stats (%+v): %v", stats, err)
		}

		ts := time.Now().Format(time.RFC3339)
		err = statsBucket.Put([]byte(ts), encoded)
		if err != nil {
			// TODO: Ignore stats update?
			fs.Errorf("bolt", "couldn't add db stats (%v): %v", ts, err)
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