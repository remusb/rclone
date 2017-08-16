package cache

import (
	"time"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

// Constants
const (
	ListBucket  = "ls"
	DataBucket  = "data"
	StatsBucket = "stats"
	TsBucket    = "ts"
)

var boltMap = make(map[string]*Bolt)
var boltMapMx sync.RWMutex

// GetBolt returns a single instance for the specific store
func GetBolt(dbPath string, refreshDb bool) *Bolt {
	// read lock to check if it exists
	boltMapMx.RLock()
	if b, ok := boltMap[dbPath]; ok {
		boltMapMx.RUnlock()
		return b
	}
	boltMapMx.RUnlock()

	// write lock to create one but let's check a 2nd time
	boltMapMx.Lock()
	defer boltMapMx.Unlock()
	if b, ok := boltMap[dbPath]; ok {
		return b
	}

	boltMap[dbPath] = newBolt(dbPath, refreshDb)
	return boltMap[dbPath]
}

// Bolt is a wrapper of persistent storage for a bolt.DB file
type Bolt struct {
	Storage

	dbPath     string
	db         *bolt.DB
	cleanupMux sync.Mutex
}

// NewBolt builds a new wrapper and connects to the bolt.DB file
func newBolt(dbPath string, refreshDb bool) *Bolt {
	b := &Bolt{
		dbPath: dbPath,
	}

	err := b.Connect(refreshDb)
	if err != nil {
		fs.Errorf(dbPath, "error opening storage cache: %v", err)
	}

	return b
}

// String will return a human friendly string for this DB (currently the dbPath)
func (b *Bolt) String() string {
	return "Bolt DB: " + b.dbPath
}

// Connect creates a connection to the configured file
// refreshDb will delete the file before to create an empty DB if it's set to true
func (b *Bolt) Connect(refreshDb bool) error {
	if refreshDb {
		err := os.Remove(b.dbPath)
		if err != nil {
			fs.Errorf(b, "failed to remove cache file: %v", err)
		}
	}

	db, err := bolt.Open(b.dbPath, 0644, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return errors.Wrapf(err, "failed to open a cache connection to %q", b.dbPath)
	}

	_ = db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte(ListBucket))
		_, _ = tx.CreateBucketIfNotExists([]byte(DataBucket))
		_, _ = tx.CreateBucketIfNotExists([]byte(TsBucket))

		return nil
	})

	b.db = db

	return nil
}

// getBucket prepares and cleans a specific path of the form: /var/tmp and will iterate through each path component
// to get to the nested bucket of the final part (in this example: tmp)
func (b *Bolt) getBucket(dir string, createIfMissing bool, tx *bolt.Tx) *bolt.Bucket {
	b.cleanPath(dir)

	entries := strings.FieldsFunc(dir, func(c rune) bool {
		return os.PathSeparator == c
	})

	bucket := tx.Bucket([]byte(ListBucket))

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

// updateChunkTs is a convenience method to update a chunk timestamp to mark it was used recently
func (b *Bolt) updateChunkTs(tx *bolt.Tx, path string, offset int64) {
	tsBucket := tx.Bucket([]byte(TsBucket))
	tsVal := path + "-" + strconv.FormatInt(offset, 10)

	// delete previous timestamps for the same object
	c := tsBucket.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if bytes.Equal(v, []byte(tsVal)) {
			err := c.Delete()
			if err != nil {
				fs.Debugf(path, "failed to clean chunk: %v", err)
			}
		}
	}

	ts := itob(time.Now().UnixNano())
	err := tsBucket.Put([]byte(ts), []byte(tsVal))
	if err != nil {
		fs.Debugf(path, "failed to timestamp chunk: %v", err)
	}
}

// GetDir will update a CachedDirectory
func (b *Bolt) GetDir(cachedDir *Directory) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedDir.Abs(), false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Abs())
		}

		val := bucket.Get([]byte("."))
		if val != nil {
			return json.Unmarshal(val, cachedDir)
		}

		return errors.Errorf("couldn't find dir in cache: %v", cachedDir.Abs())
	})
}

// AddDir will update a CachedDirectory metadata and all its entries
func (b *Bolt) AddDir(cachedDir *Directory) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedDir.Abs(), true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir)
		}

		encoded, err := json.Marshal(cachedDir)
		if err != nil {
			return errors.Errorf("couldn't marshal object (%v): %v", cachedDir, err)
		}
		return bucket.Put([]byte("."), encoded)
	})
}

// GetDirEntries will return a CachedDirectory, its list of dir entries and/or an error if it encountered issues
func (b *Bolt) GetDirEntries(cachedDir *Directory) (fs.DirEntries, error) {
	var dirEntries fs.DirEntries

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedDir.Abs(), false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Abs())
		}

		val := bucket.Get([]byte("."))
		if val != nil {
			err := json.Unmarshal(val, cachedDir)
			if err != nil {
				fs.Debugf(cachedDir.Abs(), "error during unmarshalling obj: %v", err)
			}
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// ignore metadata key: .
			if bytes.Equal(k, []byte(".")) {
				continue
			}
			entryPath := path.Join(cachedDir.Remote(), string(k))

			if v == nil { // directory
				// we try to find a cached meta for the dir
				currentBucket := c.Bucket().Bucket(k)
				if currentBucket == nil {
					return errors.Errorf("couldn't open bucket (%v)", string(k))
				}

				metaKey := currentBucket.Get([]byte("."))
				d := NewDirectoryEmpty(cachedDir.CacheFs, entryPath)
				if metaKey != nil { //if we don't find it, we create an empty dir
					err := json.Unmarshal(metaKey, d)
					if err != nil { // if even this fails, we fallback to an empty dir
						fs.Debugf(string(k), "error during unmarshalling obj: %v", err)
					}
				}

				dirEntries = append(dirEntries, d)
			} else { // object
				o := NewObjectEmpty(cachedDir.CacheFs, entryPath)
				err := json.Unmarshal(v, o)
				if err != nil {
					fs.Debugf(string(k), "error during unmarshalling obj: %v", err)
					continue
				}

				dirEntries = append(dirEntries, o)
			}
		}

		return nil
	})

	return dirEntries, err
}

// AddDirEntries will update all the entries for a single Directory
func (b *Bolt) AddDirEntries(cachedDir *Directory, entries fs.DirEntries) (fs.DirEntries, error) {
	var cachedEntries fs.DirEntries

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedDir.Abs(), true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Abs())
		}

		encoded, err := json.Marshal(cachedDir)
		if err != nil {
			return errors.Errorf("couldn't marshal object (%v): %v", cachedDir, err)
		}
		_ = bucket.Put([]byte("."), encoded)

		for _, entry := range entries {
			switch o := entry.(type) {
			case fs.Object:
				co := NewObject(cachedDir.CacheFs, o)

				encoded, err := json.Marshal(co)
				if err != nil {
					return errors.Errorf("couldn't marshal object (%v): %v", co, err)
				}

				err = bucket.Put([]byte(path.Base(co.Abs())), encoded)
				if err != nil {
					return errors.Errorf("couldn't store object (%v) : %v", co, err)
				}

				cachedEntries = append(cachedEntries, co)
			case fs.Directory:
				cd := NewDirectory(cachedDir.CacheFs, o)

				_, err := bucket.CreateBucketIfNotExists([]byte(path.Base(cd.Abs())))
				if err != nil {
					fs.Debugf(cd, "couldn't store dir: %v", err)
				}

				cachedEntries = append(cachedEntries, cd)
			default:
				return errors.Errorf("Unknown object type %T", entry)
			}
		}

		return nil
	})

	return cachedEntries, err
}

// RemoveDir will delete a CachedDirectory, all its objects and all the chunks stored for it
func (b *Bolt) RemoveDir(cachedDir *Directory) error {
	if cachedDir.Abs() == "" {
		return errors.Errorf("can't delete root %v", cachedDir.Abs())
	}

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedDir.Dir, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Abs())
		}

		err := bucket.DeleteBucket([]byte(cachedDir.Name))
		if err != nil {
			fs.Debugf(cachedDir, "couldn't delete from cache: %v", err)
		}

		// remove all cached chunks from objects in that dir
		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		c := bucket.Cursor()
		for k, _ := c.Seek([]byte(cachedDir.Abs())); k != nil && bytes.HasPrefix(k, []byte(cachedDir.Abs())); k, _ = c.Next() {
			err := c.Delete()
			if err != nil {
				fs.Debugf(cachedDir, "couldn't remove dir from cache: %v", err)
			}
		}

		return nil
	})

	return err
}

// GetObject will return a CachedObject from its parent directory or an error if it doesn't find it
func (b *Bolt) GetObject(cachedObject *Object) (err error) {
	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedObject.Dir, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", cachedObject.Dir)
		}

		val := bucket.Get([]byte(cachedObject.Name))
		if val != nil {
			return json.Unmarshal(val, cachedObject)
		}

		return errors.Errorf("couldn't find object (%v)", cachedObject.Name)
	})

	return err
}

// AddObject will create a cached object in its parent directory
func (b *Bolt) AddObject(cachedObject *Object) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedObject.Dir, true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", cachedObject)
		}

		// cache Object Info
		encoded, err := json.Marshal(cachedObject)
		if err != nil {
			return errors.Errorf("couldn't marshal object (%v) info: %v", cachedObject, err)
		}

		err = bucket.Put([]byte(cachedObject.Name), []byte(encoded))
		if err != nil {
			return errors.Errorf("couldn't cache object (%v) info: %v", cachedObject, err)
		}

		return nil
	})

	return err
}

// RemoveObject will delete a single cached object and all the chunks which belong to it
func (b *Bolt) RemoveObject(cachedObject *Object) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(cachedObject.Dir, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", cachedObject.Dir)
		}

		err := bucket.Delete([]byte(cachedObject.Name))
		if err != nil {
			fs.Debugf(cachedObject, "couldn't delete obj from storage: %v", err)
		}

		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		// safe to ignore as the file might not have been open
		_ = bucket.DeleteBucket([]byte(cachedObject.Abs()))

		return nil
	})
}

// HasChunk confirms the existence of a single chunk of an object
func (b *Bolt) HasChunk(cachedObject *Object, offset int64) bool {
	path := cachedObject.Abs()

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

// GetChunk will retrieve a single chunk which belongs to a cached object or an error if it doesn't find it
func (b *Bolt) GetChunk(cachedObject *Object, offset int64) ([]byte, error) {
	path := cachedObject.Abs()
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

	err = b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.updateChunkTs(tx, path, offset)
		return nil
	})

	return data, err
}

// AddChunk adds a new chunk of a cached object
func (b *Bolt) AddChunk(cachedObject *Object, data []byte, offset int64) error {
	path := cachedObject.Abs()

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

	err = b.db.Batch(func(tx *bolt.Tx) error {
		b.cleanupMux.Lock()
		defer b.cleanupMux.Unlock()

		// save touch ts for the newly cached piece
		b.updateChunkTs(tx, path, offset)
		return nil
	})

	return err
}

// CleanChunksByAge will cleanup on a cron basis
func (b *Bolt) CleanChunksByAge(chunkAge time.Duration) {
	err := b.db.Batch(func(tx *bolt.Tx) error {
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
				fs.Errorf("cache", "couldn't parse ts entry %v", val)
				continue
			}

			objBucket := dataBucket.Bucket([]byte(path))
			if objBucket == nil {
				fs.Errorf("cache", "Couldn't open (%v.%v) bucket", DataBucket, path)
				continue
			}

			err = objBucket.Delete(itob(offset))
			if err != nil {
				fs.Errorf("cache", "failed deleting chunk during cleanup (%v-%v): %v", path, offset, err)
				continue
			}

			err = c.Delete()
			if err != nil {
				fs.Errorf("cache", "failed deleting chunk ts during cleanup (%v-%v): %v", path, offset, err)
				continue
			}

			cnt = cnt + 1
		}
		fs.Debugf("cache", "deleted (%v) chunks", cnt)

		return nil
	})

	if err != nil {
		fs.Errorf("cache", "cleanup failed: %v", err)
	}
}

// CleanChunksByNeed is a noop for this implementation
// TODO: Add one
func (b *Bolt) CleanChunksByNeed(offset int64) {
	// noop: we want to clean a Bolt DB by time only
}

// Stats will generate stats for a specific point in time of the storage
func (b *Bolt) Stats() {
	err := b.db.Batch(func(tx *bolt.Tx) error {
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
		var totalBytes int64
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

		stats := &Stats{
			TotalDirLists: listStats.KeyN,
			TotalBytes:    fs.SizeSuffix(totalBytes).String(),
			TotalChunks:   totalChunks,
			TotalFiles:    totalFiles,
		}

		encoded, err := json.Marshal(stats)
		if err != nil {
			return errors.Errorf("couldn't marshal stats (%+v): %v", stats, err)
		}

		ts := time.Now().Format(time.RFC3339)
		err = statsBucket.Put([]byte(ts), encoded)
		if err != nil {
			// TODO: Ignore stats update?
			fs.Errorf("cache", "couldn't add db stats (%v): %v", ts, err)
		}

		fs.Debugf("cache", "DB STATS %v:\n%+v", ts, stats)

		return nil
	})

	if err != nil {
		fs.Debugf("cache", "couldn't generate cache stats: %v", err)
	}
}

func (b *Bolt) cleanPath(p string) string {
	p = path.Clean(p)
	if p == "." || p == "/" {
		p = ""
	}

	return p
}

// itob returns an 8-byte big endian representation of v.
func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
