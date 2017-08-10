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

// Bolt is a wrapper of persistent storage for a bolt.DB file
type Bolt struct {
	Storage

	dbPath     string
	db         *bolt.DB
	fs         *Fs
	cleanupMux sync.Mutex
}

// NewBolt builds a new wrapper and connects to the bolt.DB file
func NewBolt(dbPath string, refreshDb bool, f *Fs) *Bolt {
	b := &Bolt{
		dbPath: dbPath,
		fs:     f,
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

// getBucket prepares and cleans a specific path of the form: /var/tmp and will iterate through each path component
// to get to the nested bucket of the final part (in this example: tmp)
func (b *Bolt) getBucket(dir string, createIfMissing bool, tx *bolt.Tx) *bolt.Bucket {
	if dir == "." || dir == "/" { // empty cleaned paths in Go mean . but we want to refer to the root ListBucket
		dir = ""
	}

	entries := strings.FieldsFunc(dir, func(c rune) bool {
		return os.PathSeparator == c
	})

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

// GetDir will return a CachedDirectory, its list of dir entries and/or an error if it encountered issues
func (b *Bolt) GetDir(dir string) (*Directory, fs.DirEntries, error) {
	var dirEntries fs.DirEntries
	var cachedDir *Directory

	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(path.Clean(dir), false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", dir)
		}

		val := bucket.Get([]byte("."))
		err := json.Unmarshal(val, &cachedDir)
		if err != nil {
			fs.Debugf(b.fs, "error during unmarshalling obj (%v)", dir)
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// ignore metadata key: .
			if bytes.Equal(k, []byte(".")) {
				continue
			}

			if v == nil { // directory
				var d Directory
				meta := c.Bucket().Bucket(k).Get([]byte("."))
				err := json.Unmarshal(meta, &d)
				if err != nil {
					fs.Debugf(b.fs, "error during unmarshalling obj (%v)", dir)
				}
				d.CacheFs = b.fs

				dirEntries = append(dirEntries, &d)
			} else { // object
				var o Object
				err := json.Unmarshal(v, &o)
				if err != nil {
					fs.Debugf(b.fs, "error during unmarshalling obj (%v)", dir)
				}
				o.CacheFs = b.fs

				dirEntries = append(dirEntries, &o)
			}
		}

		return nil
	})

	return cachedDir, dirEntries, err
}

// AddDir will update a CachedDirectory metadata and all its entries
func (b *Bolt) AddDir(cachedDir *Directory, entries fs.DirEntries) (fs.DirEntries, error) {
	var cachedEntries fs.DirEntries

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(path.Clean(cachedDir.Remote()), true, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Remote())
		}

		encoded, err := json.Marshal(cachedDir)
		if err != nil {
			return errors.Errorf("couldn't marshal object (%v): %v", cachedDir, err)
		}
		_ = bucket.Put([]byte("."), encoded)

		for _, entry := range entries {
			switch o := entry.(type) {
			case fs.Object:
				co := NewObject(b.fs, o)

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
				cd := NewDirectory(b.fs, o)

				_, err := bucket.CreateBucketIfNotExists([]byte(path.Base(cd.Remote())))
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
	if cachedDir.Remote() == "" {
		return errors.Errorf("can't delete root %v", cachedDir.Remote())
	}

	err := b.db.Update(func(tx *bolt.Tx) error {
		parent, dirName := path.Split(cachedDir.Remote())

		bucket := b.getBucket(parent, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open bucket (%v)", cachedDir.Remote())
		}

		err := bucket.DeleteBucket([]byte(dirName))
		if err != nil {
			fs.Debugf(dirName, "couldn't delete dir from cache: %v", err)
		}

		// remove all cached chunks from objects in that dir
		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		c := bucket.Cursor()
		for k, _ := c.Seek([]byte(path.Clean(cachedDir.Remote()))); k != nil && bytes.HasPrefix(k, []byte(path.Clean(cachedDir.Remote()))); k, _ = c.Next() {
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
func (b *Bolt) GetObject(p string) (cachedObject *Object, err error) {
	parent, obj := path.Split(p)

	err = b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(parent, false, tx)
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

// AddObject will create a cached object in its parent directory
func (b *Bolt) AddObject(cachedObject *Object) error {
	parent, obj := path.Split(cachedObject.Remote())

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(parent, true, tx)
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

// RemoveObject will delete a single cached object and all the chunks which belong to it
func (b *Bolt) RemoveObject(cachedObject *Object) error {
	parent, obj := path.Split(cachedObject.Remote())

	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(parent, false, tx)
		if bucket == nil {
			return errors.Errorf("couldn't open parent bucket for %v", parent)
		}

		err := bucket.Delete([]byte(obj))
		if err != nil {
			fs.Debugf(cachedObject, "couldn't delete dir from storage: %v", err)
		}

		bucket = tx.Bucket([]byte(DataBucket))
		if bucket == nil {
			return errors.Errorf("couldn't open (%v) bucket", DataBucket)
		}

		err = bucket.DeleteBucket([]byte(parent))
		if err != nil {
			fs.Debugf(cachedObject, "couldn't delete dir from storage: %v", err)
		}

		return nil
	})

	return err
}

// HasChunk confirms the existence of a single chunk of an object
func (b *Bolt) HasChunk(cachedObject *Object, offset int64) bool {
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

// GetChunk will retrieve a single chunk which belongs to a cached object or an error if it doesn't find it
func (b *Bolt) GetChunk(cachedObject *Object, offset int64) ([]byte, error) {
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

	fs.Debugf("cache", "cleanup failed: %v", err)
}

// CleanChunksByNeed is a noop for this implementation
// TODO: Add one
func (b *Bolt) CleanChunksByNeed(offset int64) {
	// noop: we want to clean a Bolt DB by time only
}

// Purge will clear all the data in the storage
func (b *Bolt) Purge() {
	if b.db != nil {
		err := b.db.Close()
		if err != nil {
			fs.Debugf("cache", "couldn't close connection to storage: %v", err)
		}
	}

	err := b.Connect(true)
	if err != nil {
		fs.Debugf("cache", "couldn't open connection to storage: %v", err)
	}
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
			fs.Errorf("bolt", "couldn't add db stats (%v): %v", ts, err)
		}

		fs.Errorf("bolt", "info: DB STATS %v:\n%+v", ts, stats)

		return nil
	})

	if err != nil {
		fs.Debugf("cache", "couldn't generate cache stats: %v", err)
	}
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
