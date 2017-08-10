package cache

import (
	"fmt"
	"io"
	"path"
	"strings"

	"time"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

const (
	// DefCacheChunkSize is the default value for chunk size
	DefCacheChunkSize = "5M"
	// DefCacheListAge is the default value for directory listings age
	DefCacheListAge = "5m"
	// DefCacheFileAge is the default value for object info age
	DefCacheFileAge = "5m"
	// DefCacheChunkCleanAge is the default value for chunk age duration
	DefCacheChunkCleanAge = "2m"
)

// Globals
var (
	// Flags
	cacheDbPath        = fs.StringP("cache-db-path", "", path.Join(path.Dir(fs.ConfigPath), "cache.db"), "Path to cache DB")
	cacheDbPurge       = fs.BoolP("cache-db-purge", "", false, "Purge the cache DB before")
	cacheChunkSize     = fs.SizeSuffix(-1)
	cacheListAge       = fs.StringP("cache-dir-list-age", "", "", "How much time should directory listings be stored in cache")
	cacheFileAge       = fs.StringP("cache-file-info-age", "", "", "How much time should object info be stored in cache")
	cacheChunkCleanAge = fs.StringP("cache-chunk-clean-age", "", "", "How much time should a chunk be in cache before cleanup")
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cache",
		Description: "Cache a remote",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "remote",
			Help: "Remote to cache.\nNormally should contain a ':' and a path, eg \"myremote:path/to/dir\",\n\"myremote:bucket\" or maybe \"myremote:\" (not recommended).",
		}, {
			Name: "chunk_size",
			Help: "The size of a chunk.\nExamples: 1024 (or 1024b), 10M, 1G.\nDefault: 50M",
			Examples: []fs.OptionExample{
				{
					Value: "1024",
					Help:  "1024 bytes",
				}, {
					Value: "1024b",
					Help:  "1024 bytes",
				}, {
					Value: "10M",
					Help:  "10 Megabytes",
				}, {
					Value: "1G",
					Help:  "1 Gigabyte",
				},
			},
			Optional: true,
		}, {
			Name: "list_age",
			Help: "How much time should directory listings be stored in cache.\nAccepted units are: \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\nDefault: 1m (1 minute)",
			Examples: []fs.OptionExample{
				{
					Value: "30s",
					Help:  "30 seconds",
				}, {
					Value: "1m",
					Help:  "1 minute",
				}, {
					Value: "1h30m",
					Help:  "1 hour and 30 minutes",
				},
			},
			Optional: true,
		}, {
			Name: "file_age",
			Help: "How much time should object info be stored in cache.\nAccepted units are: \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\nDefault: 1m (1 minute)",
			Examples: []fs.OptionExample{
				{
					Value: "30s",
					Help:  "30 seconds",
				}, {
					Value: "1m",
					Help:  "1 minute",
				}, {
					Value: "1h30m",
					Help:  "1 hour and 30 minutes",
				},
			},
			Optional: true,
		}, {
			Name: "chunk_clean_age",
			Help: "How old should a chunk be before cleanup.\nAccepted units are: \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".\nDefault: 2h (2 hours)",
			Examples: []fs.OptionExample{
				{
					Value: "30s",
					Help:  "30 seconds",
				}, {
					Value: "1m",
					Help:  "1 minute",
				}, {
					Value: "1h30m",
					Help:  "1 hour and 30 minutes",
				},
			},
			Optional: true,
		}},
	})

	fs.VarP(&cacheChunkSize, "cache-chunk-size", "", "The size of a chunk. Examples: 1024 (or 1024b), 10M, 1G. Default: 10M")
}

// ChunkStorage is a storage type that supports only chunk operations (i.e in RAM)
type ChunkStorage interface {
	// will check if the chunk is in storage. should be fast and not read the chunk itself if possible
	HasChunk(cachedObject *Object, offset int64) bool

	// returns the chunk in storage. return an error if it's not
	GetChunk(cachedObject *Object, offset int64) ([]byte, error)

	// add a new chunk
	AddChunk(cachedObject *Object, data []byte, offset int64) error

	// if the storage can cleanup on a cron basis
	// otherwise it can do a noop operation
	CleanChunksByAge(chunkAge time.Duration)

	// if the storage can cleanup chunks after we no longer need them
	// otherwise it can do a noop operation
	CleanChunksByNeed(offset int64)

	// this will clear all the data in the storage
	Purge()
}

// Storage is a storage type (Bolt) which needs to support both chunk and file based operations
type Storage interface {
	ChunkStorage

	// will return a directory with all the entries in it or an error if it's not found
	GetDir(dir string) (*Directory, fs.DirEntries, error)

	// adds a new dir with the provided entries
	// if we need an empty directory then an empty array should be provided
	// the directory structure (all the parents of this dir) is created if its not found
	AddDir(cachedDir *Directory, entries fs.DirEntries) (fs.DirEntries, error)

	// remove a directory and all the objects and chunks in it
	RemoveDir(cachedDir *Directory) error

	// will return an object (file) or error if it doesn't find it
	GetObject(p string) (cachedObject *Object, err error)

	// add a new object to its parent directory
	// the directory structure (all the parents of this object) is created if its not found
	AddObject(cachedObject *Object) error

	// remove an object and all its chunks
	RemoveObject(cachedObject *Object) error

	// if the storage supports statistics, generate and do something with them
	Stats()
}

// Stats are stats to generate for storage usage
type Stats struct {
	TotalDirLists int
	TotalBytes    string
	TotalChunks   int
	TotalFiles    int
}

// Fs represents a wrapped fs.Fs
type Fs struct {
	fs.Fs

	name     string
	root     string
	features *fs.Features // optional features
	cache    Storage
	memory   ChunkStorage

	listAge       time.Duration
	fileAge       time.Duration
	chunkSize     int64
	chunkCleanAge time.Duration

	lastCleanup time.Time
}

// NewFs contstructs an Fs from the path, container:path
func NewFs(name, rpath string) (fs.Fs, error) {
	remote := fs.ConfigFileGet(name, "remote")
	if strings.HasPrefix(remote, name+":") {
		return nil, errors.New("can't point cache remote at itself - check the value of the remote setting")
	}

	// Look for a file first
	remotePath := path.Join(remote, rpath)
	wrappedFs, wrapErr := fs.NewFs(remotePath)

	if wrapErr != fs.ErrorIsFile && wrapErr != nil {
		return nil, errors.Wrapf(wrapErr, "failed to make remote %q to wrap", remotePath)
	}

	var chunkSize fs.SizeSuffix
	chunkSizeString := fs.ConfigFileGet(name, "chunk_size", DefCacheChunkSize)
	if cacheChunkSize.String() != "off" {
		chunkSizeString = cacheChunkSize.String()
	}
	err := chunkSize.Set(chunkSizeString)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to understand chunk size", chunkSizeString)
	}

	listAge := fs.ConfigFileGet(name, "list_age", DefCacheListAge)
	listDuration, err := time.ParseDuration(listAge)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to understand duration", listAge)
	}
	if len(*cacheListAge) > 0 {
		listDuration, err = time.ParseDuration(*cacheListAge)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to understand duration", *cacheListAge)
		}
	}

	fileAge := fs.ConfigFileGet(name, "file_info_age", DefCacheFileAge)
	fileDuration, err := time.ParseDuration(fileAge)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to understand duration", fileAge)
	}
	if len(*cacheFileAge) > 0 {
		fileDuration, err = time.ParseDuration(*cacheFileAge)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to understand duration", *cacheFileAge)
		}
	}

	chunkCleanAge := fs.ConfigFileGet(name, "chunk_clean_age", DefCacheChunkCleanAge)
	chunkCleanDuration, err := time.ParseDuration(chunkCleanAge)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to understand duration", chunkCleanAge)
	}
	if len(*cacheChunkCleanAge) > 0 {
		chunkCleanDuration, err = time.ParseDuration(*cacheChunkCleanAge)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to understand duration", *cacheChunkCleanAge)
		}
	}

	// configure cache backend
	if *cacheDbPurge {
		fs.Errorf("cache", "Info: Purging the DB")
	}

	fs.Errorf("cache", "info: listAge: %v", listDuration.String())
	fs.Errorf("cache", "info: fileAge: %v", fileDuration.String())
	fs.Errorf("cache", "info: chunkSize: %v", chunkSize.String())
	fs.Errorf("cache", "info: chunkCleanMinAge: %v", chunkCleanDuration.String())

	m := NewMemory(chunkCleanDuration)

	f := &Fs{
		Fs:            wrappedFs,
		name:          name,
		root:          rpath,
		memory:        m,
		listAge:       listDuration,
		fileAge:       fileDuration,
		chunkSize:     int64(chunkSize),
		chunkCleanAge: chunkCleanDuration,
		lastCleanup:   time.Now(),
	}

	f.cache = NewBolt(*cacheDbPath, *cacheDbPurge, f)
	if err != nil {
		return nil, err
	}

	f.features = (&fs.Features{
		ReadMimeType:    false, // MimeTypes not supported with crypt
		WriteMimeType:   false,
		Purge:           f.Purge,
		Copy:            f.Copy,
		Move:            f.Move,
		DirMove:         f.DirMove,
		DirChangeNotify: nil, // TODO: Add this
		DirCacheFlush:   nil,
		PutUnchecked:    f.PutUnchecked,
		CleanUp:         f.CleanUp,
		FileReadRaw:     true,
	}).Fill(f)

	return f, wrapErr
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("Cached drive '%s:%s'", f.name, f.root)
}

// Cache is the persistent type cache
func (f *Fs) Cache() Storage {
	return f.cache
}

// Memory is the transient type cache
func (f *Fs) Memory() ChunkStorage {
	return f.memory
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	cachedObject, err := f.cache.GetObject(remote)

	if err != nil {
		fs.Errorf(f, "info: object not found [%v]: %v", remote, err)
	} else {
		expiresAt := cachedObject.CacheTs.Add(f.fileAge)

		if time.Now().After(expiresAt) {
			fs.Errorf(cachedObject, "info: object expired")
		} else {
			fs.Errorf(cachedObject, "info: object found")
			return cachedObject, nil
		}
	}

	// Get live object from source or fail
	liveObject, err := f.Fs.NewObject(remote)
	if err != nil || liveObject == nil {
		fs.Errorf(f, "couldn't get object (%v) from source fs (%v): %v", remote, f.Fs.Name(), err)
		return nil, err
	}
	fs.Errorf(liveObject, "info: object (%v) from source fs (%v)", remote, f.Fs.Name())

	// We used the source so let's cache the result for later usage
	cachedObject = NewObject(f, liveObject)
	err = f.cache.AddObject(cachedObject)
	if err != nil {
		fs.Infof(remote, "could not be cached: %v", err)
	}

	return cachedObject, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	var cachedDir *Directory

	// Get raw cached entries from cache
	cachedDir, entries, err = f.cache.GetDir(dir)

	if err != nil {
		cachedDir = NewDirectoryEmpty(f, dir)
		fs.Errorf(dir, "info: couldn't get dir entries from cache [%v]: %v", dir, err)
	} else if len(entries) == 0 {
		cachedDir = NewDirectoryEmpty(f, dir)
		// TODO: read empty dirs from source?
	} else {
		//fs.Errorf(dir, "info: found dir entries [%v] %v", dir, len(entries))
		return entries, nil
	}

	// Get live entries from source or fail
	entries, err = f.Fs.List(dir)
	if err != nil || entries == nil {
		fs.Errorf(f, "couldn't list directory (%v) from source fs (%v): %v", dir, f.Fs.Name(), err)
		return nil, err
	}

	// We used the source so let's cache the result for later usage
	cachedEntries, err := f.cache.AddDir(cachedDir, entries)
	if err != nil {
		// TODO return original list when fails?
		fs.Errorf(cachedEntries, "couldn't cache contents of directory [%v]: %v", dir, err)
		return entries, nil
	}

	return cachedEntries, nil
}

func (f *Fs) recurse(dir string, list *fs.ListRHelper) error {
	entries, err := f.List(dir)
	if err != nil {
		return err
	}

	for i := 0; i < len(entries); i++ {
		innerDir, ok := entries[i].(fs.Directory)
		if ok {
			err := f.recurse(innerDir.Remote(), list)
			if err != nil {
				return err
			}
		}

		err := list.Add(entries[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	// we check if the source FS supports ListR
	// if it does, we'll use that to get all the entries, cache them and return
	do := f.Fs.Features().ListR
	if do != nil {
		return f.Fs.Features().ListR(dir, func(entries fs.DirEntries) error {
			// we got called back with a set of entries so let's cache them and call the original callback
			var emptyDirEntries fs.DirEntries
			var err error

			for _, entry := range entries {
				switch o := entry.(type) {
				case fs.Object:
					err = f.cache.AddObject(NewObject(f, o))
				case fs.Directory:
					_, err = f.cache.AddDir(NewDirectory(f, o), emptyDirEntries)
				default:
					return errors.Errorf("Unknown object type %T", entry)
				}

				if err != nil {
					fs.Infof(entry, "could not be cached: %v", err)
				}
			}

			// call the original callback
			return callback(entries)
		})
	}

	// if we're here, we're gonna do a standard recursive traversal and cache everything
	list := fs.NewListRHelper(callback)
	err = f.recurse(dir, list)
	if err != nil {
		return err
	}

	return list.Flush()
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// upload original object
	liveObj, err := f.Fs.Put(in, src, options...)
	if err != nil {
		return nil, err
	}

	// build a cached object and inject it in the cached listing of its parent
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "info: couldn't store in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() fs.HashSet {
	return f.Fs.Hashes()
}

// Mkdir makes the directory (container, bucket)
func (f *Fs) Mkdir(dir string) error {
	err := f.Fs.Mkdir(dir)
	if err != nil {
		return err
	}

	if dir == "" { // creating the root is possible but we don't need that cached as we have it already
		return nil
	}

	// make an empty dir
	var emptyDirEntries fs.DirEntries
	_, err = f.cache.AddDir(NewDirectoryEmpty(f, dir), emptyDirEntries)
	if err != nil {
		fs.Errorf(dir, "couldn't cache empty directory: %v", err)
	}

	return nil
}

// Rmdir removes the directory (container, bucket) if empty
func (f *Fs) Rmdir(dir string) error {
	err := f.Fs.Rmdir(dir)
	if err != nil {
		return err
	}

	err = f.cache.RemoveDir(NewDirectoryEmpty(f, dir))
	if err != nil {
		fs.Errorf("cache", "failed to remove cached dir (%v): %v", dir, err)
	}

	return nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server side move operations.
func (f *Fs) DirMove(src fs.Fs, srcRemote, dstRemote string) error {
	do := f.Fs.Features().DirMove
	if do == nil {
		return fs.ErrorCantDirMove
	}

	err := do(src, srcRemote, dstRemote)
	if err != nil {
		return err
	}

	// clear any likely dir cached at dst
	dstCachedDir := NewDirectoryEmpty(f, dstRemote)
	err = f.cache.RemoveDir(dstCachedDir)
	if err != nil {
		fs.Infof(dstRemote, "failed to remove cached dir: %v", err)
	}

	// get a list of entries of the new dir to cache
	entries, err := f.List(dstCachedDir.Remote())
	if err != nil {
		fs.Infof(dstRemote, "failed to list new dir: %v", err)
	}
	_, err = f.cache.AddDir(dstCachedDir, entries)
	if err != nil {
		fs.Infof(dstRemote, "failed to cache new dir: %v", err)
	}

	return nil
}

// Purge all files in the root and the root directory
func (f *Fs) Purge() error {
	do := f.Fs.Features().Purge
	if do == nil {
		return errors.New("can't Purge")
	}

	err := do()
	if err != nil {
		return err
	}

	f.Cache().Purge()
	return nil
}

// Copy src to this remote using server side copy operations.
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Copy
	if do == nil {
		return nil, fs.ErrorCantCopy
	}

	liveObj, err := do(src, remote)
	if err != nil {
		return liveObj, err
	}

	// store in cache
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "info: couldn't store in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// Move src to this remote using server side move operations.
func (f *Fs) Move(src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Move
	if do == nil {
		return nil, fs.ErrorCantMove
	}

	liveObj, err := do(src, remote)
	if err != nil {
		return liveObj, err
	}

	// remove data from src
	srcCachedObj := NewObject(f, src)
	err = f.cache.RemoveObject(srcCachedObj)
	if err != nil {
		fs.Infof(srcCachedObj, "couldn't remove from cache: %v", err)
	}

	// add new one in cache
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "info: couldn't store in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// PutUnchecked uploads the object
func (f *Fs) PutUnchecked(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	do := f.Fs.Features().PutUnchecked
	if do == nil {
		return nil, errors.New("can't PutUnchecked")
	}

	liveObj, err := do(in, src, options...)
	if err != nil {
		return nil, err
	}

	// store in cache
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "info: couldn't store in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// CleanUp the trash in the Fs
func (f *Fs) CleanUp() error {
	f.cleanUpCache()

	do := f.Fs.Features().CleanUp
	if do == nil {
		return errors.New("can't CleanUp")
	}
	f.lastCleanup = time.Now()

	return do()
}

func (f *Fs) cleanUpCache() {
	if time.Now().Before(f.lastCleanup.Add(f.chunkCleanAge)) {
		return
	}

	fs.Errorf("cache", "info: starting cache cleanup")
	f.cache.CleanChunksByAge(f.chunkCleanAge)

	fs.Errorf("cache", "info: starting stats")
	f.cache.Stats()

	fs.Errorf("cache", "info: starting memory cleanup")
	f.memory.CleanChunksByAge(f.chunkCleanAge)

	f.lastCleanup = time.Now()
	return
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.Fs
}

// Check the interfaces are satisfied
var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.Purger         = (*Fs)(nil)
	_ fs.Copier         = (*Fs)(nil)
	_ fs.Mover          = (*Fs)(nil)
	_ fs.DirMover       = (*Fs)(nil)
	_ fs.PutUncheckeder = (*Fs)(nil)
	_ fs.CleanUpper     = (*Fs)(nil)
	_ fs.UnWrapper      = (*Fs)(nil)
	_ fs.ListRer        = (*Fs)(nil)
)
