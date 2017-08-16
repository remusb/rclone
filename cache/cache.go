package cache

import (
	"fmt"
	"io"
	"path"
	"strings"

	"time"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
	"github.com/patrickmn/go-cache"
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
	cacheDbPath        = fs.StringP("cache-db-path", "", path.Dir(fs.ConfigPath), "Directory to cache DB")
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
	//Purge()
}

// Storage is a storage type (Bolt) which needs to support both chunk and file based operations
type Storage interface {
	ChunkStorage

	// will return a directory or an error if it's not found
	GetDir(cachedDir *Directory) error

	// will update/create a directory or an error if it's not found
	AddDir(cachedDir *Directory) error

	// will return a directory with all the entries in it or an error if it's not found
	GetDirEntries(cachedDir *Directory) (fs.DirEntries, error)

	// adds a new dir with the provided entries
	// if we need an empty directory then an empty array should be provided
	// the directory structure (all the parents of this dir) is created if its not found
	AddDirEntries(cachedDir *Directory, entries fs.DirEntries) (fs.DirEntries, error)

	// remove a directory and all the objects and chunks in it
	RemoveDir(cachedDir *Directory) error

	// clear the cache
	FlushDir()

	// will return an object (file) or error if it doesn't find it
	GetObject(cachedObject *Object) (err error)

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
	store    *cache.Cache
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
	fs.Debugf(name, "wrapped %v:%v at root %v", wrappedFs.Name(), wrappedFs.Root(), rpath)

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
		fs.Debugf(name, "Purging the DB")
	}

	//fs.Infof(name, "listAge: %v", listDuration.String())
	//fs.Infof(name, "fileAge: %v", fileDuration.String())
	//fs.Infof(name, "chunkSize: %v", chunkSize.String())
	//fs.Infof(name, "chunkCleanMinAge: %v", chunkCleanDuration.String())

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

	f.store = cache.New(fileDuration, time.Minute)

	dbPath := *cacheDbPath
	if path.Ext(dbPath) != "" {
		dbPath = path.Dir(dbPath)
	}

	dbPath = path.Join(dbPath, name+".db")
	fs.Infof(name, "Using Bolt DB: %v", dbPath)
	f.cache = GetBolt(dbPath, *cacheDbPurge)
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
		UnWrap:          f.UnWrap,
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
	return fmt.Sprintf("%s:%s", f.name, f.root)
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
	fs.Debugf(f, "find object at '%s'", f.cleanPath(remote))

	cachedObject := NewObjectEmpty(f, f.cleanPath(remote))
	err := f.cache.GetObject(cachedObject)

	if err == nil {
		return cachedObject, nil
	}

	// Get live object from source or fail
	liveObject, err := f.Fs.NewObject(remote)
	if err != nil || liveObject == nil {
		return nil, err
	}

	// We used the source so let's cache the result for later usage
	cachedObject = NewObject(f, liveObject)
	err = f.cache.AddObject(cachedObject)
	if err != nil {
		fs.Errorf(remote, "could not be cached: %v", err)
	}

	return cachedObject, err
}

// List the objects and directories in dir into entries
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "list dir at '%s'", f.cleanPath(dir))

	var cachedDir *Directory

	// Get raw cached entries from cache
	cachedDir = NewDirectoryEmpty(f, f.cleanPath(dir))
	entries, err = f.cache.GetDirEntries(cachedDir)

	if err != nil {
		fs.Debugf(dir, "no dir entries in cache: %v", err)
	} else if len(entries) == 0 {
		// TODO: read empty dirs from source?
	} else {
		fs.Debugf(f, "found dir entries at '%v': %v", dir, len(entries))
		return entries, nil
	}

	// Get live entries from source or fail
	entries, err = f.Fs.List(dir)
	if err != nil {
		return nil, err
	}

	fs.Debugf(f, "source dir entries at '%v': %+v", dir, entries)

	// We used the source so let's cache the result for later usage
	cachedEntries, err := f.cache.AddDirEntries(cachedDir, entries)
	if err != nil {
		// TODO return original list when fails?
		fs.Errorf(dir, "error caching dir entries: %v", err)
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
	fs.Debugf(f, "list recursively from '%s'", dir)

	// we check if the source FS supports ListR
	// if it does, we'll use that to get all the entries, cache them and return
	do := f.Fs.Features().ListR
	if do != nil {
		return f.Fs.Features().ListR(dir, func(entries fs.DirEntries) error {
			// we got called back with a set of entries so let's cache them and call the original callback
			var err error

			for _, entry := range entries {
				switch o := entry.(type) {
				case fs.Object:
					err = f.cache.AddObject(NewObject(f, o))
				case fs.Directory:
					err = f.cache.AddDir(NewDirectory(f, o))
				default:
					return errors.Errorf("Unknown object type %T", entry)
				}

				if err != nil {
					fs.Debugf(entry, "could not be cached: %v", err)
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

// Put in to the remote path with  the modTime given of the given size
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Debugf(f, "put data at '%s'", src.Remote())

	// upload original object
	liveObj, err := f.Fs.Put(in, src, options...)
	if err != nil {
		return nil, err
	}

	// build a cached object and inject it in the cached listing of its parent
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(liveObj, "error saving in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// PutUnchecked uploads the object
func (f *Fs) PutUnchecked(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	fs.Debugf(f, "put data unchecked in '%s'", src.Remote())

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
		fs.Errorf(cachedObj, "error saving in cache: %v", err)
		return cachedObj, err
	}

	return cachedObj, nil
}

// Mkdir makes the directory (container, bucket)
func (f *Fs) Mkdir(dir string) error {
	fs.Debugf(f, "make dir '%s'", dir)

	err := f.Fs.Mkdir(dir)
	if err != nil {
		return err
	}

	if dir == "" && f.Root() == "" { // creating the root is possible but we don't need that cached as we have it already
		fs.Debugf(dir, "skipping empty dir in cache")
		return nil
	}

	// make an empty dir
	err = f.cache.AddDir(NewDirectoryEmpty(f, dir))
	if err != nil {
		fs.Errorf(dir, "error caching dir: %v", err)
		return nil
	}

	return nil
}

// Rmdir removes the directory (container, bucket) if empty
func (f *Fs) Rmdir(dir string) error {
	fs.Debugf(f, "rm dir '%s'", dir)

	err := f.Fs.Rmdir(dir)
	if err != nil {
		return err
	}

	err = f.cache.RemoveDir(NewDirectoryEmpty(f, dir))
	if err != nil {
		fs.Errorf(dir, "error removing cached dir: %v", err)
		return nil
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

	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}

	if srcFs.Fs.Name() != f.Fs.Name() {
		fs.Debugf(srcFs, "can't move directory - not wrapping same remotes")
		return fs.ErrorCantDirMove
	}

	fs.Debugf(f, "move dir '%s'/'%s' -> '%s'", srcRemote, srcFs.Root(), dstRemote)

	err := do(src.Features().UnWrap(), srcRemote, dstRemote)
	if err != nil {
		return err
	}

	// clear any likely dir cached at dst
	srcCachedDir := NewDirectoryEmpty(srcFs, srcRemote)
	err = f.cache.RemoveDir(srcCachedDir)
	if err != nil {
		fs.Errorf(srcCachedDir, "error removing cached dir: %v", err)
		return nil
	}

	// get a list of entries of the new dir to cache
	dstCachedDir := NewDirectoryEmpty(f, dstRemote)
	err = f.cache.AddDir(dstCachedDir)
	if err != nil {
		fs.Errorf(dstRemote, "failed to cache new dir: %v", err)
	}

	return nil
}

// Copy src to this remote using server side copy operations.
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Copy
	if do == nil {
		return nil, fs.ErrorCantCopy
	}

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(srcObj, "can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}

	if srcObj.CacheFs.Fs.Name() != f.Fs.Name() {
		fs.Debugf(srcObj, "can't copy - not wrapping same remote types")
		return nil, fs.ErrorCantCopy
	}

	fs.Debugf(f, "copy obj '%s' -> '%s'", srcObj.Abs(), remote)

	if err := srcObj.RefreshFromSource(); err != nil {
		fs.Errorf(f, "can't move %v - %v", src, err)
		return nil, fs.ErrorCantCopy
	}

	liveObj, err := do(srcObj.Object, remote)
	if err != nil {
		return liveObj, err
	}

	// store in cache
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "error saving in cache: %v", err)
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

	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(srcObj, "can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	if srcObj.CacheFs.Fs.Name() != f.Fs.Name() {
		fs.Debugf(srcObj, "can't move - not wrapping same remote types")
		return nil, fs.ErrorCantMove
	}

	fs.Debugf(f, "move obj '%s' -> %s", srcObj.Abs(), remote)

	if err := srcObj.RefreshFromSource(); err != nil {
		fs.Errorf(f, "can't move %v - %v", src, err)
		return nil, fs.ErrorCantMove
	}

	liveObj, err := do(srcObj.Object, remote)
	if err != nil {
		return liveObj, err
	}

	// add new one in cache
	cachedObj := NewObject(f, liveObj)
	err = f.cache.AddObject(cachedObj)
	if err != nil {
		fs.Errorf(cachedObj, "error saving in cache: %v", err)
		return cachedObj, err
	}

	// remove data from src
	err = f.cache.RemoveObject(srcObj)
	if err != nil {
		fs.Errorf(srcObj, "error removing from cache: %v", err)
	}

	return cachedObj, nil
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() fs.HashSet {
	return f.Fs.Hashes()
}

// Purge all files in the root and the root directory
func (f *Fs) Purge() error {
	fs.Debugf(f, "purge")

	do := f.Fs.Features().Purge
	if do == nil {
		return errors.New("can't Purge")
	}

	err := do()
	if err != nil {
		fs.Errorf(f, "source purge failed: %v", err)
		return err
	}

	d := NewDirectoryEmpty(f, "")
	err = f.Cache().RemoveDir(d)
	if err != nil {
		fs.Errorf(f, "cache purge failed: %v", err)
	}

	f.cacheState.Flush()

	return nil
}

// CleanUp the trash in the Fs
func (f *Fs) CleanUp() error {
	fs.Debugf(f, "cleanup")

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

	fs.Debugf("cache", "starting cache cleanup")
	f.cache.CleanChunksByAge(f.chunkCleanAge)

	fs.Debugf("cache", "starting stats")
	f.cache.Stats()

	fs.Debugf("cache", "starting memory cleanup")
	f.memory.CleanChunksByAge(f.chunkCleanAge)

	f.lastCleanup = time.Now()
	return
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.Fs
}

// DirCacheFlush flushes the dir cache
func (f *Fs) DirCacheFlush() {
	f.cacheState.Flush()
}

func (f *Fs) cleanPath(p string) string {
	p = path.Clean(p)
	if p == "." || p == "/" {
		p = ""
	}

	return p
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
