package cache

import (
	"path"
	"strings"
	"fmt"
	"io"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
	"time"
)

const (
	DefCacheChunkSize = "5M"
	DefCacheListAge = "5m"
	DefCacheFileAge = "5m"
	DefCacheChunkCleanAge = "2m"
)

// Globals
var (
	// Flags
	cacheDbPath = fs.StringP("cache-db-path", "", path.Join(path.Dir(fs.ConfigPath), "cache.db"), "Path to cache DB")
	cacheDbPurge = fs.BoolP("cache-db-purge", "", false, "Purge the cache DB before")
	cacheChunkSize = fs.SizeSuffix(-1)
	cacheListAge = fs.StringP("cache-dir-list-age", "", "", "How much time should directory listings be stored in cache")
	cacheFileAge = fs.StringP("cache-file-info-age", "", "", "How much time should object info be stored in cache")
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
			Optional:   true,
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
			Optional:   true,
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
			Optional:   true,
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
			Optional:   true,
		}},
	})

	fs.VarP(&cacheChunkSize, "cache-chunk-size", "", "The size of a chunk. Examples: 1024 (or 1024b), 10M, 1G. Default: 10M")
}

type CacheStats struct {
	TotalDirLists		int
	TotalBytes			string
	TotalChunks			int
	TotalFiles			int
}

// Fs represents a wrapped fs.Fs
type Fs struct {
	fs.Fs

	name     	string
	root     	string
	features	*fs.Features // optional features
	cache   	*Bolt
	memory		*Memory

	listAge		time.Duration
	fileAge		time.Duration
	chunkSize int64
	chunkCleanAge	time.Duration

	lastCleanup	time.Time
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

	db := NewBolt(*cacheDbPath)
	err = db.Connect(*cacheDbPurge)
	if err != nil {
		return nil, err
	}

	m := NewMemory()

	f := &Fs{
		Fs:     wrappedFs,
		name:   name,
		root:   rpath,
		cache:	db,
		memory: m,
		listAge: listDuration,
		fileAge: fileDuration,
		chunkSize: int64(chunkSize),
		chunkCleanAge: chunkCleanDuration,
		lastCleanup: time.Now(),
	}

	f.features = (&fs.Features{
		ReadMimeType:    false, // MimeTypes not supported with crypt
		WriteMimeType:   false,
		Purge:					 f.Purge,
		Copy:						 f.Copy,
		Move:						 f.Move,
		DirMove:				 f.DirMove,
		DirChangeNotify: nil, // TODO: Add this
		DirCacheFlush:	 nil,
		PutUnchecked:		 f.PutUnchecked,
		CleanUp:				 f.CleanUp,
		FileReadRaw:		 true,
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

func (f *Fs) Cache() *Bolt {
	return f.cache
}

func (f *Fs) Memory() *Memory {
	return f.memory
}

// NewObject finds the Object at remote.
// TODO: this is kind of useless
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	cachedObject, err := f.cache.ObjectGet(f.cleanRemote(remote))

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
	cachedObject = NewCachedObject(f, liveObject)
	f.cache.ObjectPut(cachedObject)

	return cachedObject, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	key := f.cleanRemote(dir)

	// Get raw cached entries from cache
	cachedEntries := NewCachedDirEntries(f, entries)
	err = f.cache.ListGet(key, cachedEntries)

	if err != nil {
		fs.Errorf(f, "info: couldn't get dir entries from cache [%v]: %v", key, err)
	} else {
		f.CleanUpCache()

		// Convert from raw format to proper fs.DirEntries
		entries = cachedEntries.toDirEntries()
		fs.Errorf(cachedEntries, "info: found dir entries [%v] %v", key, len(entries))

		return entries, nil
	}

	// Get live entries from source or fail
	entries, err = f.Fs.List(dir)
	if err != nil || entries == nil {
		fs.Errorf(f, "couldn't list directory (%v) from source fs (%v): %v", key, f.Fs.Name(), err)
		return nil, err
	}

	// Convert to cache interfaces for storage
	cachedEntries.AddEntries(entries)

	// We used the source so let's cache the result for later usage
	err = f.cache.ListPut(key, cachedEntries)
	if err != nil {
		// TODO return original list when fails?
		fs.Errorf(cachedEntries, "couldn't cache contents of directory [%v]: %v", key, err)
		return entries, nil
	}

	return cachedEntries.toDirEntries(), nil
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	// we want to get in here to give a chance the cache to return some entries
	// and maybe even cache everything in one go
	entries, err := f.List(dir)
	if err != nil {
		return err
	}

	return callback(entries)
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// get parent folder
	dir := f.getParentRemote(src.Remote())
	fs.Errorf(src, "info: object uploading to %v", dir)

	// upload original object
	originalObj, err := f.Fs.Put(in, src, options...)
	if err != nil {
		return nil, err
	}

	// build a cached object and inject it in the cached listing of its parent
	cachedObj := NewCachedObject(f, originalObj)
	cachedEntries := NewEmptyCachedDirEntries(f)

	err = f.cache.ListGet(dir, cachedEntries)
	// not in cache or expired
	if err != nil {
		fs.Errorf(src, "info: couldn't get parent entries: %v", err)
		return cachedObj, err
	}

	// We used the source so let's cache the result for later usage
	cachedEntries.AddItem(cachedObj)
	err = f.cache.ListPut(dir, cachedEntries)
	if err != nil {
		fs.Errorf(dir, "couldn't cache contents of directory: %v", err)
	}

	return cachedObj, err
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

	// make an empty dir
	cachedEntries := NewEmptyCachedDirEntries(f)
	err = f.cache.ListPut(f.cleanRemote(dir), cachedEntries)
	if err != nil {
		fs.Errorf(dir, "couldn't cache contents of directory: %v", err)
	}

	// build a cached dir and inject it in the cached listing of its parent
	parent := f.getParentRemote(dir)
	cachedObj := NewEmptyCachedDirectory(f, dir)
	cachedEntries = NewEmptyCachedDirEntries(f)

	err = f.cache.ListGet(parent, cachedEntries)
	// not in cache or expired
	if err != nil {
		fs.Errorf(parent, "info: couldn't get parent entries: %v", err)
		return nil
	}

	// We used the source so let's cache the result for later usage
	cachedEntries.AddItem(cachedObj)
	err = f.cache.ListPut(parent, cachedEntries)
	if err != nil {
		fs.Errorf(parent, "couldn't cache contents of directory: %v", err)
	}

	return nil
}

// Rmdir removes the directory (container, bucket) if empty
func (f *Fs) Rmdir(dir string) error {
	key := f.cleanRemote(dir)

	err := f.Fs.Rmdir(dir)
	if err != nil {
		return err
	}

	err = f.cache.ListRemove(key)
	if err != nil {
		fs.Errorf("cache", "failed to remove cached dir (%v): %v", dir, err)
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

	originalObj, err := do(src, remote)
	if err != nil {
		return originalObj, err
	}

	// we could in theory move the chunks around but if they're not really needed, they're gonna sit around in the cache

	f.expireListing(f.getParentRemote(remote))
	cachedObj := NewCachedObject(f, originalObj)

	return cachedObj, nil
}

// Move src to this remote using server side move operations.
func (f *Fs) Move(src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Move
	if do == nil {
		return nil, fs.ErrorCantMove
	}

	originalObj, err := do(src, remote)
	if err != nil {
		return originalObj, err
	}

	// we could in theory move the chunks around but if they're not really needed, they're gonna sit around in the cache

	// expire dest parent
	f.expireListing(f.getParentRemote(remote))
	// expire src parent
	f.expireListing(f.getParentRemote(src.Remote()))

	// remove data from src
	srcCachedObj := NewCachedObject(f, src)
	f.cache.ObjectRemove(srcCachedObj)

	cachedObj := NewCachedObject(f, originalObj)
	return cachedObj, nil
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

	// expire dest parent
	f.expireListing(f.getParentRemote(dstRemote))

	return nil
}

// PutUnchecked uploads the object
func (f *Fs) PutUnchecked(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	do := f.Fs.Features().PutUnchecked
	if do == nil {
		return nil, errors.New("can't PutUnchecked")
	}

	originalObj, err := do(in, src, options...)
	if err != nil {
		return nil, err
	}

	cachedObj := NewCachedObject(f, originalObj)
	f.expireListing(f.getParentRemote(cachedObj.Remote()))

	return cachedObj, err
}

// CleanUp the trash in the Fs
func (f *Fs) CleanUp() error {
	f.CleanUpCache()

	do := f.Fs.Features().CleanUp
	if do == nil {
		return errors.New("can't CleanUp")
	}
	f.lastCleanup = time.Now()

	return do()
}

func (f *Fs) CleanUpCache() error {
	if time.Now().Before(f.lastCleanup.Add(f.chunkCleanAge)) {
		return nil
	}

	fs.Errorf("cache", "info: starting cache cleanup")
	f.cache.ObjectDataClean(f.chunkCleanAge)

	fs.Errorf("cache", "info: starting stats")
	f.cache.Stats()

	f.lastCleanup = time.Now()
	return nil
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.Fs
}

// this will retrieve a listing from cache and set its timestamp to an older date to expire
func (f *Fs) expireListing(dir string) error {
	// Get raw cached entries from cache
	cachedEntries := NewEmptyCachedDirEntries(f)
	err := f.cache.ListGet(dir, cachedEntries)
	if err != nil {
		fs.Errorf(f.name, "info: dir entries not found [%v]: %v", dir, err)
		return err
	}
	cachedEntries.CacheTs = cachedEntries.CacheTs.Truncate(f.listAge)

	// let's cache the result for later usage
	err = f.cache.ListPut(dir, cachedEntries)
	if err != nil {
		fs.Errorf(f.name, "couldn't cache contents of directory [%v]: %v", dir, err)
		return err
	}

	return err
}

// gets a parent remote (folder) from the current path and cleans it up
func (f *Fs) getParentRemote(p string) string {
	dir := path.Dir(p)
	if dir == "." { // if it's root, override it
		dir = ""
	}

	return f.cleanRemote(dir)
}

// clean a remote
func (f *Fs) cleanRemote(p string) string {
	if p == "." { // if it's root, override it
		p = ""
	}

	return "/" + p
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
