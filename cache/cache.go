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
	DefCacheChunkSize = "50M"
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

	fs.Errorf("cache-" + name, "info: listAge: %v", listDuration.String())
	fs.Errorf("cache-" + name, "info: fileAge: %v", fileDuration.String())
	fs.Errorf("cache-" + name, "info: chunkSize: %v", chunkSize.String())
	fs.Errorf("cache-" + name, "info: chunkCleanMinAge: %v", chunkCleanDuration.String())

	db := NewBolt(*cacheDbPath)
	err = db.Connect(*cacheDbPurge)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		Fs:     wrappedFs,
		name:   name,
		root:   rpath,
		cache:	db,
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
		Copy:						 nil,
		Move:						 nil,
		DirMove:				 nil,
		DirChangeNotify: nil, // TODO: Add this
		DirCacheFlush:	 nil,
		PutUnchecked:		 nil,
		CleanUp:				 f.CleanUp,
	}).Fill(f).Mask(wrappedFs)

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

// NewObject finds the Object at remote.
// TODO: this is kind of useless
func (f *Fs) NewObject(remote string) (fs.Object, error) {
	key := "/" + remote

	cachedObject, err := f.cache.ObjectGet(key)
	if err != nil {
		fs.Errorf("cache", "info: object not found [%v]: %v", remote, err)
	} else {
		expiresAt := cachedObject.CacheTs.Add(f.fileAge)

		if time.Now().After(expiresAt) {
			fs.Errorf("cache", "info: object expired [%v]", remote)
		} else {
			fs.Errorf("cache", "info: object found [%v]", remote)
			return cachedObject, nil
		}
	}

	// Get live object from source or fail
	liveObject, err := f.Fs.NewObject(remote)
	if err != nil || liveObject == nil {
		fs.Errorf("cache", "Something went wrong, couldn't get object (%v) from source fs (%v): %v", remote, f.Fs.Name(), err)
		return nil, err
	}
	//fs.Errorf("cache", "info: Fetched object (%v) from source fs (%v)", remote, f.Fs.Name())

	// We used the source so let's cache the result for later usage
	cachedObject = NewCachedObject(f, liveObject)
	go f.cache.ObjectPut(cachedObject)

	return cachedObject, nil
	//if err == nil {
	//	fs.Errorf("cache", "Info: Cached object (%v) from source fs (%v)", remote, f.Fs.Name())
	//	return cachedObject, nil
	//}

	// TODO return original object when fails?
	//fs.Errorf(f.name, "Couldn't cache object [%v]: %v", remote, err)
	//return liveObject, nil
}

// List the objects and directories in dir into entries
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	key := "/" + dir

	// Get raw cached entries from cache
	cachedEntries := NewCachedDirEntries(f, entries)
	err = f.cache.ListGet(key, cachedEntries)
	expiresAt := cachedEntries.CacheTs.Add(f.listAge)

	if err != nil {
		// TODO log
		//return nil, err
		fs.Errorf(f.name, "info: dir entries not found [%v]: %v", dir, err)
	} else if len(cachedEntries.DirEntries) == 0 {
		fs.Errorf(f.name, "info: empty dir entries [%v]", dir)
	} else if time.Now().After(expiresAt) {
		fs.Errorf(f.name, "info: expired dir entries [%v]", dir)
	} else {
		// Convert from raw format to proper fs.DirEntries
		entries = cachedEntries.toDirEntries()
		fs.Errorf(f.name, "info: found dir entries [%v] %v", dir, len(entries))

		// todo: always ask from source if we have empty dirs?
		return entries, nil
		//if len(entries) == 0 {
			// TODO log
			//return nil, err
		//	fs.Errorf(f.name, "info: empty dir entries [%v]", dir)
		//} else {
		//	return entries, nil
		//}
	}

	// Get live entries from source or fail
	entries, err = f.Fs.List(dir)
	if err != nil || entries == nil {
		fs.Errorf("cache", "Couldn't list directory (%v) from source fs (%v): %v", dir, f.Fs.Name(), err)
		return nil, err
	}

	// Convert to cache interfaces for storage
	cachedEntries.AddEntries(entries)

	// We used the source so let's cache the result for later usage
	err = f.cache.ListPut(key, cachedEntries)
	if err != nil {
		// TODO return original list when fails?
		fs.Errorf(f.name, "Couldn't cache contents of directory [%v]: %v", dir, err)
		return entries, nil
	}

	return cachedEntries.toDirEntries(), nil
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
// TODO: check it
func (f *Fs) ListR(dir string, callback fs.ListRCallback) (err error) {
	return f.Fs.Features().ListR(dir, callback)
}

// Put in to the remote path with the modTime given of the given size
// TODO Write support
func (f *Fs) Put(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errors.New("can't Put")
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() fs.HashSet {
	return fs.HashSet(fs.HashNone)
}

// Mkdir makes the directory (container, bucket)
// TODO Write support
func (f *Fs) Mkdir(dir string) error {
	return errors.New("can't Mkdir")
}

// Rmdir removes the directory (container, bucket) if empty
// TODO Write support
func (f *Fs) Rmdir(dir string) error {
	return errors.New("can't Rmdir")
}

// Purge all files in the root and the root directory
// TODO Write support
func (f *Fs) Purge() error {
	do := f.Fs.Features().Purge
	if do == nil {
		return errors.New("can't Purge")
	}

	err := do()
	if err != nil {
		f.Cache().Purge()
	}

	return err
}

// Copy src to this remote using server side copy operations.
// TODO Write support
func (f *Fs) Copy(src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantCopy
}

// Move src to this remote using server side move operations.
// TODO Write support
func (f *Fs) Move(src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorCantMove
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server side move operations.
// TODO Write support
func (f *Fs) DirMove(src fs.Fs, srcRemote, dstRemote string) error {
	return fs.ErrorCantDirMove
}

// PutUnchecked uploads the object
// TODO Write support
func (f *Fs) PutUnchecked(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errors.New("can't PutUnchecked")
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

	//if err != nil {
	//	fs.Errorf("cache", "Couldn't cleanup cache", err)
	//	return err
	//}

	f.lastCleanup = time.Now()
	return nil
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
