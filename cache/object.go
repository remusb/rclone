package cache

import (
	"io"
	"sync"
	"time"

	"os"
	"path"
	"strings"

	"github.com/ncw/rclone/fs"
)

// Object is a generic file like object that stores basic information about it
type Object struct {
	fs.Object           `json:"-"`
	fs.ObjectUnbuffered `json:"-"`

	CacheFs       *Fs    `json:"-"`        // cache fs
	Name          string `json:"name"`     // name of the directory
	Dir           string `json:"dir"`      // abs path of the object
	CacheModTime  int64  `json:"modTime"`  // modification or creation time - IsZero for unknown
	CacheSize     int64  `json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable bool   `json:"storable"` // says whether this object can be stored

	CacheHashes map[fs.HashType]string `json:"hashes"` // all supported hashes cached
	CacheType   string                 `json:"cacheType"`
	CacheTs     time.Time              `json:"cacheTs"` // cache timestamp

	refreshMutex sync.Mutex
	cacheManager *Manager
}

// NewObject builds one from a generic fs.Object
func NewObject(f *Fs, o fs.Object) *Object {
	dir, name := path.Split(path.Join(f.Root(), o.Remote()))
	//fs.Debugf(f, "building '%v'/'%v' from '%v'", dir, name, o.Remote())

	co := &Object{
		CacheFs: f,
		Name:    name,
		Dir:     dir,
	}
	co.updateData(o)
	co.cacheManager = NewManager(co)

	//fs.Debugf(f, "remote for '%v' is '%v'", o.Remote(), co.Remote())

	return co
}

// NewObjectEmpty builds one from a generic fs.Object
func NewObjectEmpty(f *Fs, remote string) *Object {
	dir, name := path.Split(path.Join(f.Root(), remote))

	//fs.Debugf(f, "building '%v'/'%v' from '%v'", dir, name, remote)

	co := &Object{
		CacheFs:       f,
		Name:          name,
		Dir:           dir,
		CacheModTime:  time.Now().UnixNano(),
		CacheSize:     0,
		CacheStorable: false,
		CacheType:     "Object",
		CacheTs:       time.Now(),
	}

	//fs.Debugf(f, "remote for '%v' is '%v'", remote, co.Remote())

	co.cacheManager = NewManager(co)
	return co
}

func (o *Object) updateData(source fs.Object) {
	o.Object = source
	o.CacheModTime = source.ModTime().UnixNano()
	o.CacheSize = source.Size()
	o.CacheStorable = source.Storable()
	o.CacheTs = time.Now()
	o.CacheHashes = make(map[fs.HashType]string)
}

// Fs returns its FS info
func (o *Object) Fs() fs.Info {
	return o.CacheFs
}

// GetCacheFs returns the CacheFS type
func (o *Object) GetCacheFs() *Fs {
	return o.CacheFs
}

// String returns a human friendly name for this object
func (o *Object) String() string {
	return path.Join(o.Dir, o.Name)
}

// Remote returns the remote path
func (o *Object) Remote() string {
	p := path.Join(o.Dir, o.Name)
	if o.CacheFs.Root() != "" {
		p = strings.Replace(p, o.CacheFs.Root(), "", 1)
		p = strings.TrimPrefix(p, string(os.PathSeparator))
	}

	return p
}

// Abs returns the absolute path to the object
func (o *Object) Abs() string {
	return path.Join(o.Dir, o.Name)
}

// ModTime returns the cached ModTime
func (o *Object) ModTime() time.Time {
	o.RefreshFromCache()
	return time.Unix(0, o.CacheModTime)
}

// Size returns the cached Size
func (o *Object) Size() int64 {
	o.RefreshFromCache()
	return o.CacheSize
}

// Storable returns the cached Storable
func (o *Object) Storable() bool {
	o.RefreshFromCache()
	return o.CacheStorable
}

// RefreshFromCache requests the original FS for the object in case it comes from a cached entry
func (o *Object) RefreshFromCache() {
	o.refreshMutex.Lock()
	defer o.refreshMutex.Unlock()

	err := o.CacheFs.cache.GetObject(o)
	if err != nil {
		fs.Errorf(o, "error refreshing cached object: %v", err)
		return
	}
}

// RefreshFromSource requests the original FS for the object in case it comes from a cached entry
func (o *Object) RefreshFromSource() error {
	o.refreshMutex.Lock()
	defer o.refreshMutex.Unlock()

	if o.Object != nil {
		return nil
	}

	liveObject, err := o.CacheFs.Fs.NewObject(o.Remote())
	if err != nil {
		fs.Errorf(o, "error refreshing object: %v", err)
		return err
	}

	fs.Debugf(o.Fs(), "refreshed object %v", o)

	o.updateData(liveObject)
	err = o.CacheFs.cache.AddObject(o)
	if err != nil {
		fs.Errorf(o, "error caching refreshed object: %v", err)
		return err
	}

	return nil
}

// SetModTime sets the ModTime of this object
func (o *Object) SetModTime(t time.Time) error {
	if err := o.RefreshFromSource(); err != nil {
		return err
	}

	err := o.Object.SetModTime(t)
	if err != nil {
		return err
	}

	o.CacheModTime = t.UnixNano()
	err = o.CacheFs.cache.AddObject(o)
	if err != nil {
		fs.Errorf(o, "error updating ModTime: %v", err)
		return nil
	}
	fs.Debugf(o.Fs(), "updated ModTime %v: %v", o, t)

	return nil
}

// Open is used to request a specific part of the file using fs.RangeOption
func (o *Object) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	//TODO: find a better way to lock this method
	if err := o.RefreshFromSource(); err != nil {
		return nil, err
	}

	rangeFound := false

	for _, option := range options {
		switch option.(type) {
		case *fs.RangeOption:
			rangeFound = true
		}
	}

	if !rangeFound {
		fs.Errorf(o, "WARNING: reading object directly from source")
	}

	return o.Object.Open(options...)
}

// Read is requested by fuse (most likely) for a specific chunk of the file
func (o *Object) Read(reqSize, reqOffset int64) (respData []byte, err error) {
	if err := o.RefreshFromSource(); err != nil {
		return nil, err
	}

	reqEnd := reqOffset + reqSize
	if reqEnd > o.Object.Size() {
		reqEnd = o.Object.Size()
	}

	go o.cacheManager.StartWorkers(reqOffset)
	return o.cacheManager.GetChunk(reqOffset, reqEnd)
}

// Update will change the object data
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fs.Debugf(o.Fs(), "update data: %v", o)

	//cachedObject, ok := src.(Object)
	//if !ok {
	//	fs.Errorf(o, "wrong object type update: %v", src)
	//	return fs.ErrorCantSetModTime
	//}

	// TODO: do we do something more here?
	if err := o.RefreshFromSource(); err != nil {
		return err
	}

	err := o.Object.Update(in, src, options...)
	if err != nil {
		fs.Errorf(o, "error updating source: %v", err)
		return err
	}

	o.CacheModTime = src.ModTime().UnixNano()
	o.CacheSize = src.Size()
	o.CacheTs = time.Now()
	o.CacheHashes = make(map[fs.HashType]string)

	err = o.CacheFs.cache.AddObject(o)
	if err != nil {
		fs.Errorf(o, "error updating object hash in cache: %v", err)
	}

	return nil
}

// Remove deletes the object from both the cache and the source
func (o *Object) Remove() error {
	fs.Debugf(o.Fs(), "removing object: %v", o)

	if err := o.RefreshFromSource(); err != nil {
		return err
	}

	err := o.Object.Remove()
	if err != nil {
		return err
	}

	// if the object was deleted from source we can clean up the cache too
	err = o.CacheFs.Cache().RemoveObject(o)
	if err != nil {
		fs.Errorf(o, "error removing from cache: %v", err)
		return nil
	}

	return nil
}

// Hash requests a hash of the object and stores in the cache
// since it might or might not be called, this is lazy loaded
func (o *Object) Hash(ht fs.HashType) (string, error) {
	o.RefreshFromCache()

	if o.CacheHashes == nil {
		o.CacheHashes = make(map[fs.HashType]string)
	}

	cachedHash, found := o.CacheHashes[ht]
	if found {
		fs.Infof(o, "object hash found: %v", cachedHash)
		return cachedHash, nil
	}

	if err := o.RefreshFromSource(); err != nil {
		return "", err
	}

	liveHash, err := o.Object.Hash(ht)
	if err != nil {
		return "", err
	}

	o.CacheHashes[ht] = liveHash
	o.CacheTs = time.Now()

	err = o.CacheFs.cache.AddObject(o)
	if err != nil {
		fs.Errorf(o, "error updating object hash in cache: %v", err)
	}
	fs.Infof(o, "object hash cached: %v", liveHash)

	return liveHash, nil
}

var (
	_ fs.Object           = (*Object)(nil)
	_ fs.ObjectUnbuffered = (*Object)(nil)
)
