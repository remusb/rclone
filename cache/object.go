package cache

import (
	"io"
	"sync"
	"time"

	"os"
	"path"
	"strings"

	"github.com/ncw/rclone/fs"
	"github.com/patrickmn/go-cache"
)

// GetObjectFromSource returns a single Object from source FS as a callback
type GetObjectFromSource func() (fs.Object, error)

// Object is a generic file like object that stores basic information about it
type Object struct {
	fs.Object      `json:"-"`
	fs.BlockReader `json:"-"`

	CacheFs       *Fs    `json:"-"`        // cache fs
	Name          string `json:"name"`     // name of the directory
	Dir           string `json:"dir"`      // abs path of the object
	CacheModTime  int64  `json:"modTime"`  // modification or creation time - IsZero for unknown
	CacheSize     int64  `json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable bool   `json:"storable"` // says whether this object can be stored

	CacheHashes map[fs.HashType]string `json:"hashes"` // all supported hashes cached
	CacheType   string                 `json:"cacheType"`

	refreshMutex sync.Mutex
}

// NewObject builds one from a generic fs.Object
func NewObject(f *Fs, remote string) *Object {
	fullRemote := path.Join(f.Root(), remote)
	dir, name := path.Split(fullRemote)

	co := &Object{
		CacheFs:       f,
		Name:          f.cleanPath(name),
		Dir:           f.cleanPath(dir),
		CacheModTime:  time.Now().UnixNano(),
		CacheSize:     0,
		CacheStorable: false,
		CacheType:     "Object",
	}

	return co
}

// ObjectFromOriginal builds one from a generic fs.Object
func ObjectFromOriginal(f *Fs, o fs.Object) *Object {
	var co *Object
	fullRemote := path.Join(f.Root(), o.Remote())

	// search in transient cache
	if v, found := f.CacheInfo().Get(fullRemote); found {
		ok := false
		if co, ok = v.(*Object); ok && co != nil {
			co.updateData(o)
			return co
		}
	}

	dir, name := path.Split(fullRemote)
	co = &Object{
		CacheFs:   f,
		Name:      f.cleanPath(name),
		Dir:       f.cleanPath(dir),
		CacheType: "Object",
	}
	co.updateData(o)
	return co
}

// ObjectFromCacheOrSource builds one from a generic fs.Object
func ObjectFromCacheOrSource(f *Fs, remote string, query GetObjectFromSource) (*Object, error) {
	var co *Object
	fullRemote := path.Join(f.Root(), remote)

	if v, found := f.CacheInfo().Get(fullRemote); found {
		ok := false
		if co, ok = v.(*Object); ok && co != nil {
			return co, nil
		}
	}

	dir, name := path.Split(path.Join(f.Root(), remote))
	co = &Object{
		CacheFs:   f,
		Name:      f.cleanPath(name),
		Dir:       f.cleanPath(dir),
		CacheType: "Object",
	}
	err := f.cache.GetObject(co)
	if err == nil {
		co.Cache()
		return co, nil
	}

	liveObject, err := query()
	if err != nil || liveObject == nil {
		return nil, err
	}

	co.updateData(liveObject)
	co.Persist()
	co.Cache()
	return co, nil
}

// ObjectFromSource returns an object by directly asking the source
func ObjectFromSource(f *Fs, remote string, query GetObjectFromSource) (*Object, error) {
	dir, name := path.Split(path.Join(f.Root(), remote))
	co := &Object{
		CacheFs:   f,
		Name:      f.cleanPath(name),
		Dir:       f.cleanPath(dir),
		CacheType: "Object",
	}

	liveObject, err := query()
	if err != nil || liveObject == nil {
		return nil, err
	}

	co.updateData(liveObject)
	co.Cache()
	co.Persist()
	return co, nil
}

func (o *Object) updateData(source fs.Object) {
	o.Object = source
	o.CacheModTime = source.ModTime().UnixNano()
	o.CacheSize = source.Size()
	o.CacheStorable = source.Storable()
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
	return time.Unix(0, o.CacheModTime)
}

// Size returns the cached Size
func (o *Object) Size() int64 {
	return o.CacheSize
}

// Storable returns the cached Storable
func (o *Object) Storable() bool {
	return o.CacheStorable
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
	o.Persist()

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
	o.Persist()
	fs.Debugf(o.Fs(), "updated ModTime %v: %v", o, t)

	return nil
}

// Open is used to request a specific part of the file using fs.RangeOption
func (o *Object) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	//TODO: find a better way to lock this method
	if err := o.RefreshFromSource(); err != nil {
		return nil, err
	}

	cacheReader := NewReader(o)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			_, err := cacheReader.Seek(x.Offset, os.SEEK_SET)
			if err != nil {
				return cacheReader, err
			}
		}
	}

	return cacheReader, nil
}

// Update will change the object data
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	fs.Debugf(o.Fs(), "update data: %v", o)

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
	o.CacheHashes = make(map[fs.HashType]string)

	o.Persist()

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

	o.Flush()
	return err
}

// Hash requests a hash of the object and stores in the cache
// since it might or might not be called, this is lazy loaded
func (o *Object) Hash(ht fs.HashType) (string, error) {
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

	o.Persist()
	fs.Infof(o, "object hash cached: %v", liveHash)

	return liveHash, nil
}

// Flush removes this object from caches
func (o *Object) Flush() *Object {
	o.CacheFs.CacheInfo().Delete(o.Abs())

	// if the object was deleted from source we can clean up the cache too
	err := o.CacheFs.Cache().RemoveObject(o)
	if err != nil {
		fs.Errorf(o, "error removing from cache: %v", err)
		return nil
	}

	return o
}

// Cache adds this object to the transient cache
func (o *Object) Cache() *Object {
	o.CacheFs.CacheInfo().Set(o.Abs(), o, cache.DefaultExpiration)
	return o
}

// Persist adds this object to the persistent cache
func (o *Object) Persist() *Object {
	err := o.CacheFs.Cache().AddObject(o)
	if err != nil {
		fs.Errorf(o, "failed to cache object: %v", err)
	}
	return o
}

var (
	_ fs.Object = (*Object)(nil)
	//_ fs.BlockReader 			= (*Object)(nil)
)
