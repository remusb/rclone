package cache

import (
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ncw/rclone/fs"
)

// Object is a generic file like object that stores basic information about it
type Object struct {
	fs.Object `json:"-"`

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
		Name:          cleanPath(name),
		Dir:           cleanPath(dir),
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
	fullRemote := cleanPath(path.Join(f.Root(), o.Remote()))

	dir, name := path.Split(fullRemote)
	co = &Object{
		CacheFs:   f,
		Name:      cleanPath(name),
		Dir:       cleanPath(dir),
		CacheType: "Object",
	}
	co.updateData(o)
	return co
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
	if err := o.RefreshFromSource(); err != nil {
		return nil, err
	}
	o.CacheFs.CheckIfWarmupNeeded(o.Remote())

	cacheReader := NewObjectHandle(o)
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
	if err := o.RefreshFromSource(); err != nil {
		return err
	}
	fs.Infof(o, "updating object contents with size %q", src.Size())

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
	if err := o.RefreshFromSource(); err != nil {
		return err
	}
	err := o.Object.Remove()
	if err != nil {
		return err
	}
	fs.Infof(o, "removing object")

	_ = o.CacheFs.Cache().RemoveObject(o.Abs())
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
	fs.Debugf(o, "object hash cached: %v", liveHash)

	return liveHash, nil
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
)
