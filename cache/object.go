package cache

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/ncw/rclone/fs"
)

// Object is a generic file like object that stores basic information about it
type Object struct {
	fs.Object           `json:"-"`
	fs.ObjectUnbuffered `json:"-"`

	CacheFs       *Fs       `json:"-"`        // cache fs
	CacheString   string    `json:"string"`   // name
	CacheRemote   string    `json:"remote"`   // name of the directory
	CacheModTime  time.Time `json:"modTime"`  // modification or creation time - IsZero for unknown
	CacheSize     int64     `json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable bool      `json:"storable"` // says whether this object can be stored

	CacheHashes map[fs.HashType]string `json:"hashes"` // all supported hashes cached
	CacheType   string                 `json:"cacheType"`
	CacheTs     time.Time              `json:"cacheTs"` // cache timestamp

	sourceMutex  sync.Mutex
	cacheManager *Manager
}

// NewObject builds one from a generic fs.Object
func NewObject(f *Fs, o fs.Object) *Object {
	co := &Object{
		Object:        o,
		CacheFs:       f,
		CacheString:   o.String(),
		CacheRemote:   o.Remote(),
		CacheModTime:  o.ModTime(),
		CacheSize:     o.Size(),
		CacheStorable: o.Storable(),
		CacheType:     "Object",
		CacheTs:       time.Now(),
	}

	co.cacheManager = NewManager(co)

	return co
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
	return o.CacheString
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.CacheRemote
}

// ModTime returns the cached ModTime
func (o *Object) ModTime() time.Time {
	return o.CacheModTime
}

// Size returns the cached Size
func (o *Object) Size() int64 {
	return o.CacheSize
}

// Storable returns the cached Storable
func (o *Object) Storable() bool {
	return o.CacheStorable
}

// RefreshObject requests the original FS for the object in case it comes from a cached entry
func (o *Object) RefreshObject() {
	o.sourceMutex.Lock()
	defer o.sourceMutex.Unlock()

	fs.Errorf("cache", "refreshing object %v", o.Remote())

	liveObject, err := o.CacheFs.Fs.NewObject(o.Remote())

	if err != nil {
		fs.Errorf(o, "couldn't find source object: %v", err)
		return
	}

	o.Object = liveObject
}

// SetModTime sets the ModTime of this object
func (o *Object) SetModTime(t time.Time) error {
	if o.Object == nil {
		o.RefreshObject()
	}

	o.CacheModTime = t

	err := o.CacheFs.cache.AddObject(o)
	if err != nil {
		// TODO ignore cache failure
		fs.Errorf("cache", "couldn't cache object hash [%v]: %v", o.Remote(), err)
	}

	return o.Object.SetModTime(t)
}

// Open is used to request a specific part of the file using fs.RangeOption
func (o *Object) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	//TODO: find a better way to lock this method
	rangeFound := false

	for _, option := range options {
		switch option.(type) {
		case *fs.RangeOption:
			rangeFound = true
		}
	}

	if !rangeFound {
		return nil, errors.New("can't use buffered read from source")
	}

	if o.Object == nil {
		o.RefreshObject()
	}

	// otherwise read from source
	return o.Object.Open(options...)
}

// Read is requested by fuse (most likely) for a specific chunk of the file
func (o *Object) Read(reqSize, reqOffset int64) (respData []byte, err error) {
	if o.Object == nil {
		o.RefreshObject()
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
	if o.Object == nil {
		o.RefreshObject()
	}

	return o.Object.Update(in, src, options...)
}

// Remove deletes the object from both the cache and the source
func (o *Object) Remove() error {
	if o.Object == nil {
		o.RefreshObject()
	}

	err := o.Object.Remove()
	// if the object was deleted from source we can clean up the cache too
	if err == nil {
		// we delete the cache and don't care what happens with it
		err = o.CacheFs.Cache().RemoveObject(o)
		if err != nil {
			fs.Errorf("cache", "Couldn't delete object (%v) from cache: %+v", o.Remote(), err)
		} else {
			fs.Errorf("cache", "info: object deleted (%v)", o.Remote())
		}

		return nil
	}

	fs.Errorf("cache", "Source couldn't delete the object (%v) so we're keeping it in the cache: %+v", o.Remote(), err)
	return err
}

// Hash requests a hash of the object and stores in the cache
// since it might or might not be called, this is lazy loaded
func (o *Object) Hash(ht fs.HashType) (string, error) {
	if o.CacheHashes == nil {
		o.CacheHashes = make(map[fs.HashType]string)
	}

	cachedHash, found := o.CacheHashes[ht]
	expiresAt := o.CacheTs.Add(o.CacheFs.fileAge)
	if time.Now().After(expiresAt) {
		fs.Errorf("cache", "info: object hash expired (%v)", o.Remote())
		_ = o.CacheFs.Cache().RemoveObject(o)
	} else if !found {
		// noop
	} else {
		fs.Errorf("cache", "info: object hash found (%v)", o.Remote())
		return cachedHash, nil
	}

	if o.Object == nil {
		o.RefreshObject()
	}

	liveHash, err := o.Object.Hash(ht)
	if err != nil {
		fs.Errorf("cache", "Couldn't get object hash (%v) from source fs (%v): %v", ht.String(), o.CacheFs.Name(), err)
		return "", err
	}

	o.CacheHashes[ht] = liveHash
	o.CacheTs = time.Now()

	err = o.CacheFs.cache.AddObject(o)
	if err != nil {
		// TODO return live hash when fails?
		fs.Errorf(o.CacheFs.Name(), "Couldn't cache object hash [%v]: %v", o.Remote(), err)
	}

	fs.Errorf("cache", "info: object hash cached (%v)", o.Remote())

	return liveHash, nil
}

var (
	_ fs.Object           = (*Object)(nil)
	_ fs.ObjectUnbuffered = (*Object)(nil)
)
