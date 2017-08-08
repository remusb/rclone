package cache

import (
	"github.com/ncw/rclone/fs"
	"time"
	"io"
	"errors"
	"sync"
)

// a generic file like object that stores basic information about it
type CachedObject struct {
	fs.Object									`json:"-"`
	fs.ObjectUnbuffered				`json:"-"`

	CacheFs				*Fs					`json:"-"`				// cache fs
	CacheString		string			`json:"string"`		// name
	CacheRemote  	string			`json:"remote"`   // name of the directory
	CacheModTime 	time.Time		`json:"modTime"`	// modification or creation time - IsZero for unknown
	CacheSize    	int64				`json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable	bool				`json:"storable"`	// says whether this object can be stored

	CacheHashes	 map[fs.HashType]string	`json:"hashes"`		// all supported hashes cached
	CacheType		 string 				`json:"cacheType"`
	CacheTs			 time.Time			`json:"cacheTs"`	// cache timestamp

	sourceMutex	sync.Mutex
	cacheManager *Manager
}

// build one from a generic fs.Object
func NewCachedObject(f *Fs, o fs.Object) *CachedObject {
	co := &CachedObject{
		Object:				o,
		CacheFs:       f,
		CacheString:   o.String(),
		CacheRemote:  o.Remote(),
		CacheModTime: o.ModTime(),
		CacheSize:    o.Size(),
		CacheStorable: o.Storable(),
		CacheType:		"Object",
		CacheTs:			time.Now(),
	}

	co.cacheManager = NewManager(co)

	return co
}

func (o *CachedObject) Fs() fs.Info {
	return o.CacheFs
}

func (o *CachedObject) GetCacheFs() *Fs {
	return o.CacheFs
}

func (o *CachedObject) String() string {
	return o.CacheString
}

func (o *CachedObject) Remote() string {
	return o.CacheRemote
}

func (o *CachedObject) ModTime() time.Time {
	return o.CacheModTime
}

func (o *CachedObject) Size() int64 {
	return o.CacheSize
}

func (o *CachedObject) Storable() bool {
	return o.CacheStorable
}

// this requests the original FS for the object in case it comes from a cached entry
func (o *CachedObject) RefreshObject() error {
	o.sourceMutex.Lock()
	defer o.sourceMutex.Unlock()

	fs.Errorf("cache", "refreshing object %v", o.Remote())

	liveObject, err := o.CacheFs.Fs.NewObject(o.Remote())

	if err != nil {
		fs.Errorf("cache", "couldn't find source object (%v): %v", o.Remote(), err)
		return err
	}

	o.Object = liveObject
	return nil
}

func (o *CachedObject) SetModTime(t time.Time) error {
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

func (o *CachedObject) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
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

func (o *CachedObject) Read(reqSize, reqOffset int64) (respData []byte, err error) {
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

func (o *CachedObject) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errors.New("can't Update")
}

func (o *CachedObject) Remove() error {
	if o.Object == nil {
		o.RefreshObject()
	}

	err := o.Object.Remove()
	// if the object was deleted from source we can clean up the cache too
	if (err == nil) {
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

func (o *CachedObject) Hash(ht fs.HashType) (string, error) {
	if o.CacheHashes == nil {
		o.CacheHashes = make(map[fs.HashType]string)
	}

	cachedHash, found := o.CacheHashes[ht]
	expiresAt := o.CacheTs.Add(o.CacheFs.fileAge)
	if time.Now().After(expiresAt) {
		fs.Errorf("cache", "info: object hash expired (%v)", o.Remote())
		o.CacheFs.Cache().RemoveObject(o)
	} else if !found {
		fs.Errorf("cache", "info: object hash not found (%v)", o.Remote())
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
	_ fs.Object         	= (*CachedObject)(nil)
	_ fs.ObjectUnbuffered = (*CachedObject)(nil)
)
