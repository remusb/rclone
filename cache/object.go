package cache

import (
	"github.com/ncw/rclone/fs"
	"time"
	"io"
	"errors"
	"bytes"
	"io/ioutil"
	"path"
)

// a generic file like object that stores basic information about it
type CachedObject struct {
	fs.Object									`json:"-"`

	CacheFs				*Fs					`json:"-"`				// cache fs
	CacheString		string			`json:"string"`		// name
	CacheRemote  	string			`json:"remote"`   // name of the directory
	CacheModTime 	time.Time		`json:"modTime"`	// modification or creation time - IsZero for unknown
	CacheSize    	int64				`json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable	bool				`json:"storable"`	// says whether this object can be stored

	CacheHashes	 map[fs.HashType]string	`json:"hashes"`		// all supported hashes cached
	CacheType		 string 				`json:"cacheType"`
	CacheTs			 time.Time			`json:"cacheTs"`	// cache timestamp
}

// build one from a generic fs.Object
func NewCachedObject(f *Fs, o fs.Object) *CachedObject {
	return &CachedObject{
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
	liveObject, err := o.CacheFs.Fs.NewObject(o.Remote())

	if err != nil {
		fs.Errorf("cache", "Couldn't find source object (%v): %v", o.Remote(), err)
		return err
	}

	o.Object = liveObject
	return nil
}

func (o *CachedObject) SetModTime(t time.Time) error {
	return errors.New("can't SetModTime")
}

func (o *CachedObject) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset int64 = 0

	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}

	// start a cleanup in parallel if it's time
	o.CacheFs.CleanUpCache()

	// search for cached data
	cachedDataReader, bytesRead, err := o.CacheFs.Cache().ObjectDataGet(o, offset)
	if err != nil {
		fs.Errorf("cache", "info: object data not found (%v:%v)", path.Base(o.Remote()), offset)
	} else {
		fs.Errorf("cache", "info: returning cached data [%v %v:%v]", path.Base(o.Remote()), offset, offset+int64(bytesRead))
		return ioutil.NopCloser(cachedDataReader), nil
	}

	if o.Object == nil {
		o.RefreshObject()
	}

	// Here we add an option for the wrapping FS that we want a chunk of the file
	rangeOption := &fs.RangeOption{
		Start: offset,
		End: offset + o.CacheFs.chunkSize,
	}
	options = append(options, rangeOption)

	// Get the data from the source and make a copy of it
	rc, err := o.Object.Open(options...)
	defer rc.Close() // let's close it cause we got what we need
	liveData := make([]byte, o.CacheFs.chunkSize)
	actualRead, err := rc.Read(liveData)

	fs.Errorf("cache", "info: read from source [%v %v:%v]", path.Base(o.Remote()), offset, offset+int64(actualRead))

	// cache the copy
	err = o.CacheFs.Cache().ObjectDataPut(o, liveData, offset)
	if err != nil {
		// TODO return live data when fails?
		fs.Errorf("cache", "Couldn't cache object data [%v:%v]: %+v", path.Base(o.Remote()), offset, err)
	}

	fs.Errorf("cache", "info: returning live data [%v %v:%v]", path.Base(o.Remote()), offset, offset+int64(actualRead))

	// return the copy in a new reader
	return ioutil.NopCloser(bytes.NewReader(liveData)), nil
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
		err = o.CacheFs.Cache().ObjectRemove(o)

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
		o.CacheFs.Cache().ObjectRemove(o)
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
	//fs.Errorf("cache", "Info: Fetched object hash (%v) from source fs (%v)", o.Remote(), o.CacheFs.Fs.Name())

	o.CacheHashes[ht] = liveHash
	o.CacheTs = time.Now()

	err = o.CacheFs.cache.ObjectPut(o)
	if err != nil {
		// TODO return live hash when fails?
		fs.Errorf(o.CacheFs.Name(), "Couldn't cache object hash [%v]: %v", o.Remote(), err)
	}

	fs.Errorf("cache", "info: object hash cached (%v)", o.Remote())

	return liveHash, nil
}

var (
	_ fs.Object         	= (*CachedObject)(nil)
)
