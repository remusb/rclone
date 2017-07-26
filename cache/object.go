package cache

import (
	"github.com/ncw/rclone/fs"
	"time"
	"io"
	"errors"
	"path"
	"math"
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
	if o.Object == nil {
		o.RefreshObject()
	}

	o.CacheModTime = t

	err := o.CacheFs.cache.ObjectPut(o)
	if err != nil {
		// TODO ignore cache failure
		fs.Errorf("cache", "Couldn't cache object hash [%v]: %v", o.Remote(), err)
	}

	return o.Object.SetModTime(t)
}

func (o *CachedObject) CacheOpen(offset int64, writer *io.PipeWriter) {
	chunkSize := o.CacheFs.chunkSize
	// index for start offset of every chunk (starts from requested offset)
	chunkOffset := offset

	fs.Errorf("cache", "info: object opened (%v:%v)", path.Base(o.Remote()), chunkOffset)

	for {
		var readBytes []byte
		var totalReadBytes int64
		var err error

		// we calculate the modulus of the requested offset with the size of a chunk
		offsetMod := int64(math.Mod(float64(chunkOffset), float64(chunkSize)))

		// we align the start offset of the first chunk to a likely chunk in the storage
		// if it's 0, even better, nothing changes
		// if it's >0, we'll read from the start of the chunk and we'll need to trim the offsetMod bytes from it after EOF
		if offsetMod > 0 {
			chunkOffset = chunkOffset - offsetMod
		}

		fs.Errorf("cache", "info: chunk needed (%v): %v", path.Base(o.Remote()), chunkOffset)

		// search for cached data
		readBytes, err = o.CacheFs.Cache().ObjectDataGet(o, chunkOffset)

		// something wrong with reading from cache or it simply doesn't exist
		if err != nil {
			if o.Object == nil {
				o.RefreshObject()
			}

			// Here we add an option for the wrapping FS that we want a chunk of the file
			rangeOption := &fs.RangeOption{
				Start: chunkOffset,
				End: chunkOffset + chunkSize,
			}

			// open file at desired offset
			rc, err := o.Object.Open(&fs.SeekOption{ Offset: chunkOffset }, rangeOption)
			// if something went wrong, we need to close the writer
			if err != nil {
				fs.Errorf("cache", "info: object open failed (%v): %v", path.Base(o.Remote()), err)
				writer.CloseWithError(err)
				break
			}

			// read the data from source and close that reader
			readBytes = make([]byte, chunkSize)
			c, err := rc.Read(readBytes)
			totalReadBytes = int64(c)
			rc.Close()

			// if we can't read from source we abort
			if err != nil {
				fs.Errorf("cache", "info: object read error (%v)", path.Base(o.Remote()), err)
				writer.CloseWithError(err)
				break
			}

			// cache the copy
			err = o.CacheFs.Cache().ObjectDataPut(o, readBytes, chunkOffset)
			if err != nil {
				// TODO ignore when caching fails
				fs.Errorf("cache", "Couldn't cache chunk [%v:%v]: %+v", path.Base(o.Remote()), chunkOffset, err)
			}

			fs.Errorf("cache", "info: chunk from source (%v): %v-%v", path.Base(o.Remote()), chunkOffset, chunkOffset+int64(totalReadBytes))
		} else {
			totalReadBytes = int64(len(readBytes))

			fs.Errorf("cache", "info: chunk from cache (%v): %v-%v", path.Base(o.Remote()), chunkOffset, chunkOffset+int64(totalReadBytes))
		}

		// align the chunk with the original chunk offset
		if offsetMod > 0 {
			readBytes = readBytes[offsetMod:]
			totalReadBytes = int64(len(readBytes))
			chunkOffset = chunkOffset + offsetMod
			fs.Errorf("cache", "info: aligned chunk (%v): %v-%v", path.Base(o.Remote()), chunkOffset, chunkOffset+int64(totalReadBytes))
		}

		totalWrittenBytes, err := writer.Write(readBytes)
		if err == io.ErrClosedPipe {
			//fs.Errorf("cache", "info: object pipe closed already (%v)", path.Base(o.Remote()))
			fs.Errorf("cache", "info: chunk written (%v): %v-%v", path.Base(o.Remote()), chunkOffset, chunkOffset+int64(totalWrittenBytes))
			break
		} else if err != nil {
			fs.Errorf("cache", "info: chunk write error (%v): %v", path.Base(o.Remote()), err)
			writer.CloseWithError(err)
			break
		}

		fs.Errorf("cache", "info: chunk written (%v): %v-%v", path.Base(o.Remote()), chunkOffset, chunkOffset+int64(totalWrittenBytes))
		chunkOffset = chunkOffset + int64(totalWrittenBytes)
	}
}

func (o *CachedObject) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset int64 = 0

	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		default:
			if option.Mandatory() {
				fs.Errorf("cache", "Unsupported mandatory option: %v", option)
			}
		}
	}

	// start a cleanup in parallel if it's time
	//o.CacheFs.CleanUpCache()

	dstReader, dstWriter := io.Pipe()
	go o.CacheOpen(offset, dstWriter)

	return dstReader, nil
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
