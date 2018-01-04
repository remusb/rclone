// +build !plan9,go1.7

package cache

import (
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

const (
	objectInCache       = "Object"
	objectPendingUpload = "TempObject"
)

// Object is a generic file like object that stores basic information about it
type Object struct {
	fs.Object `json:"-"`

	ParentFs      fs.Fs                  `json:"-"`        // parent fs
	CacheFs       *Fs                    `json:"-"`        // cache fs
	Name          string                 `json:"name"`     // name of the directory
	Dir           string                 `json:"dir"`      // abs path of the object
	CacheModTime  int64                  `json:"modTime"`  // modification or creation time - IsZero for unknown
	CacheSize     int64                  `json:"size"`     // size of directory and contents or -1 if unknown
	CacheStorable bool                   `json:"storable"` // says whether this object can be stored
	CacheType     string                 `json:"cacheType"`
	CacheTs       time.Time              `json:"cacheTs"`
	CacheHashes   map[fs.HashType]string // all supported hashes cached

	refreshMutex sync.Mutex
}

// NewObject builds one from a generic fs.Object
func NewObject(f *Fs, remote string) *Object {
	fullRemote := path.Join(f.Root(), remote)
	dir, name := path.Split(fullRemote)

	cacheType := objectInCache
	parentFs := f.UnWrap()
	if f.tempWritePath != "" {
		_, err := f.cache.searchPendingUpload(fullRemote)
		if err == nil { // queued for upload
			cacheType = objectPendingUpload
			parentFs = f.tempFs
			fs.Debugf(fullRemote, "pending upload found")
		}
	}

	co := &Object{
		ParentFs:      parentFs,
		CacheFs:       f,
		Name:          cleanPath(name),
		Dir:           cleanPath(dir),
		CacheModTime:  time.Now().UnixNano(),
		CacheSize:     0,
		CacheStorable: false,
		CacheType:     cacheType,
		CacheTs:       time.Now(),
	}
	return co
}

// ObjectFromOriginal builds one from a generic fs.Object
func ObjectFromOriginal(f *Fs, o fs.Object) *Object {
	var co *Object
	fullRemote := cleanPath(path.Join(f.Root(), o.Remote()))
	dir, name := path.Split(fullRemote)

	cacheType := objectInCache
	parentFs := f.UnWrap()
	if f.tempWritePath != "" {
		_, err := f.cache.searchPendingUpload(fullRemote)
		if err == nil { // queued for upload
			cacheType = objectPendingUpload
			parentFs = f.tempFs
			fs.Debugf(fullRemote, "pending upload found")
		}
	}

	co = &Object{
		ParentFs:  parentFs,
		CacheFs:   f,
		Name:      cleanPath(name),
		Dir:       cleanPath(dir),
		CacheType: cacheType,
		CacheTs:   time.Now(),
	}
	co.updateData(o)
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

// String returns a human friendly name for this object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	p := path.Join(o.Dir, o.Name)
	return o.CacheFs.cleanRootFromPath(p)
}

// abs returns the absolute path to the object
func (o *Object) abs() string {
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

// refreshFromSource requests the original FS for the object in case it comes from a cached entry
func (o *Object) refreshFromSource() error {
	o.refreshMutex.Lock()
	defer o.refreshMutex.Unlock()
	var err error
	var liveObject fs.Object

	if o.Object != nil {
		return nil
	}
	if o.isTempFile() {
		liveObject, err = o.ParentFs.NewObject(o.Remote())
		err = errors.Wrapf(err, "in parent fs %v", o.ParentFs)
	} else {
		liveObject, err = o.CacheFs.Fs.NewObject(o.Remote())
		err = errors.Wrapf(err, "in cache fs %v", o.CacheFs.Fs)
	}
	if err != nil {
		fs.Errorf(o, "error refreshing object in : %v", err)
		return err
	}
	o.updateData(liveObject)
	o.persist()

	return nil
}

// SetModTime sets the ModTime of this object
func (o *Object) SetModTime(t time.Time) error {
	if err := o.refreshFromSource(); err != nil {
		return err
	}

	err := o.Object.SetModTime(t)
	if err != nil {
		return err
	}

	o.CacheModTime = t.UnixNano()
	o.persist()
	fs.Debugf(o, "updated ModTime: %v", t)

	return nil
}

// Open is used to request a specific part of the file using fs.RangeOption
func (o *Object) Open(options ...fs.OpenOption) (io.ReadCloser, error) {
	if err := o.refreshFromSource(); err != nil {
		return nil, err
	}

	if o.isTempFile() {
		return o.Object.Open(options...)
	}

	var err error
	cacheReader := NewObjectHandle(o, o.CacheFs)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			_, err = cacheReader.Seek(x.Offset, os.SEEK_SET)
		case *fs.RangeOption:
			_, err = cacheReader.Seek(x.Start, os.SEEK_SET)
		}
		if err != nil {
			return cacheReader, err
		}
	}

	return cacheReader, nil
}

// Update will change the object data
func (o *Object) Update(in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if err := o.refreshFromSource(); err != nil {
		return err
	}
	// pause background uploads if active
	if o.CacheFs.tempWritePath != "" {
		o.CacheFs.backgroundRunner.pause()
		defer o.CacheFs.backgroundRunner.play()
		// don't allow started uploads
		if o.isTempFile() && o.tempFileStartedUpload() {
			return errors.Errorf("%v is currently uploading, can't update", o)
		}
	}
	fs.Debugf(o, "updating object contents with size %v", src.Size())

	// FIXME use reliable upload
	err := o.Object.Update(in, src, options...)
	if err != nil {
		fs.Errorf(o, "error updating source: %v", err)
		return err
	}

	// deleting cached chunks and info to be replaced with new ones
	_ = o.CacheFs.cache.RemoveObject(o.abs())

	o.CacheModTime = src.ModTime().UnixNano()
	o.CacheSize = src.Size()
	o.CacheHashes = make(map[fs.HashType]string)
	o.CacheTs = time.Now()
	o.persist()

	return nil
}

// Remove deletes the object from both the cache and the source
func (o *Object) Remove() error {
	if err := o.refreshFromSource(); err != nil {
		return err
	}
	// pause background uploads if active
	if o.CacheFs.tempWritePath != "" {
		o.CacheFs.backgroundRunner.pause()
		defer o.CacheFs.backgroundRunner.play()
		// don't allow started uploads
		if o.isTempFile() && o.tempFileStartedUpload() {
			return errors.Errorf("%v is currently uploading, can't delete", o)
		}
	}
	err := o.Object.Remove()
	if err != nil {
		return err
	}

	fs.Debugf(o, "removing object")
	_ = o.CacheFs.cache.RemoveObject(o.abs())
	_ = o.CacheFs.cache.removePendingUpload(o.abs())
	_ = o.CacheFs.cache.ExpireDir(NewDirectory(o.CacheFs, cleanPath(path.Dir(o.Remote()))))

	return nil
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
	if err := o.refreshFromSource(); err != nil {
		return "", err
	}
	liveHash, err := o.Object.Hash(ht)
	if err != nil {
		return "", err
	}
	o.CacheHashes[ht] = liveHash

	o.persist()
	fs.Debugf(o, "object hash cached: %v", liveHash)

	return liveHash, nil
}

// persist adds this object to the persistent cache
func (o *Object) persist() *Object {
	err := o.CacheFs.cache.AddObject(o)
	if err != nil {
		fs.Errorf(o, "failed to cache object: %v", err)
	}
	return o
}

func (o *Object) isTempFile() bool {
	_, err := o.CacheFs.cache.searchPendingUpload(o.abs())
	if err != nil {
		o.CacheType = objectInCache
		return false
	}

	o.CacheType = objectPendingUpload
	return true
}

func (o *Object) tempFileStartedUpload() bool {
	started, err := o.CacheFs.cache.searchPendingUpload(o.abs())
	if err != nil {
		return false
	}
	return started
}

var (
	_ fs.Object = (*Object)(nil)
)
