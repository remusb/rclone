package cache

import (
	"time"

	"os"
	"path"
	"strings"

	"github.com/ncw/rclone/fs"
	"github.com/patrickmn/go-cache"
)

type GetEntriesFromSource func() (fs.DirEntries, error)

// Directory is a generic dir that stores basic information about it
type Directory struct {
	fs.Directory `json:"-"`

	CacheFs      *Fs    `json:"-"`       // cache fs
	Name         string `json:"name"`    // name of the directory
	Dir          string `json:"dir"`     // abs path of the directory
	CacheModTime int64  `json:"modTime"` // modification or creation time - IsZero for unknown
	CacheSize    int64  `json:"size"`    // size of directory and contents or -1 if unknown

	CacheItems int64     `json:"items"`     // number of objects or -1 for unknown
	CacheType  string    `json:"cacheType"` // object type
}

// NewDirectory builds an empty dir which will be used to unmarshal data in it
// caching must be managed at the location through Cache() or Persist()
func NewDirectory(f *Fs, remote string) *Directory {
	var cd *Directory
	fullRemote := path.Join(f.Root(), remote)

	// build a new one
	dir := f.cleanPath(path.Dir(fullRemote))
	name := f.cleanPath(path.Base(fullRemote))
	cd = &Directory{
		CacheFs:      f,
		Name:         name,
		Dir:          dir,
		CacheModTime: time.Now().UnixNano(),
		CacheSize:    0,
		CacheItems:   0,
		CacheType:    "Directory",
	}

	return cd
}

// DirectoryFromOriginal builds one from a generic fs.Directory
func DirectoryFromOriginal(f *Fs, d fs.Directory) *Directory {
	var cd *Directory
	fullRemote := path.Join(f.Root(), d.Remote())

	if v, found := f.CacheInfo().Get(fullRemote); found {
		ok := false
		if cd, ok = v.(*Directory); ok && cd != nil {
			cd.CacheModTime = d.ModTime().UnixNano()
			cd.CacheItems = d.Items()
			cd.CacheSize = d.Size()
			return cd
		}
	}

	dir := f.cleanPath(path.Dir(fullRemote))
	name := f.cleanPath(path.Base(fullRemote))
	cd = &Directory{
		Directory:    d,
		CacheFs:      f,
		Name:         name,
		Dir:          dir,
		CacheModTime: d.ModTime().UnixNano(),
		CacheSize:    d.Size(),
		CacheItems:   d.Items(),
		CacheType:    "Directory",
	}

	return cd
}

// DirectoryFromCacheOrNew searches for a dir in the cache or builds a new one
func DirectoryFromCacheOrNew(f *Fs, remote string) *Directory {
	var cd *Directory
	fullRemote := path.Join(f.Root(), remote)

	// search in transient cache
	if v, found := f.CacheInfo().Get(fullRemote); found {
		ok := false
		if cd, ok = v.(*Directory); ok && cd != nil {
			return cd
		}
	}

	// build a new one
	dir := f.cleanPath(path.Dir(fullRemote))
	name := f.cleanPath(path.Base(fullRemote))
	cd = &Directory{
		CacheFs:      f,
		Name:         name,
		Dir:          dir,
		CacheModTime: time.Now().UnixNano(),
		CacheSize:    0,
		CacheItems:   0,
		CacheType:    "Directory",
	}

	// search in persistent cache or ignore and return the empty one
	err := f.Cache().GetDir(cd)
	if err != nil {
		cd.Persist()
	}

	cd.Cache()
	return cd
}

// Fs returns its FS info
func (d *Directory) Fs() fs.Info {
	return d.CacheFs
}

// GetCacheFs returns the CacheFS type
func (d *Directory) GetCacheFs() *Fs {
	return d.CacheFs
}

// String returns a human friendly name for this object
func (d *Directory) String() string {
	return path.Join(d.Dir, d.Name)
}

// Remote returns the remote path
func (d *Directory) Remote() string {
	p := d.CacheFs.cleanPath(path.Join(d.Dir, d.Name))
	if d.CacheFs.Root() != "" {
		p = strings.Replace(p, d.CacheFs.Root(), "", 1)
		p = strings.TrimPrefix(p, string(os.PathSeparator))
	}

	return p
}

// Abs returns the absolute path to the dir
func (d *Directory) Abs() string {
	return d.CacheFs.cleanPath(path.Join(d.Dir, d.Name))
}

// ModTime returns the cached ModTime
func (d *Directory) ModTime() time.Time {
	return time.Unix(0, d.CacheModTime)
}

// Size returns the cached Size
func (d *Directory) Size() int64 {
	return d.CacheSize
}

// Items returns the cached Items
func (d *Directory) Items() int64 {
	return d.CacheItems
}

// GetEntries retrieves all this dir entries from the source
func (d *Directory) GetEntries(query GetEntriesFromSource) (fs.DirEntries, error) {
	var entries fs.DirEntries
	var err error

	entries, err = d.CacheFs.Cache().GetDirEntries(d)
	if err != nil {
		fs.Debugf(d, "no dir entries in cache: %v", err)
	} else if len(entries) == 0 {
		// TODO: read empty dirs from source?
	} else {
		return entries, nil
	}

	entries, err = query()
	if err != nil {
		return nil, err
	}

	// We used the source so let's cache the result for later usage
	cachedEntries, err := d.CacheFs.Cache().AddDirEntries(d, entries)
	if err != nil {
		fs.Errorf(d, "error caching dir entries: %v", err)
		return entries, nil
	}

	return cachedEntries, nil
}

// Remove deletes the dir from all caches
func (d *Directory) Flush() {
	d.CacheFs.CacheInfo().Delete(d.Abs())
	err := d.CacheFs.Cache().RemoveDir(d)
	if err != nil {
		fs.Errorf(d.Abs(), "error removing cached dir: %v", err)
	}
}

// Cache adds this dir to the transient cache
func (d *Directory) Cache() *Directory {
	d.CacheFs.CacheInfo().Set(d.Abs(), d, cache.DefaultExpiration)
	return d
}

// Persist adds this dir to the persistent cache
func (d *Directory) Persist() *Directory {
	err := d.CacheFs.Cache().AddDir(d)
	if err != nil {
		fs.Errorf(d, "failed to cache dir: %v", err)
	}
	return d
}

var (
	_ fs.Directory = (*Directory)(nil)
)
