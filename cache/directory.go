package cache

import (
	"time"

	"os"
	"path"
	"strings"
	"sync"

	"github.com/ncw/rclone/fs"
)

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
	CacheTs    time.Time `json:"cacheTs"`   // cache timestamp

	refreshMutex sync.Mutex
}

// NewDirectory builds one from a generic fs.Directory
func NewDirectory(f *Fs, d fs.Directory) *Directory {
	fullRemote := path.Join(f.Root(), d.Remote())
	dir := f.cleanPath(path.Dir(fullRemote))
	name := f.cleanPath(path.Base(fullRemote))

	//fs.Debugf(f, "building %v/%v from %v", dir, name, d.Remote())

	cd := &Directory{
		Directory:    d,
		CacheFs:      f,
		Name:         name,
		Dir:          dir,
		CacheModTime: d.ModTime().UnixNano(),
		CacheSize:    d.Size(),
		CacheItems:   d.Items(),
		CacheType:    "Directory",
		CacheTs:      time.Now(),
	}

	//fs.Debugf(f, "remote for %v is %v", d.Remote(), cd.Remote())
	return cd
}

// NewDirectoryEmpty builds one from a generic fs.Directory
func NewDirectoryEmpty(f *Fs, remote string) *Directory {
	fullRemote := path.Join(f.Root(), remote)
	dir := f.cleanPath(path.Dir(fullRemote))
	name := f.cleanPath(path.Base(fullRemote))

	//fs.Debugf(f, "building '%v'/'%v' from '%v'", dir, name, remote)

	cd := &Directory{
		CacheFs:      f,
		Name:         name,
		Dir:          dir,
		CacheModTime: time.Now().UnixNano(),
		CacheSize:    0,
		CacheItems:   0,
		CacheType:    "Directory",
		CacheTs:      time.Now(),
	}

	//fs.Debugf(f, "remote for '%v' is '%v'", remote, cd.Remote())
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
	d.RefreshFromCache()
	return time.Unix(0, d.CacheModTime)
}

// Size returns the cached Size
func (d *Directory) Size() int64 {
	d.RefreshFromCache()
	return d.CacheSize
}

// Items returns the cached Items
func (d *Directory) Items() int64 {
	d.RefreshFromCache()
	return d.CacheItems
}

// RefreshFromCache requests the original FS for the object in case it comes from a cached entry
func (d *Directory) RefreshFromCache() {
	d.refreshMutex.Lock()
	defer d.refreshMutex.Unlock()

	err := d.CacheFs.cache.GetDir(d)
	if err != nil {
		fs.Errorf(d, "error refreshing cached dir: %v", err)
		return
	}
}

var (
	_ fs.Directory = (*Directory)(nil)
)
