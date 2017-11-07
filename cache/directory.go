package cache

import (
	"time"

	"os"
	"path"
	"strings"

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

	CacheItems int64  `json:"items"`     // number of objects or -1 for unknown
	CacheType  string `json:"cacheType"` // object type
}

// NewDirectory builds an empty dir which will be used to unmarshal data in it
func NewDirectory(f *Fs, remote string) *Directory {
	var cd *Directory
	fullRemote := cleanPath(path.Join(f.Root(), remote))

	// build a new one
	dir := cleanPath(path.Dir(fullRemote))
	name := cleanPath(path.Base(fullRemote))
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

	dir := cleanPath(path.Dir(fullRemote))
	name := cleanPath(path.Base(fullRemote))
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
	p := cleanPath(path.Join(d.Dir, d.Name))
	if d.CacheFs.Root() != "" {
		p = strings.Replace(p, d.CacheFs.Root(), "", 1)
		p = strings.TrimPrefix(p, string(os.PathSeparator))
	}

	return p
}

// Abs returns the absolute path to the dir
func (d *Directory) Abs() string {
	return cleanPath(path.Join(d.Dir, d.Name))
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

var (
	_ fs.Directory = (*Directory)(nil)
)
