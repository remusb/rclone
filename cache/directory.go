package cache

import (
	"time"

	"github.com/ncw/rclone/fs"
)

// Directory is a generic dir that stores basic information about it
type Directory struct {
	fs.Directory `json:"-"`

	CacheFs      *Fs       `json:"-"`       // cache fs
	CacheString  string    `json:"string"`  // name
	CacheRemote  string    `json:"remote"`  // name of the directory
	CacheModTime time.Time `json:"modTime"` // modification or creation time - IsZero for unknown
	CacheSize    int64     `json:"size"`    // size of directory and contents or -1 if unknown

	CacheItems int64     `json:"items"`     // number of objects or -1 for unknown
	CacheType  string    `json:"cacheType"` // object type
	CacheTs    time.Time `json:"cacheTs"`   // cache timestamp
}

// NewDirectory builds one from a generic fs.Directory
func NewDirectory(f *Fs, d fs.Directory) *Directory {
	return &Directory{
		Directory:    d,
		CacheFs:      f,
		CacheString:  d.String(),
		CacheRemote:  d.Remote(),
		CacheModTime: d.ModTime(),
		CacheSize:    d.Size(),
		CacheItems:   d.Items(),
		CacheType:    "Directory",
		CacheTs:      time.Now(),
	}
}

// NewDirectoryEmpty builds one from a generic fs.Directory
func NewDirectoryEmpty(f *Fs, remote string) *Directory {
	return &Directory{
		CacheFs:      f,
		CacheString:  remote,
		CacheRemote:  remote,
		CacheModTime: time.Now(),
		CacheSize:    0,
		CacheItems:   0,
		CacheType:    "Directory",
		CacheTs:      time.Now(),
	}
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
	return d.CacheString
}

// Remote returns the remote path
func (d *Directory) Remote() string {
	return d.CacheRemote
}

// ModTime returns the cached ModTime
func (d *Directory) ModTime() time.Time {
	return d.CacheModTime
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
