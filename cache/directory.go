package cache

import (
	"github.com/ncw/rclone/fs"
	"time"
)

// a generic dir that stores basic information about it
type CachedDirectory struct {
	fs.Directory						`json:"-"`

	CacheFs			*Fs					`json:"-"`				// cache fs
	CacheString	string			`json:"string"`		// name
	CacheRemote string			`json:"remote"`   // name of the directory
	CacheModTime time.Time	`json:"modTime"`	// modification or creation time - IsZero for unknown
	CacheSize    int64			`json:"size"`     // size of directory and contents or -1 if unknown

	CacheItems   int64			`json:"items"`		// number of objects or -1 for unknown
	CacheType		 string			`json:"cacheType"`// object type
	CacheTs			 time.Time	`json:"cacheTs"`	// cache timestamp
}

// build one from a generic fs.Directory
func NewCachedDirectory(f *Fs, d fs.Directory) *CachedDirectory {
	return &CachedDirectory{
		Directory:		 d,
		CacheFs:       f,
		CacheString:   d.String(),
		CacheRemote:  d.Remote(),
		CacheModTime: d.ModTime(),
		CacheSize:    d.Size(),
		CacheItems:   d.Items(),
		CacheType:		"Directory",
		CacheTs:			time.Now(),
	}
}

// build one from a generic fs.Directory
func NewEmptyCachedDirectory(f *Fs, remote string) *CachedDirectory {
	return &CachedDirectory{
		CacheFs:       	f,
		CacheString:  	remote,
		CacheRemote:  	remote,
		CacheModTime: 	time.Now(),
		CacheSize:    	0,
		CacheItems:   	0,
		CacheType:			"Directory",
		CacheTs:				time.Now(),
	}
}

func (d *CachedDirectory) Fs() fs.Info {
	return d.CacheFs
}

func (d *CachedDirectory) GetCacheFs() *Fs {
	return d.CacheFs
}

func (d *CachedDirectory) String() string {
	return d.CacheString
}

func (d *CachedDirectory) Remote() string {
	return d.CacheRemote
}

func (d *CachedDirectory) ModTime() time.Time {
	return d.CacheModTime
}

func (d *CachedDirectory) Size() int64 {
	return d.CacheSize
}

func (d *CachedDirectory) Items() int64 {
	return d.CacheItems
}

var (
	_ fs.Directory    	= (*CachedDirectory)(nil)
)
