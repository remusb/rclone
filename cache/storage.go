package cache

//import "github.com/ncw/rclone/fs"

// ObjectInfo provides read only information about an object.
type Storage interface {
	// Cache DirEntries
	// Used mostly on listings
	ListGet(dir string) (cachedEntries CachedDirEntries, err error)
}
