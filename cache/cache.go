package cache

import (
	"path"
	"strings"
	"fmt"

	"github.com/ncw/rclone/fs"
	"github.com/pkg/errors"
)

// Globals
var (
	// Flags
	cacheDbPath = fs.StringP("cache-db-path", "", path.Join(path.Dir(fs.ConfigPath), "cache.db"), "Path to cache DB")
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cache",
		Description: "Cache a remote",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "remote",
			Help: "Remote to cache.\nNormally should contain a ':' and a path, eg \"myremote:path/to/dir\",\n\"myremote:bucket\" or maybe \"myremote:\" (not recommended).",
		}},
	})
}

// NewFs contstructs an Fs from the path, container:path
func NewFs(name, rpath string) (fs.Fs, error) {
	remote := fs.ConfigFileGet(name, "remote")
	if strings.HasPrefix(remote, name+":") {
		return nil, errors.New("can't point cache remote at itself - check the value of the remote setting")
	}

	// Look for a file first
	remotePath := path.Join(remote, rpath)
	wrappedFs, err := fs.NewFs(remotePath)

	// if that didn't produce a file, look for a directory
	if err != fs.ErrorIsFile {
		remotePath = path.Join(remote, rpath)
		wrappedFs, err = fs.NewFs(remotePath)
	}

	if err != fs.ErrorIsFile && err != nil {
		return nil, errors.Wrapf(err, "failed to make remote %q to wrap", remotePath)
	}

	f := &Fs{
		Fs:     wrappedFs,
		name:   name,
	}

	f.features = (&fs.Features{}).Fill(f)

	return f, err
}

// Fs represents a wrapped fs.Fs
type Fs struct {
	fs.Fs
	name     string
	root     string
	features *fs.Features // optional features
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("Cached drive '%s:%s'", f.name, f.root)
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(dir string) (entries fs.DirEntries, err error) {
	entries, err = f.Fs.List(dir)

	if err != nil {
		return nil, err
	}

	return entries, err
}

// Object describes a wrapped for being read from the Fs
//
// This decrypts the remote name and decrypts the data
type Object struct {
	fs.Object
	f *Fs
}

func (f *Fs) newObject(o fs.Object) *Object {
	return &Object{
		Object: o,
		f:      f,
	}
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.f
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.Remote()
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.Object.Remote()
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.Object.Size()
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(hash fs.HashType) (string, error) {
	return "", nil
}

// UnWrap returns the wrapped Object
func (o *Object) UnWrap() fs.Object {
	return o.Object
}

// ObjectInfo describes a wrapped fs.ObjectInfo for being the source
//
// This encrypts the remote name and adjusts the size
type ObjectInfo struct {
	fs.ObjectInfo
	f *Fs
}

func (f *Fs) newObjectInfo(src fs.ObjectInfo) *ObjectInfo {
	return &ObjectInfo{
		ObjectInfo: src,
		f:          f,
	}
}

// Fs returns read only access to the Fs that this object is part of
func (o *ObjectInfo) Fs() fs.Info {
	return o.f
}

// Remote returns the remote path
func (o *ObjectInfo) Remote() string {
	return o.ObjectInfo.Remote()
}

// Size returns the size of the file
func (o *ObjectInfo) Size() int64 {
	return o.ObjectInfo.Size()
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *ObjectInfo) Hash(hash fs.HashType) (string, error) {
	return "", nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.ObjectInfo     = (*ObjectInfo)(nil)
	_ fs.Object         = (*Object)(nil)
)
