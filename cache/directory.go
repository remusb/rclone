package cache

import (
	"github.com/ncw/rclone/fs"
	"time"
	"encoding/json"
	"github.com/pkg/errors"
)

// we need a separate store for dir entries to maintain data
// this needs to be converted between listings and cache storage
type CachedDirEntries struct {
	CacheFs			*Fs							`json:"-"`
	DirEntries fs.DirEntries 		`json:"entries"`
	CacheTs			time.Time				`json:"cacheTs"`	// cache timestamp
}

// this translates the json and inspects each dir entry
// it will cast each entry to a proper object based on the data in it
func (ce *CachedDirEntries) UnmarshalJSON(b []byte) error {
	var objMap map[string]*json.RawMessage
	err := json.Unmarshal(b, &objMap)
	if err != nil {
		fs.Errorf("cache", "error during unmarshalling map for remote (%v)", ce.CacheFs.String())
		return err
	}

	var rawDirEntries []*json.RawMessage
	err = json.Unmarshal(*objMap["entries"], &rawDirEntries)
	if err != nil {
		fs.Errorf("cache", "error during unmarshalling entries for remote (%v)", ce.CacheFs.String())
		return err
	}
	err = json.Unmarshal(*objMap["cacheTs"], &ce.CacheTs)
	if err != nil {
		fs.Errorf("cache", "error during unmarshalling ts for remote (%v)", ce.CacheFs.String())
		return err
	}

	// Let's add a place to store our de-serialized objects
	ce.DirEntries = make(fs.DirEntries, len(rawDirEntries))

	var m map[string]interface{}
	for index, rawMessage := range rawDirEntries {
		err = json.Unmarshal(*rawMessage, &m)
		if err != nil {
			return err
		}

		// Depending on the type, we can run json.Unmarshal again on the same byte slice
		// But this time, we'll pass in the appropriate struct instead of a map
		if m["cacheType"] == "Directory" {
			var d CachedDirectory
			err := json.Unmarshal(*rawMessage, &d)
			if err != nil {
				fs.Errorf("cache", "error during unmarshalling dir for remote (%v)", ce.CacheFs.String())
				return err
			}
			d.CacheFs = ce.CacheFs
			// After creating our struct, we should save it
			ce.DirEntries[index] = &d
		} else if m["cacheType"] == "Object" {
			var o CachedObject
			err := json.Unmarshal(*rawMessage, &o)
			if err != nil {
				fs.Errorf("cache", "error during unmarshalling obj for remote (%v)", ce.CacheFs.String())
				return err
			}
			o.CacheFs = ce.CacheFs
			// After creating our struct, we should save it
			ce.DirEntries[index] = &o
		} else {
			fs.Errorf("cache", "Unsupported type found (%+v)!", m)
			return errors.Errorf("Unsupported type found (%+v)!", m)
		}
	}

	return nil
}

// this constructs a new store based on a generic one
func NewCachedDirEntries(f *Fs, de fs.DirEntries) *CachedDirEntries {
	var dirEntries fs.DirEntries

	for _, entry := range de {
		switch entry.(type) {
		case fs.Object:
			dirEntries = append(dirEntries, NewCachedObject(f, entry.(fs.Object)))
		case fs.Directory:
			dirEntries = append(dirEntries, NewCachedDirectory(f, entry.(fs.Directory)))
		}
	}

	return &CachedDirEntries{
		CacheFs: f,
		DirEntries: dirEntries,
		CacheTs: time.Now(),
	}
}

func (ce *CachedDirEntries) AddEntries(de fs.DirEntries) {
	var dirEntries fs.DirEntries

	for _, entry := range de {
		switch entry.(type) {
		case fs.Object:
			dirEntries = append(dirEntries, NewCachedObject(ce.CacheFs, entry.(fs.Object)))
		case fs.Directory:
			dirEntries = append(dirEntries, NewCachedDirectory(ce.CacheFs, entry.(fs.Directory)))
		}
	}

	ce.DirEntries = dirEntries
}

// this exports from a cache store to a generic one
func (ce *CachedDirEntries) toDirEntries() fs.DirEntries {
	var dirEntries fs.DirEntries

	for _, entry := range ce.DirEntries {
		dirEntries = append(dirEntries, entry)
	}

	return dirEntries
}

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
