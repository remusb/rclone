// +build !plan9,go1.7

package cache_test

import (
	"github.com/stretchr/testify/require"
	"time"
	"os"
	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/cmd/mount"
	"github.com/ncw/rclone/cmd/mountlib"
	"testing"
)

func (r *run) mountFs(t *testing.T, f fs.Fs) {
	device := f.Name()+":"+f.Root()
	var options = []fuse.MountOption{
		fuse.MaxReadahead(uint32(mountlib.MaxReadAhead)),
		fuse.Subtype("rclone"),
		fuse.FSName(device), fuse.VolumeName(device),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.AllowOther(),
	}
	err := os.MkdirAll(r.mntDir, os.ModePerm)
	require.NoError(t, err)
	c, err := fuse.Mount(r.mntDir, options...)
	require.NoError(t, err)
	filesys := mount.NewFS(f)
	server := fusefs.New(c, nil)

	// Serve the mount point in the background returning error to errChan
	r.unmountRes = make(chan error, 1)
	go func() {
		err := server.Serve(filesys)
		closeErr := c.Close()
		if err == nil {
			err = closeErr
		}
		r.unmountRes <- err
	}()

	// check if the mount process has an error to report
	<-c.Ready
	require.NoError(t, c.MountError)

	r.unmountFn = func() error {
		// Shutdown the VFS
		filesys.VFS.Shutdown()
		return fuse.Unmount(r.mntDir)
	}

	r.vfs = filesys.VFS
	r.isMounted = true
}

func (r *run) unmountFs(t *testing.T, f fs.Fs) {
	var err error

	for i := 0; i < 4; i++ {
		err = r.unmountFn()
		if err != nil {
			//log.Printf("signal to umount failed - retrying: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	require.NoError(t, err)
	err = <-r.unmountRes
	require.NoError(t, err)
	err = r.vfs.CleanUp()
	require.NoError(t, err)
	r.isMounted = false
}
