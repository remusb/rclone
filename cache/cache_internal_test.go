// +build !plan9,go1.7

package cache_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ncw/rclone/cache"
	_ "github.com/ncw/rclone/drive"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fstest"
	"github.com/ncw/rclone/local"
	flag "github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

var (
	remoteName = flag.String("remote-internal", "TestCache", "Remote to test with, defaults to local filesystem")
	okDiff     = time.Second * 9 // really big diff here but the build machines seem to be slow. need a different way for this
	allCfgMap  = map[string]string{
		"plex_url":         "",
		"plex_username":    "",
		"plex_password":    "",
		"chunk_size":       cache.DefCacheChunkSize,
		"info_age":         cache.DefCacheInfoAge,
		"chunk_total_size": cache.DefCacheTotalChunkSize,
	}
	allFlagMap = map[string]string{
		"cache-db-path":              filepath.Join(fs.CacheDir, "cache-backend"),
		"cache-chunk-path":           filepath.Join(fs.CacheDir, "cache-backend"),
		"cache-db-purge":             "true",
		"cache-chunk-size":           cache.DefCacheChunkSize,
		"cache-total-chunk-size":     cache.DefCacheTotalChunkSize,
		"cache-chunk-clean-interval": cache.DefCacheChunkCleanInterval,
		"cache-info-age":             cache.DefCacheInfoAge,
		"cache-read-retries":         strconv.Itoa(cache.DefCacheReadRetries),
		"cache-workers":              strconv.Itoa(cache.DefCacheTotalWorkers),
		"cache-chunk-no-memory":      "false",
		"cache-rps":                  strconv.Itoa(cache.DefCacheRps),
		"cache-writes":               "false",
		"cache-tmp-upload-path":      "",
		"cache-tmp-wait-time":        "0s",
	}
)

func TestInternalListRootAndInnerRemotes(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tilrair", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	// Instantiate inner fs
	innerFolder := "inner"
	err := rootFs.Mkdir(innerFolder)
	require.NoError(t, err)
	rootFs2, boltDb2 := newCacheFs(t, *remoteName, "tilrair/"+innerFolder, true, nil, nil)
	defer cleanupFs(t, rootFs2, boltDb2)

	obj := writeObjectString(t, rootFs2, "one", "content")

	listRoot, err := rootFs.List("")
	require.NoError(t, err)
	listRootInner, err := rootFs.List(innerFolder)
	require.NoError(t, err)
	listInner, err := rootFs2.List("")
	require.NoError(t, err)

	require.Lenf(t, listRoot, 1, "remote %v should have 1 entry", rootFs.Root())
	require.Lenf(t, listRootInner, 1, "remote %v should have 1 entry in %v", rootFs.Root(), innerFolder)
	require.Lenf(t, listInner, 1, "remote %v should have 1 entry", rootFs2.Root())

	err = obj.Remove()
	require.NoError(t, err)
}

func TestInternalObjWrapFsFound(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tiowff", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	wrappedFs := cfs.UnWrap()
	data := "content"
	writeObjectString(t, wrappedFs, "second", data)

	listRoot, err := rootFs.List("")
	require.NoError(t, err)
	require.Lenf(t, listRoot, 1, "remote %v should have 1 entry", rootFs.Root())

	co, err := rootFs.NewObject("second")
	require.NoError(t, err)
	r, err := co.Open()
	require.NoError(t, err)
	cachedData, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)

	strCached := string(cachedData)
	require.Equal(t, data, strCached)

	err = co.Remove()
	require.NoError(t, err)

	listRoot, err = wrappedFs.List("")
	require.NoError(t, err)
	require.Lenf(t, listRoot, 0, "remote %v should have 0 entries: %v", wrappedFs.Root(), listRoot)
}

func TestInternalObjNotFound(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tionf", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	obj, err := rootFs.NewObject("404")
	require.Error(t, err)
	require.Nil(t, obj)
}

func TestInternalCachedWrittenContentMatches(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "ticwcm", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	testData := make([]byte, (chunkSize*4 + chunkSize/2))
	testSize, err := rand.Read(testData)
	require.Equal(t, len(testData), testSize, "data size doesn't match")
	require.NoError(t, err)

	// write the object
	o := writeObjectBytes(t, rootFs, "data.bin", testData)
	require.Equal(t, o.Size(), int64(testSize))

	// check sample of data from in-file
	sampleStart := chunkSize / 2
	sampleEnd := chunkSize
	testSample := testData[sampleStart:sampleEnd]
	checkSample := readDataFromObj(t, o, sampleStart, sampleEnd, false)
	require.Equal(t, int64(len(checkSample)), sampleEnd-sampleStart)
	require.Equal(t, checkSample, testSample)
}

func TestInternalCachedUpdatedContentMatches(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "ticucm", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	// create some rand test data
	testData1 := []byte(fstest.RandomString(100))
	testData2 := []byte(fstest.RandomString(200))

	// write the object
	o := updateObjectBytes(t, rootFs, "data.bin", testData1, testData2)
	require.Equal(t, o.Size(), int64(len(testData2)))

	// check data from in-file
	reader, err := o.Open()
	require.NoError(t, err)
	checkSample, err := ioutil.ReadAll(reader)
	_ = reader.Close()
	require.NoError(t, err)
	require.Equal(t, checkSample, testData2)
}

func TestInternalWrappedWrittenContentMatches(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tiwwcm", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	testData := make([]byte, (chunkSize*4 + chunkSize/2))
	testSize, err := rand.Read(testData)
	require.Equal(t, len(testData), testSize)
	require.NoError(t, err)

	// write the object
	o := writeObjectBytes(t, cfs.UnWrap(), "data.bin", testData)
	require.Equal(t, o.Size(), int64(testSize))

	o2, err := rootFs.NewObject("data.bin")
	require.NoError(t, err)
	require.Equal(t, o2.Size(), o.Size())

	// check sample of data from in-file
	sampleStart := chunkSize / 2
	sampleEnd := chunkSize
	testSample := testData[sampleStart:sampleEnd]
	checkSample := readDataFromObj(t, o2, sampleStart, sampleEnd, false)
	require.Equal(t, len(checkSample), len(testSample))

	for i := 0; i < len(checkSample); i++ {
		require.Equal(t, testSample[i], checkSample[i])
	}
}

func TestInternalLargeWrittenContentMatches(t *testing.T) {
	t.Skip("FIXME disabled because it is unreliable")
	rootFs, boltDb := newCacheFs(t, *remoteName, "tilwcm", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	testData := make([]byte, (chunkSize*10 + chunkSize/2))
	testSize, err := rand.Read(testData)
	require.Equal(t, len(testData), testSize)
	require.NoError(t, err)

	// write the object
	o := writeObjectBytes(t, cfs.UnWrap(), "data.bin", testData)
	require.Equal(t, o.Size(), int64(testSize))

	o2, err := rootFs.NewObject("data.bin")
	require.NoError(t, err)
	require.Equal(t, o2.Size(), o.Size())

	// check data from in-file
	checkSample := readDataFromObj(t, o2, int64(0), int64(testSize), false)
	require.Equal(t, len(checkSample), len(testData))

	for i := 0; i < len(checkSample); i++ {
		require.Equal(t, testData[i], checkSample[i], "byte: %d (%d), chunk: %d", int64(i)%chunkSize, i, int64(i)/chunkSize)
	}
}

func TestInternalLargeWrittenContentMatches2(t *testing.T) {
	cryptFs, boltDb := newCacheFs(t, *remoteName, "tilwcm2", true, nil, nil)
	defer cleanupFs(t, cryptFs, boltDb)

	cfs, err := getCacheFs(cryptFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()
	fileSize := 87197196
	readOffset := 87195648

	// create some rand test data
	testData := make([]byte, fileSize)
	testSize, err := rand.Read(testData)
	require.Equal(t, len(testData), testSize)
	require.NoError(t, err)

	// write the object
	o := writeObjectBytes(t, cryptFs, "data.bin", testData)
	require.Equal(t, o.Size(), int64(testSize))

	o2, err := cryptFs.NewObject("data.bin")
	require.NoError(t, err)
	require.Equal(t, o2.Size(), o.Size())

	// check data from in-file
	reader, err := o2.Open(&fs.SeekOption{Offset: int64(readOffset)})
	require.NoError(t, err)
	rs, ok := reader.(io.Seeker)
	require.True(t, ok)
	checkOffset, err := rs.Seek(int64(readOffset), 0)
	require.NoError(t, err)
	require.Equal(t, checkOffset, int64(readOffset))
	checkSample, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	_ = reader.Close()

	require.Equal(t, len(checkSample), fileSize-readOffset)
	for i := 0; i < fileSize-readOffset; i++ {
		require.Equal(t, testData[readOffset+i], checkSample[i], "byte: %d (%d), chunk: %d", int64(i)%chunkSize, i, int64(i)/chunkSize)
	}
}

func TestInternalWrappedFsChangeNotSeen(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tiwfcns", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	co := writeObjectRandomBytes(t, rootFs, "", (chunkSize*4 + chunkSize/2))

	// update in the wrapped fs
	o, err := cfs.UnWrap().NewObject(co.Remote())
	require.NoError(t, err)
	err = o.SetModTime(co.ModTime().Truncate(time.Hour))
	require.NoError(t, err)

	// get a new instance from the cache
	co2, err := rootFs.NewObject(o.Remote())
	require.NoError(t, err)

	require.NotEqual(t, o.ModTime().String(), co.ModTime().String())
	require.NotEqual(t, o.ModTime().String(), co2.ModTime().String())
	require.Equal(t, co.ModTime().String(), co2.ModTime().String())
}

func TestInternalChangeSeenAfterDirCacheFlush(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "ticsadcf", true, nil, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	co := writeObjectRandomBytes(t, rootFs, "", (chunkSize*4 + chunkSize/2))

	// update in the wrapped fs
	o, err := cfs.UnWrap().NewObject(co.Remote())
	require.NoError(t, err)
	err = o.SetModTime(co.ModTime().Add(-1 * time.Hour))
	require.NoError(t, err)

	// get a new instance from the cache
	co2, err := rootFs.NewObject(o.Remote())
	require.NoError(t, err)

	require.NotEqual(t, o.ModTime().String(), co.ModTime().String())
	require.NotEqual(t, o.ModTime().String(), co2.ModTime().String())
	require.Equal(t, co.ModTime().String(), co2.ModTime().String())

	cfs.DirCacheFlush() // flush the cache

	l, err := cfs.UnWrap().List("")
	require.NoError(t, err)
	require.Len(t, l, 1)
	o2 := l[0]

	// get a new instance from the cache
	co, err = rootFs.NewObject(o.Remote())
	require.NoError(t, err)
	require.Equal(t, o2.ModTime().String(), co.ModTime().String())
}

func TestInternalCacheWrites(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "ticw", true, nil, map[string]string{"cache-writes": "true"})
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()

	// create some rand test data
	co := writeObjectRandomBytes(t, rootFs, "", (chunkSize*4 + chunkSize/2))
	expectedTs := time.Now()
	ts, err := boltDb.GetChunkTs(path.Join(rootFs.Root(), co.Remote()), 0)
	require.NoError(t, err)
	require.WithinDuration(t, expectedTs, ts, okDiff)
}

func TestInternalMaxChunkSizeRespected(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "timcsr", true, nil, map[string]string{"cache-workers": "1"})
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)
	chunkSize := cfs.ChunkSize()
	totalChunks := 20

	// create some rand test data
	obj := writeObjectRandomBytes(t, cfs, "", (int64(totalChunks-1)*chunkSize + chunkSize/2))
	o, err := rootFs.NewObject(obj.Remote())
	require.NoError(t, err)
	co, ok := o.(*cache.Object)
	require.True(t, ok)

	for i := 0; i < 4; i++ { // read first 4
		_ = readDataFromObj(t, co, chunkSize*int64(i), chunkSize*int64(i+1), false)
	}
	cfs.CleanUpCache(true)
	// the last 2 **must** be in the cache
	require.True(t, boltDb.HasChunk(co, chunkSize*2))
	require.True(t, boltDb.HasChunk(co, chunkSize*3))

	for i := 4; i < 6; i++ { // read next 2
		_ = readDataFromObj(t, co, chunkSize*int64(i), chunkSize*int64(i+1), false)
	}
	cfs.CleanUpCache(true)
	// the last 2 **must** be in the cache
	require.True(t, boltDb.HasChunk(co, chunkSize*4))
	require.True(t, boltDb.HasChunk(co, chunkSize*5))
}

func TestInternalExpiredEntriesRemoved(t *testing.T) {
	rootFs, boltDb := newCacheFs(t, *remoteName, "tieer", true, map[string]string{"info_age": "5s"}, nil)
	defer cleanupFs(t, rootFs, boltDb)

	cfs, err := getCacheFs(rootFs)
	require.NoError(t, err)

	// create some rand test data
	_ = writeObjectString(t, cfs, "one", "one content")
	err = cfs.Mkdir("test")
	require.NoError(t, err)
	_ = writeObjectString(t, cfs, "test/second", "second content")

	l, err := cfs.List("test")
	require.NoError(t, err)
	require.Len(t, l, 1)

	err = cfs.UnWrap().Mkdir("test/test2")
	require.NoError(t, err)

	l, err = cfs.List("test")
	require.NoError(t, err)
	require.Len(t, l, 1)

	waitTime := time.Second * 5
	t.Logf("Waiting %v seconds for cache to expire\n", waitTime)
	time.Sleep(waitTime)

	l, err = cfs.List("test")
	require.NoError(t, err)
	require.Len(t, l, 2)
}

// FIXME, enable this when mount is sorted out
//func TestInternalFilesMissingInMount1904(t *testing.T) {
//	t.Skip("Not yet")
//	if runtime.GOOS == "windows" {
//		t.Skip("Not yet")
//	}
//	id := "tifm1904"
//	rootFs, _ := newCacheFs(t, *remoteName, id, false,
//		map[string]string{"chunk_size": "5M", "info_age": "1m", "chunk_total_size": "500M", "cache-writes": "true"})
//	mntPoint := path.Join("/tmp", "tifm1904-mnt")
//	testPoint := path.Join(mntPoint, id)
//	checkOutput := "1 10 100 11 12 13 14 15 16 17 18 19 2 20 21 22 23 24 25 26 27 28 29 3 30 31 32 33 34 35 36 37 38 39 4 40 41 42 43 44 45 46 47 48 49 5 50 51 52 53 54 55 56 57 58 59 6 60 61 62 63 64 65 66 67 68 69 7 70 71 72 73 74 75 76 77 78 79 8 80 81 82 83 84 85 86 87 88 89 9 90 91 92 93 94 95 96 97 98 99 "
//
//	_ = os.MkdirAll(mntPoint, os.ModePerm)
//
//	list, err := rootFs.List("")
//	require.NoError(t, err)
//	found := false
//	list.ForDir(func(d fs.Directory) {
//		if strings.Contains(d.Remote(), id) {
//			found = true
//		}
//	})
//
//	if !found {
//		t.Skip("Test folder '%v' doesn't exist", id)
//	}
//
//	mountFs(t, rootFs, mntPoint)
//	defer unmountFs(t, mntPoint)
//
//	for i := 1; i <= 2; i++ {
//		out, err := exec.Command("ls", testPoint).Output()
//		require.NoError(t, err)
//		require.Equal(t, checkOutput, strings.Replace(string(out), "\n", " ", -1))
//		t.Logf("root path has all files")
//		_ = writeObjectString(t, rootFs, path.Join(id, strconv.Itoa(i), strconv.Itoa(i), "one_file"), "one content")
//
//		for j := 1; j <= 100; j++ {
//			out, err := exec.Command("ls", path.Join(testPoint, strconv.Itoa(j))).Output()
//			require.NoError(t, err)
//			require.Equal(t, checkOutput, strings.Replace(string(out), "\n", " ", -1), "'%v' doesn't match", j)
//		}
//		obj, err := rootFs.NewObject(path.Join(id, strconv.Itoa(i), strconv.Itoa(i), "one_file"))
//		require.NoError(t, err)
//		err = obj.Remove()
//		require.NoError(t, err)
//		t.Logf("folders contain all the files")
//
//		out, err = exec.Command("date").Output()
//		require.NoError(t, err)
//		t.Logf("check #%v date: '%v'", i, strings.Replace(string(out), "\n", " ", -1))
//
//		if i < 2 {
//			time.Sleep(time.Second * 60)
//		}
//	}
//}

func TestUploadTempDirCreated(t *testing.T) {
	id := "tutdc"
	_ = os.RemoveAll(path.Join("/tmp", id))
	rootFs, boltDb := newCacheFs(t, *remoteName, id, true,
		map[string]string{"chunk_size": "5M", "info_age": "60m", "chunk_total_size": "500M"},
		map[string]string{"cache-tmp-upload-path": path.Join("/tmp", id)})
	defer cleanupFs(t, rootFs, boltDb)

	_, err := os.Stat(path.Join("/tmp", id))
	require.NoError(t, err)
}

func TestUploadQueue(t *testing.T) {
	id := "tuq"
	_ = os.RemoveAll(path.Join("/tmp", id))
	rootFs, boltDb := newCacheFs(t, *remoteName, id, true,
		map[string]string{"chunk_size": "5M", "info_age": "60m", "chunk_total_size": "500M"},
		map[string]string{"cache-tmp-upload-path": path.Join("/tmp", id)})
	defer cleanupFs(t, rootFs, boltDb)

	_, err := getCacheFs(rootFs)
	require.NoError(t, err)

	// create some rand test data
	_ = writeObjectString(t, rootFs, "one", "one content")

	// check if it can be read
	obj1, err := rootFs.NewObject("one")
	require.NoError(t, err)
	data1 := readDataFromObj(t, obj1, 0, int64(len([]byte("one content"))), false)
	require.Equal(t, []byte("one content"), data1)
	// validate that it exists in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "one"))
	require.NoError(t, err)

	// wait for background uploader to do its thing
	time.Sleep(time.Second * 2)

	// check if it was removed from temp fs
	_, err = os.Stat(path.Join("/tmp", id, "one"))
	require.True(t, os.IsNotExist(err))
	// check if it can be read
	obj2, err := rootFs.NewObject("one")
	require.NoError(t, err)
	data2 := readDataFromObj(t, obj2, 0, int64(len([]byte("one content"))), false)
	require.Equal(t, []byte("one content"), data2)
}

func TestUploadQueueMoreFiles(t *testing.T) {
	id := "tuqmf"
	_ = os.RemoveAll(path.Join("/tmp", id))
	rootFs, boltDb := newCacheFs(t, *remoteName, id, true,
		map[string]string{"chunk_size": "5M", "info_age": "60m", "chunk_total_size": "500M"},
		map[string]string{"cache-tmp-upload-path": path.Join("/tmp", id)})
	defer cleanupFs(t, rootFs, boltDb)

	err := rootFs.Mkdir("test")
	require.NoError(t, err)
	minSize := 52428800
	maxSize := 104857600
	totalFiles := 100
	rand.Seed(time.Now().Unix())
	for i := 0; i < totalFiles; i++ {
		size := int64(rand.Intn(maxSize-minSize) + minSize)
		o := writeObjectRandomBytes(t, rootFs, "test", size)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, o.Remote()))
		require.NoError(t, err)
	}
	// check if cache lists 100 files, likely temp upload didn't finish yet
	de1, err := rootFs.List("test")
	require.NoError(t, err)
	require.Len(t, de1, totalFiles)

	// wait for background uploader to do its thing
	time.Sleep(time.Second * 1)

	// retry until we have no more temp files and fail if they don't go down to 0
	retryTimes := 5
	for i := 1; i <= retryTimes; i++ {
		tf, err := ioutil.ReadDir(path.Join("/tmp", id, "test"))
		require.NoError(t, err)

		if len(tf) > 0 && i < retryTimes {
			time.Sleep(time.Second * time.Duration(i))
			continue
		}
		require.Len(t, tf, 0)
	}
}

func TestUploadTempFileOperations(t *testing.T) {
	id := "tutfo"
	_ = os.RemoveAll(path.Join("/tmp", id))
	rootFs, boltDb := newCacheFs(t, *remoteName, id, true,
		map[string]string{"chunk_size": "5M", "info_age": "60m", "chunk_total_size": "500M"},
		map[string]string{"cache-tmp-upload-path": path.Join("/tmp", id), "cache-tmp-wait-time": "1h"})
	defer cleanupFs(t, rootFs, boltDb)

	boltDb.PurgeTempUploads()

	// create some rand test data
	_ = rootFs.Mkdir("test")
	_ = writeObjectString(t, rootFs, "test/one", "one content")

	// check if it can be read
	obj1, err := rootFs.NewObject("test/one")
	require.NoError(t, err)
	data1 := readDataFromObj(t, obj1, 0, int64(len([]byte("one content"))), false)
	require.Equal(t, []byte("one content"), data1)
	// validate that it exists in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)

	// test DirMove
	if rootFs.Features().DirMove != nil {
		err = rootFs.Features().DirMove(rootFs, "test", "test2")
		require.Error(t, err)
		obj1, err = rootFs.NewObject("test/one")
		require.NoError(t, err)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one"))
		require.NoError(t, err)
		_ = rootFs.Mkdir("test")
	} else {
		t.Logf("DirMove not supported by %v", rootFs)
	}

	// test Rmdir
	_ = writeObjectString(t, rootFs, "test/one", "one content")
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	err = rootFs.Rmdir("test")
	require.Error(t, err)
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	// validate that it doesn't exist in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)

	// test Move/Rename
	if rootFs.Features().Move != nil {
		_ = writeObjectString(t, rootFs, "test/one", "one content")
		obj1, err := rootFs.NewObject("test/one")
		require.NoError(t, err)
		obj2, err := rootFs.Features().Move(obj1, "test/one2")
		require.NoError(t, err)
		// try to read from it
		obj1, err = rootFs.NewObject("test/one")
		require.Error(t, err)
		obj2, err = rootFs.NewObject("test/one2")
		require.NoError(t, err)
		data2 := readDataFromObj(t, obj2, 0, int64(len([]byte("one content"))), false)
		require.Equal(t, []byte("one content"), data2)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one"))
		require.Error(t, err)
		_, err = os.Stat(path.Join("/tmp", id, "test/one2"))
		require.NoError(t, err)
	} else {
		t.Logf("Move not supported by %v", rootFs)
	}

	// test Copy
	if rootFs.Features().Copy != nil {
		_ = writeObjectString(t, rootFs, "test/one", "one content")
		obj1, err := rootFs.NewObject("test/one")
		require.NoError(t, err)
		obj3, err := rootFs.Features().Copy(obj1, "test/one3")
		require.NoError(t, err)
		// try to read from it
		obj3, err = rootFs.NewObject("test/one3")
		require.NoError(t, err)
		data3 := readDataFromObj(t, obj3, 0, int64(len([]byte("one content"))), false)
		require.Equal(t, []byte("one content"), data3)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one3"))
		require.NoError(t, err)
	} else {
		t.Logf("Copy not supported by %v", rootFs)
	}

	// test Remove
	_ = writeObjectString(t, rootFs, "test/one", "one content")
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	err = obj1.Remove()
	require.NoError(t, err)
	_, err = rootFs.NewObject("test/one")
	require.Error(t, err)
	// validate that it doesn't exist in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.Error(t, err)

	// test Update
	_ = writeObjectString(t, rootFs, "test/one", "one content")
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	data1 = []byte("one content updated")
	r := bytes.NewReader(data1)
	objInfo1 := fs.NewStaticObjectInfo("test/one", time.Now(), int64(len(data1)), true, nil, rootFs)
	err = obj1.Update(r, objInfo1)
	require.NoError(t, err)
	tmpInfo, err := os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)
	require.Equal(t, int64(len(data1)), tmpInfo.Size())
	obj2, err := rootFs.NewObject("test/one")
	require.NoError(t, err)
	data2 := readDataFromObj(t, obj2, 0, int64(len(data1)), false)
	require.Equal(t, data1, data2)
	err = obj2.Remove()
	require.NoError(t, err)

	// test SetModTime
	_ = writeObjectString(t, rootFs, "test/one", "one content")
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	futureDate := time.Now().Add(time.Minute * 60)
	err = obj1.SetModTime(futureDate)
	require.NoError(t, err)
	tmpInfo, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)
	require.WithinDuration(t, futureDate, tmpInfo.ModTime(), okDiff)
	obj2, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	require.WithinDuration(t, futureDate, obj2.ModTime(), okDiff)
}

func TestUploadUploadingFileOperations(t *testing.T) {
	id := "tuufo"
	_ = os.RemoveAll(path.Join("/tmp", id))
	rootFs, boltDb := newCacheFs(t, *remoteName, id, true,
		map[string]string{"chunk_size": "5M", "info_age": "60m", "chunk_total_size": "500M"},
		map[string]string{"cache-tmp-upload-path": path.Join("/tmp", id), "cache-tmp-wait-time": "1h"})
	defer cleanupFs(t, rootFs, boltDb)

	boltDb.PurgeTempUploads()

	// create some rand test data
	_ = rootFs.Mkdir("test")
	_ = writeObjectString(t, rootFs, "test/one", "one content")

	// check if it can be read
	obj1, err := rootFs.NewObject("test/one")
	require.NoError(t, err)
	data1 := readDataFromObj(t, obj1, 0, int64(len([]byte("one content"))), false)
	require.Equal(t, []byte("one content"), data1)
	// validate that it exists in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)

	err = boltDb.SetPendingUploadToStarted(path.Join(rootFs.Root(), "test/one"))
	require.NoError(t, err)

	// test DirMove
	if rootFs.Features().DirMove != nil {
		err = rootFs.Features().DirMove(rootFs, "test", "test2")
		require.Error(t, err)
		t.Logf("DirMove expected error: %v", err)
		obj1, err = rootFs.NewObject("test/one")
		require.NoError(t, err)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one"))
		require.NoError(t, err)
	} else {
		t.Logf("DirMove not supported by %v", rootFs)
	}

	err = rootFs.Rmdir("test")
	require.Error(t, err)
	t.Logf("Rmdir expected error: %v", err)
	obj1, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	// validate that it exists in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)

	// test Move/Rename
	if rootFs.Features().Move != nil {
		_, err := rootFs.Features().Move(obj1, "test/one2")
		require.Error(t, err)
		t.Logf("Move expected error: %v", err)
		obj1, err = rootFs.NewObject("test/one")
		require.NoError(t, err)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one"))
		require.NoError(t, err)
	} else {
		t.Logf("Move not supported by %v", rootFs)
	}

	// test Copy -- allowed
	if rootFs.Features().Copy != nil {
		_, err := rootFs.Features().Copy(obj1, "test/one2")
		require.NoError(t, err)
		obj1, err = rootFs.NewObject("test/one")
		require.NoError(t, err)
		obj2, err := rootFs.NewObject("test/one2")
		require.NoError(t, err)
		data2 := readDataFromObj(t, obj2, 0, int64(len([]byte("one content"))), false)
		require.Equal(t, []byte("one content"), data2)
		// validate that it exists in temp fs
		_, err = os.Stat(path.Join("/tmp", id, "test/one"))
		require.NoError(t, err)
		_, err = os.Stat(path.Join("/tmp", id, "test/one2"))
		require.NoError(t, err)
	} else {
		t.Logf("Copy not supported by %v", rootFs)
	}

	// test Remove
	err = obj1.Remove()
	require.Error(t, err)
	t.Logf("Remove expected error: %v", err)
	_, err = rootFs.NewObject("test/one")
	require.NoError(t, err)
	// validate that it exists in temp fs
	_, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)

	// test Update
	data1 = []byte("one content updated")
	r := bytes.NewReader(data1)
	objInfo1 := fs.NewStaticObjectInfo("test/one", time.Now(), int64(len(data1)), true, nil, rootFs)
	err = obj1.Update(r, objInfo1)
	require.Error(t, err)
	t.Logf("Updated expected error: %v", err)
	tmpInfo, err := os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)
	data2 := readDataFromObj(t, obj1, 0, int64(len(data1)), true)
	require.Equal(t, "one content", string(data2))

	// test SetModTime -- allowed
	futureDate := time.Now().Add(time.Minute * 60)
	err = obj1.SetModTime(futureDate)
	require.NoError(t, err)
	tmpInfo, err = os.Stat(path.Join("/tmp", id, "test/one"))
	require.NoError(t, err)
	require.WithinDuration(t, futureDate, tmpInfo.ModTime(), okDiff)
	obj2, err := rootFs.NewObject("test/one")
	require.NoError(t, err)
	require.WithinDuration(t, futureDate, obj2.ModTime(), okDiff)
}

func writeObjectRandomBytes(t *testing.T, f fs.Fs, p string, size int64) fs.Object {
	remote := path.Join(p, strconv.Itoa(rand.Int())+".bin")
	// create some rand test data
	testData := make([]byte, size)
	testSize, err := rand.Read(testData)
	require.Equal(t, size, int64(len(testData)))
	require.Equal(t, size, int64(testSize))
	require.NoError(t, err)

	o := writeObjectBytes(t, f, remote, testData)
	require.Equal(t, size, o.Size())

	return o
}

func writeObjectString(t *testing.T, f fs.Fs, remote, content string) fs.Object {
	return writeObjectBytes(t, f, remote, []byte(content))
}

func writeObjectBytes(t *testing.T, f fs.Fs, remote string, data []byte) fs.Object {
	in := bytes.NewReader(data)
	modTime := time.Now()
	objInfo := fs.NewStaticObjectInfo(remote, modTime, int64(len(data)), true, nil, f)

	obj, err := f.Put(in, objInfo)
	require.NoError(t, err)

	return obj
}

func updateObjectBytes(t *testing.T, f fs.Fs, remote string, data1 []byte, data2 []byte) fs.Object {
	in1 := bytes.NewReader(data1)
	in2 := bytes.NewReader(data2)
	objInfo1 := fs.NewStaticObjectInfo(remote, time.Now(), int64(len(data1)), true, nil, f)
	objInfo2 := fs.NewStaticObjectInfo(remote, time.Now(), int64(len(data2)), true, nil, f)

	obj, err := f.Put(in1, objInfo1)
	require.NoError(t, err)
	obj, err = f.NewObject(remote)
	require.NoError(t, err)
	err = obj.Update(in2, objInfo2)

	return obj
}

func readDataFromObj(t *testing.T, co fs.Object, offset, end int64, noLengthCheck bool) []byte {
	var reader io.ReadCloser
	var err error
	size := end - offset
	checkSample := make([]byte, size)

	reader, err = co.Open(&fs.SeekOption{Offset: offset})
	require.NoError(t, err)

	totalRead, err := io.ReadFull(reader, checkSample)
	if err == io.ErrUnexpectedEOF && noLengthCheck {
		err = nil
		checkSample = checkSample[:totalRead]
	}
	require.NoError(t, err)
	_ = reader.Close()
	if !noLengthCheck {
		require.Equal(t, int64(totalRead), size, "wrong data read size from file")
	}

	return checkSample
}

func cleanupFs(t *testing.T, f fs.Fs, b *cache.Persistent) {
	err := f.Features().Purge()
	require.NoError(t, err)
	cfs, err := getCacheFs(f)
	require.NoError(t, err)
	cfs.StopBackgroundRunners()
}

func newCacheFs(t *testing.T, remote, id string, purge bool, cfg map[string]string, flags map[string]string) (fs.Fs, *cache.Persistent) {
	fstest.Initialise()

	remoteExists := false
	for _, s := range fs.ConfigFileSections() {
		if s == remote {
			remoteExists = true
		}
	}

	// if the remote doesn't exist, create a new one with a local one for it
	// identify which is the cache remote (it can be wrapped by a crypt too)
	cacheRemote := remote
	if !remoteExists {
		localRemote := remote + "-local"
		fs.ConfigFileSet(localRemote, "type", "local")
		fs.ConfigFileSet(localRemote, "nounc", "true")
		fs.ConfigFileSet(remote, "type", "cache")
		fs.ConfigFileSet(remote, "remote", localRemote+":/var/tmp/"+localRemote)
	} else {
		remoteType := fs.ConfigFileGet(remote, "type", "")
		if remoteType == "" {
			t.Skipf("skipped due to invalid remote type for %v", remote)
			return nil, nil
		}
		if remoteType != "cache" {
			remoteRemote := fs.ConfigFileGet(remote, "remote", "")
			if remoteRemote == "" {
				t.Skipf("skipped due to invalid remote wrapper for %v", remote)
				return nil, nil
			}
			remoteRemoteParts := strings.Split(remoteRemote, ":")
			remoteWrapping := remoteRemoteParts[0]
			remoteType := fs.ConfigFileGet(remoteWrapping, "type", "")
			if remoteType != "cache" {
				t.Skipf("skipped due to invalid remote type for %v: '%v'", remoteWrapping, remoteType)
				return nil, nil
			}
			cacheRemote = remoteWrapping
		}
	}

	dbPath := filepath.Join(fs.CacheDir, "cache-backend", cacheRemote+".db")
	chunkPath := filepath.Join(fs.CacheDir, "cache-backend", cacheRemote)
	boltDb, err := cache.GetPersistent(dbPath, chunkPath, &cache.Features{PurgeDb: true})
	require.NoError(t, err)

	for k, v := range allCfgMap {
		if c, ok := cfg[k]; ok {
			fs.ConfigFileSet(cacheRemote, k, c)
		} else {
			fs.ConfigFileSet(cacheRemote, k, v)
		}
	}
	for k, v := range allFlagMap {
		if c, ok := flags[k]; ok {
			_ = flag.Set(k, c)
		} else {
			_ = flag.Set(k, v)
		}
	}

	// Instantiate root
	if purge {
		boltDb.PurgeTempUploads()
	}
	f, err := fs.NewFs(remote + ":" + id)
	require.NoError(t, err)
	if purge {
		_ = f.Features().Purge()
		require.NoError(t, err)
	}
	err = f.Mkdir("")
	require.NoError(t, err)

	return f, boltDb
}

//func mountFs(t *testing.T, f fs.Fs, mntPoint string) {
//	if runtime.GOOS == "windows" {
//		t.Skip("Skipping test cause on windows")
//		return
//	}
//
//	_ = flag.Set("debug-fuse", "false")
//
//	go func() {
//		mountlib.DebugFUSE = false
//		mountlib.AllowOther = true
//		mount.Mount(f, mntPoint)
//	}()
//
//	time.Sleep(time.Second * 3)
//}

//func unmountFs(t *testing.T, mntPoint string) {
//	var out []byte
//	var err error
//
//	if runtime.GOOS == "windows" {
//		t.Skip("Skipping test cause on windows")
//		return
//	} else if runtime.GOOS == "linux" {
//		out, err = exec.Command("fusermount", "-u", mntPoint).Output()
//	} else if runtime.GOOS == "darwin" {
//		out, err = exec.Command("diskutil", "unmount", mntPoint).Output()
//	}
//
//	t.Logf("Unmount output: %v", string(out))
//	require.NoError(t, err)
//}

func getCacheFs(f fs.Fs) (*cache.Fs, error) {
	cfs, ok := f.(*cache.Fs)
	if ok {
		return cfs, nil
	} else {
		if f.Features().UnWrap != nil {
			cfs, ok := f.Features().UnWrap().(*cache.Fs)
			if ok {
				return cfs, nil
			}
		}
	}

	return nil, fmt.Errorf("didn't found a cache fs")
}

var (
	_ fs.Fs = (*cache.Fs)(nil)
	_ fs.Fs = (*local.Fs)(nil)
)
