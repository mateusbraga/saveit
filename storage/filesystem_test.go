package storage

import (
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"
)

func TestFilesystemAsAStorage(t *testing.T) {
	var fs FilesystemStorage

	// SETUP
	// get temp dir
	tempDirName, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Could not create tempdir: %v", err)
	}
	defer os.RemoveAll(tempDirName)

	// get test file
	filename := path.Join(tempDirName, "test1")
	size := 50
	data := createFakeData(size)

	// test Storage.Status
	if err := fs.Status(); err != nil {
		t.Fatalf("FilesystemStorage is not operating: %v", err)
	}

	// test Storage.Exist
	// file should not already exist
	exist, err := fs.Exist(filename)
	if err != nil {
		t.Fatalf("Could not ask if file exist: %v", err)
	}
	if exist {
		t.Errorf("File already exist!")
	}

	// test Storage.Writer
	writer, err := fs.Writer(filename)
	if err != nil {
		t.Fatalf("Could not open file to write %v: %v", filename, err)
	}

	// write first time to test file
	n, err := writer.Write(data)
	if err != nil {
		t.Errorf("Could not write to file %v: %v", filename, err)
	}
	if n != size {
		t.Errorf("Expected %v, wrote only %v bytes", size, n)
	}

	// test writer.Close
	err = writer.Close()
	if err != nil {
		t.Errorf("Could not close writer")
	}

	// now it should exist
	exist, err = fs.Exist(filename)
	if err != nil {
		t.Fatalf("Could not ask if file exist: %v", err)
	}
	if !exist {
		t.Errorf("File should exist!")
	}

	// test Storage.Reader
	// let's read it
	reader, err := fs.Reader(filename)
	if err != nil {
		t.Fatalf("Could not open file to read %v: %v", filename, err)
	}

	buf := make([]byte, size)
	n, err = io.ReadFull(reader, buf)
	if err != nil {
		t.Errorf("Expected %v bytes to be read from %v, got %v", size, filename, n)
	}

	// test reader.Close
	err = reader.Close()
	if err != nil {
		t.Errorf("Could not close reader")
	}

	// test Storage.Stat
	fileInfo, err := fs.Stat(filename)
	if err != nil {
		t.Fatalf("Could not stat file %v: %v", filename, err)
	}
	if fileInfo.Size() != int64(size) {
		t.Errorf("Expected fileInfo.Size() == %v, got %v", size, fileInfo.Size())
	}

	// test Storage.List
	// let's see if it is listed
	fileInfos, err := fs.List(tempDirName)
	if err != nil {
		t.Errorf("Could not list %v: %v", tempDirName, err)
	}
	if len(fileInfos) != 1 {
		t.Errorf("Expected to find %v files, got %v: %v", 1, len(fileInfos), fileInfos)
	}

	// test Storage.Delete
	// let's delete it
	err = fs.Delete(filename)
	if err != nil {
		t.Errorf("Could not delete %v: %v", filename, err)
	}

	// let's check if it is not listed anymore
	fileInfos, err = fs.List(tempDirName)
	if err != nil {
		t.Errorf("Could not list %v: %v", tempDirName, err)
	}
	if len(fileInfos) != 0 {
		t.Errorf("Expected to find %v files, got %v: %v", 0, len(fileInfos), fileInfos)
	}
}

func createFakeData(size int) []byte {
	data := make([]byte, size)

	n, err := io.ReadFull(rand.Reader, data)
	if n != len(data) || err != nil {
		log.Fatalln("error to generate data:", err)
	}
	return data
}
