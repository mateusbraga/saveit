package storage

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestNewDstFile(t *testing.T) {
	// SETUP
	tempDirName, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Could not create tempdir: %v", err)
	}
	defer os.RemoveAll(tempDirName)

	// get test file
	srcFilename := path.Join(tempDirName, "test1")
	size := 50
	data := createFakeData(size)

	err = ioutil.WriteFile(srcFilename, data, 0666)
	if err != nil {
		t.Fatalf("Failed to create dumb test file: %v", err)
	}

	dstFilename := path.Join(tempDirName, "test2")

	Copy(srcFilename, dstFilename)

	srcData, err := ioutil.ReadFile(srcFilename)
	if err != nil {
		t.Fatalf("Failed to read srcFilename %v: %v", srcFilename, err)
	}
	dstData, err := ioutil.ReadFile(dstFilename)
	if err != nil {
		t.Fatalf("Failed to read dstFilename %v: %v", dstFilename, err)
	}

	if !bytes.Equal(srcData, dstData) {
		t.Errorf("Files are not equal after Copy")
	}
}
