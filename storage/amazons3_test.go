package storage

import (
	"io"
	"testing"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

const testBucket = "armazen"

func TestAmazonS3AsAStorage(t *testing.T) {
	// SETUP
	s3Storage := AmazonS3Storage{}
	auth, err := aws.EnvAuth()
	if err != nil {
		t.Fatalf("Failed to authenticate with aws: %v", err)
	}
	s3Storage.Auth = auth
	s3Storage.S3 = s3.New(s3Storage.Auth, aws.USEast)

	// get test file
	size := 50
	data := createFakeData(size)
	filename := "s3+http://" + testBucket + "/testing_moveon"

	// test Storage.Status
	if err := s3Storage.Status(); err != nil {
		t.Fatalf("AmazonS3Storage is not operating: %v", err)
	}

	// test Storage.Exist
	// file should not already exist
	exist, err := s3Storage.Exist(filename)
	if err != nil {
		t.Fatalf("Could not ask if file exist: %v", err)
	}
	if exist {
		t.Errorf("Testing file already exist in s3!")
	}

	// test Storage.Writer
	writer, err := s3Storage.Writer(filename)
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
	exist, err = s3Storage.Exist(filename)
	if err != nil {
		t.Fatalf("Could not ask if file exist: %v", err)
	}
	if !exist {
		t.Errorf("File should exist!")
	}

	// test Storage.Reader
	// let's read it
	reader, err := s3Storage.Reader(filename)
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
	//fileInfo, err := s3Storage.Stat(filename)
	//if err != nil {
	//t.Fatalf("Could not stat file %v: %v", filename, err)
	//}
	//if fileInfo.Size() != int64(size) {
	//t.Errorf("Expected fileInfo.Size() == %v, got %v", size, fileInfo.Size())
	//}

	// test Storage.List
	// let's see if it is listed
	//fileInfos, err := s3Storage.List(tempDirName)
	//if err != nil {
	//t.Errorf("Could not list %v: %v", tempDirName, err)
	//}
	//if len(fileInfos) != 1 {
	//t.Errorf("Expected to find %v files, got %v: %v", 1, len(fileInfos), fileInfos)
	//}

	// test Storage.Delete
	// let's delete it
	err = s3Storage.Delete(filename)
	if err != nil {
		t.Errorf("Could not delete %v: %v", filename, err)
	}

	// let's check if it is not listed anymore
	//fileInfos, err = s3Storage.List(tempDirName)
	//if err != nil {
	//t.Errorf("Could not list %v: %v", tempDirName, err)
	//}
	//if len(fileInfos) != 0 {
	//t.Errorf("Expected to find %v files, got %v: %v", 0, len(fileInfos), fileInfos)
	//}
}
