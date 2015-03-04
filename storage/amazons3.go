package storage

import (
	"fmt"
	"io"
	"net/url"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

const maxBucketFiles = 1000

type AmazonS3Storage struct {
	Auth aws.Auth
	S3   *s3.S3
}

func CheckAmazonS3Url(rawurl string) (*url.URL, error) {
	parsedUrl, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if parsedUrl.Scheme != "s3+http" {
		return nil, fmt.Errorf("Amazon S3 scheme is s3+http, not %v", parsedUrl.Scheme)
	}
	return parsedUrl, nil
}

//func (stor AmazonS3Storage) List(dirpath string) ([]FileInfo, error) {
//parsedUrl, err := CheckAmazonS3Url(dirpath)
//if err != nil {
//return nil, err
//}

//bucket := stor.S3.Bucket(parsedUrl.Host)
//listResult, err := bucket.List(parsedUrl.Path, "/", "", maxBucketFiles)
//if err != nil {
//return nil, err
//}

//newFileInfos := make([]FileInfo, len(listResult.Contents))
//for i, key := range listResult.Contents {
//// TODO init fileinfo
//newFileInfos[i] = AmazonS3FileInfo{storageBackend: stor}
//}

//return newFileInfos, nil
//}

//func (stor AmazonS3Storage) Stat(filename string) (FileInfo, error) {
//fi, err := os.Stat(filename)
//if err != nil {
//return nil, err
//}

//return AmazonS3FileInfo{FileInfo: fi, storageBackend: stor}, nil
//}

func (stor AmazonS3Storage) Exist(filename string) (bool, error) {
	parsedUrl, err := CheckAmazonS3Url(filename)
	if err != nil {
		return false, err
	}

	bucket := stor.S3.Bucket(parsedUrl.Host)

	// Remove leading '/' from parsedUrl.Path
	listResult, err := bucket.List(parsedUrl.Path[1:], "/", "", 1)
	if err != nil {
		return false, err
	}

	exist := len(listResult.Contents) == 1
	return exist, nil
}

func (stor AmazonS3Storage) Reader(filename string) (io.ReadCloser, error) {
	parsedUrl, err := CheckAmazonS3Url(filename)
	if err != nil {
		return nil, err
	}

	bucket := stor.S3.Bucket(parsedUrl.Host)

	return bucket.GetReader(parsedUrl.Path)
}

type amazonS3Writer struct {
	bucket   *s3.Bucket
	filepath string
	data     *[]byte
}

func (writer amazonS3Writer) Write(buf []byte) (int, error) {
	*writer.data = append(*writer.data, buf...)
	return len(buf), nil
}

func (writer amazonS3Writer) Close() error {
	return writer.bucket.Put(writer.filepath, *writer.data, "", s3.BucketOwnerFull)
}

func (stor AmazonS3Storage) Writer(filename string) (io.WriteCloser, error) {
	parsedUrl, err := CheckAmazonS3Url(filename)
	if err != nil {
		return nil, err
	}

	bucket := stor.S3.Bucket(parsedUrl.Host)

	var dataBuf []byte
	writer := amazonS3Writer{bucket: bucket, filepath: parsedUrl.Path, data: &dataBuf}

	return writer, nil
}

func (stor AmazonS3Storage) Delete(filename string) error {
	parsedUrl, err := CheckAmazonS3Url(filename)
	if err != nil {
		return err
	}

	bucket := stor.S3.Bucket(parsedUrl.Host)

	return bucket.Del(parsedUrl.Path)
}

func (stor AmazonS3Storage) Status() error {
	return nil
}

//type AmazonS3FileInfo struct {
//name string
//size int64
//mode os.FileMode
//modTime string
//isDir bool

//storageBackend Storage
//}

//func (fsi AmazonS3FileInfo) Name() string {
//return fsi.Key.Key
//}

//func (fsi AmazonS3FileInfo) Size() int64 {
//return fsi.Key.Size
//}

//func (fsi AmazonS3FileInfo) Mode() os.FileMode {
////TODO complete
//return 0
//}

//func (fsi AmazonS3FileInfo) ModTime() time.Time {
//t, err := time.Parse("2006-01-02T15:04:05.999Z", fsi.Key.LastModified)
//if err != nil {
//log.Panicf("Could not parse LastModified time from s3 item: %v\n", err)
//}
//return t
//}

//func (fsi AmazonS3FileInfo) IsDir() bool {
////TODO complete
//return false
//}

//func (fsi AmazonS3FileInfo) Storage() string {
//return "s3+http"
//}

//func (fsi AmazonS3FileInfo) List() ([]FileInfo, error) {
//return fsi.Storage().List(fsi.Name())
//}

//func (fsi AmazonS3FileInfo) Reader() (io.ReadCloser, error) {
//return fsi.Storage().Reader(fsi.Name())
//}

//func (fsi AmazonS3FileInfo) Writer() (io.WriteCloser, error) {
//return fsi.Storage().Writer(fsi.Name())
//}

//func (fsi AmazonS3FileInfo) Delete() error {
//return fsi.Storage().Delete(fsi.Name())
//}
