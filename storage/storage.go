package storage

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

// Storage is the abstraction of different storages.
// It hides how the data will be sent from the running machine to the storage, and the storage to the running machine (i.e. to compress?, to encrypt?, to authenticate? can the storage execute a part of the algorithm? (i.e. rsync server)).
// It hides how the data will be stored (in per storage terms, since the user of the Storage abstraction can change the data representation for all storages).
type Storage interface {
	//List(stop <-chan struct{}, dirpath string) (<-chan FileInfo, error)
	//Stat(filename string) (FileInfo, error)

	Delete(filename string) error

	// Reader, Writer and Exist are needed to perform Copy operation.
	Reader(filename string) (io.ReadCloser, error)
	Writer(filename string) (io.WriteCloser, error)
	Exist(filename string) (bool, error)

	// Status is for checking if Storage is operating.
	Status() error
}

type FileInfo interface {
	os.FileInfo
}

// Copy copies a storage/directory/file specified in srcRawUrl to dstRawUrl.
func Copy(srcRawUrl string, dstRawUrl string) error {
	srcUrl, err := url.Parse(srcRawUrl)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to parse source url: %v\n", err))
	}

	dstUrl, err := url.Parse(dstRawUrl)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to parse destination url: %v\n", err))
	}
	srcStorage := GetStorage(srcUrl.Scheme)
	dstStorage := GetStorage(dstUrl.Scheme)

	exist, err := srcStorage.Exist(srcRawUrl)
	if err != nil {
		log.Fatalln(err)
		return fmt.Errorf("Failed to check if source file '%v' exists: %v\n", srcUrl, err)
	}
	if !exist {
		return fmt.Errorf("Source file '%v' do not exists: %v\n", srcUrl, err)
	}

	log.Printf("Copying data from %v to %v\n", srcRawUrl, dstRawUrl)

	err = doCopy(srcRawUrl, srcStorage, dstRawUrl, dstStorage)
	if err != nil {
		return err
	}

	return nil
}

// GetStorage returns the Storage with the scheme.
func GetStorage(scheme string) Storage {
	switch scheme {
	case "":
		return FilesystemStorage{}
	case "s3+http":
		amazonS3Storage := AmazonS3Storage{}
		auth, err := aws.EnvAuth()
		if err != nil {
			log.Fatalln("Failed to authenticate with aws:", err)
		}
		amazonS3Storage.Auth = auth
		amazonS3Storage.S3 = s3.New(amazonS3Storage.Auth, aws.USEast)
		return amazonS3Storage
	}

	log.Fatalln("Failed to find destination storage with scheme %v", scheme)
	return nil
}

// doCopy copies from source to destination, using the Storage abstraction.
// It allows to change the representation of what will be stored on the other side (i.e. to encrypt, to sign)
func doCopy(srcpath string, srcStorage Storage, dstpath string, dstStorage Storage) error {
	err := srcStorage.Status()
	if err != nil {
		return fmt.Errorf("Source Storage is not operating: %v", err)
	}

	err = dstStorage.Status()
	if err != nil {
		return fmt.Errorf("Destination is not operating: %v", err)
	}

	srcReader, err := srcStorage.Reader(srcpath)
	if err != nil {
		return fmt.Errorf("Failed to get source reader: %v", err)
	}
	defer closeIO(srcReader)

	dstWriter, err := dstStorage.Writer(dstpath)
	if err != nil {
		return fmt.Errorf("Failed to get destination writer: %v", err)
	}
	defer closeIO(dstWriter)

	_, err = io.Copy(dstWriter, srcReader)
	if err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	return nil
}

// closeIO closes the io and exit if any error occurs.
func closeIO(rw io.Closer) {
	err := rw.Close()
	if err != nil {
		log.Fatalf("Failed to close io: %v", err)
	}
}
