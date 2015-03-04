package storage

import (
	"io"
	"io/ioutil"
	"os"
	"time"
)

type FilesystemStorage struct{}

func (fs FilesystemStorage) List(dirpath string) ([]FileInfo, error) {
	fileInfos, err := ioutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}

	newFileInfos := make([]FileInfo, len(fileInfos))
	for i, fileInfo := range fileInfos {
		newFileInfos[i] = FilesystemFileInfo{FileInfo: fileInfo, storageBackend: fs}
	}

	return newFileInfos, nil
}

func (fs FilesystemStorage) Reader(filename string) (io.ReadCloser, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs FilesystemStorage) Writer(filename string) (io.WriteCloser, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (fs FilesystemStorage) Delete(filename string) error {
	err := os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}

func (fs FilesystemStorage) Status() error {
	return nil
}

func (fs FilesystemStorage) Stat(filename string) (FileInfo, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	return FilesystemFileInfo{FileInfo: fi, storageBackend: fs}, nil
}

func (fs FilesystemStorage) Exist(filename string) (bool, error) {
	_, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

type FilesystemFileInfo struct {
	os.FileInfo
	storageBackend Storage
}

func (fsi FilesystemFileInfo) Name() string {
	return fsi.FileInfo.Name()
}

func (fsi FilesystemFileInfo) Size() int64 {
	return fsi.FileInfo.Size()
}

func (fsi FilesystemFileInfo) Mode() os.FileMode {
	return fsi.FileInfo.Mode()
}

func (fsi FilesystemFileInfo) ModTime() time.Time {
	return fsi.FileInfo.ModTime()
}

func (fsi FilesystemFileInfo) IsDir() bool {
	return fsi.FileInfo.IsDir()
}

func (fsi FilesystemFileInfo) Storage() string {
	return "localfs"
}

//func (fsi FilesystemFileInfo) List() ([]FileInfo, error) {
//return fsi.Storage().List(fsi.Name())
//}

//func (fsi FilesystemFileInfo) Reader() (io.ReadCloser, error) {
//return fsi.Storage().Reader(fsi.Name())
//}

//func (fsi FilesystemFileInfo) Writer() (io.WriteCloser, error) {
//return fsi.Storage().Writer(fsi.Name())
//}

//func (fsi FilesystemFileInfo) Delete() error {
//return fsi.Storage().Delete(fsi.Name())
//}
