package backup

import (
	"encoding/gob"
	"github.com/mateusbraga/saveit/rsync"
	"io"
	"io/ioutil"
	"os"
)

func FullBackupReader(src io.Reader, dstSig io.Writer, dstFull io.Writer) error {
	sigWriter := rsync.NewSignatureWriter()
	multiwriter := io.MultiWriter(sigWriter, dstFull)

	_, err := io.Copy(multiwriter, src)
	if err != nil {
		return err
	}

	sig := sigWriter.Signature()
	enc := gob.NewEncoder(dstSig)
	err = enc.Encode(sig)
	if err != nil {
		return err
	}
	return nil
}

func IncrBackupReader(oldDataSignature rsync.Signature, src io.Reader, dstSig io.Writer, dstIncr io.Writer) error {
	sigWriter := rsync.NewSignatureWriter()
	teeReader := io.TeeReader(src, sigWriter)

	opc, errc := rsync.Delta(oldDataSignature, teeReader)
	encIncr := gob.NewEncoder(dstIncr)
	for op := range opc {
		err := encIncr.Encode(op)
		if err != nil {
			return err
		}
	}
	if err := <-errc; err != nil {
		return nil
	}

	sig := sigWriter.Signature()
	encSig := gob.NewEncoder(dstSig)
	err := encSig.Encode(sig)
	if err != nil {
		return err
	}
	return nil
}

func RestoreBackup(dst io.Writer, fullReader io.ReaderAt, diffReaders ...io.Reader) error {
	tempFile, err := ioutil.TempFile("", "restore-backup")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	lastFullReader := fullReader
	for i, diffReader := range diffReaders {
		opc, errc := readRsyncOps(diffReader)

		isLastReader := i == len(diffReaders)-1
		if isLastReader {
			err := rsync.Patch(lastFullReader, opc, errc, dst)
			if err != nil {
				return err
			}
		} else {
			err := rsync.Patch(lastFullReader, opc, errc, tempFile)
			if err != nil {
				return err
			}
		}
		lastFullReader = tempFile
	}

	return nil
}

func readRsyncOps(opReader io.Reader) (<-chan rsync.Op, <-chan error) {
	opc := make(chan rsync.Op, 20)
	errc := make(chan error, 1)

	go func() {
		defer close(opc)

		var op rsync.Op
		dec := gob.NewDecoder(opReader)
		for {
			err := dec.Decode(&op)
			if err == io.EOF {
				break
			}
			if err != nil {
				errc <- err
			}
			opc <- op
		}
		errc <- nil
	}()

	return opc, errc
}
