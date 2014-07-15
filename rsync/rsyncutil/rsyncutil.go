package rsyncutil

import (
	"bufio"
	"encoding/gob"
	"github.com/mateusbraga/saveit/rsync"
	"os"
)

func CreateSignatureFile(signatureFile string, file string) error {
	fp, err := os.Open(file)
	if err != nil {
		return err
	}
	defer fp.Close()
	fileBuffer := bufio.NewReader(fp)

	sig, err := rsync.NewSignature(fileBuffer)
	if err != nil {
		return err
	}

	sfp, err := os.Create(signatureFile)
	if err != nil {
		return err
	}
	defer sfp.Close()
	signatureBuffer := bufio.NewWriter(sfp)
	defer signatureBuffer.Flush()

	enc := gob.NewEncoder(signatureBuffer)
	err = enc.Encode(sig)
	if err != nil {
		return err
	}

	return nil
}

func CreateDeltaFile(deltaFile string, signatureOldFile string, newFile string) error {
	sfp, err := os.Open(signatureOldFile)
	if err != nil {
		return err
	}
	defer sfp.Close()
	signatureBuffer := bufio.NewReader(sfp)

	dec := gob.NewDecoder(signatureBuffer)

	var sig rsync.Signature
	dec.Decode(&sig)

	fp, err := os.Open(newFile)
	if err != nil {
		return err
	}
	defer fp.Close()
	fileBuffer := bufio.NewReader(fp)

	opc, errc := rsync.Delta(sig, fileBuffer)

	ops, err := DeltaChanToArray(opc, errc)
	if err != nil {
		return nil
	}

	dfp, err := os.Create(deltaFile)
	if err != nil {
		return err
	}
	defer dfp.Close()
	deltaBuffer := bufio.NewWriter(dfp)
	defer deltaBuffer.Flush()

	enc := gob.NewEncoder(deltaBuffer)
	err = enc.Encode(ops)
	if err != nil {
		return err
	}

	return nil
}

func DeltaChanToArray(opc <-chan rsync.Op, errc <-chan error) ([]rsync.Op, error) {
	var result []rsync.Op
	for op := range opc {
		result = append(result, op)
	}
	if err := <-errc; err != nil {
		return nil, err
	}
	return result, nil
}

func deltaArrayToChan(ops []rsync.Op) chan rsync.Op {
	opc := make(chan rsync.Op)
	go func() {
	    defer close(opc)
		for _, op := range ops {
			opc <- op
		}
	}()
	return opc
}

func PatchFile(newFile string, oldFile string, deltaFile string) error {
	dfp, err := os.Open(deltaFile)
	if err != nil {
		return err
	}
	defer dfp.Close()
	deltaBuffer := bufio.NewReader(dfp)

	dec := gob.NewDecoder(deltaBuffer)

	var ops []rsync.Op
	dec.Decode(&ops)
    opc := deltaArrayToChan(ops)

	oldFp, err := os.Open(oldFile)
	if err != nil {
		return err
	}
	defer oldFp.Close()

	newFp, err := os.Create(newFile)
	if err != nil {
		return err
	}
	defer newFp.Close()
	newFileBuffer := bufio.NewWriter(newFp)
	defer newFileBuffer.Flush()

    closedErrChan := make(chan error)
    close(closedErrChan)
	err = rsync.Patch(oldFp, opc, closedErrChan, newFileBuffer)
	if err != nil {
        return err
	}

	return nil
}
