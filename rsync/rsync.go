// Package rsync implements the rsync algorithm.
//
// rsync is defined in http://rsync.samba.org/tech_report/tech_report.html.
package rsync

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log"
)

const (
	BlockSize = 1024*64
)

// RsyncOp opcodes.
const (
	// Index of matched block found on old file.
	BLOCK = iota
	// Raw data
	RAW_DATA
)

// RsyncOp describes an operation to build a file being patched/copied.
type RsyncOp struct {
	opCode int
	data   []byte
	index  int
}

func (op RsyncOp) String() string {
	switch op.opCode {
	case BLOCK:
		return fmt.Sprintf("BLOCK %v", op.index)
	case RAW_DATA:
		return fmt.Sprintf("RAW_DATA %v bytes", len(op.data))
    default:
		return fmt.Sprintf("Invalid opCode %v", op.opCode)
	}
}

type blockChecksum struct {
	index          int
	strongChecksum []byte
	weakChecksum   uint32
}

// Signature contains the block checksums used to find differences between two files
type Signature []*blockChecksum

// NewSignature creates the Signature of the data.
func NewSignature(data io.Reader) (Signature, error) {
	strongHash := md5.New()
	weakHash := newWeakChecksum()
	bothHash := io.MultiWriter(strongHash, weakHash)
	aBlockSizeSlice := make([]byte, BlockSize)

	var sig []*blockChecksum
	var currentIndex int
	for {
		_, err := copyWithSlice(bothHash, data, aBlockSizeSlice)
		if err != nil && err != io.EOF {
			return nil, err
		}

		bc := &blockChecksum{
			index:          currentIndex,
			strongChecksum: strongHash.Sum(nil),
			weakChecksum:   weakHash.Sum32(),
		}
		sig = append(sig, bc)
		strongHash.Reset()
		weakHash.Reset()

		if err == io.EOF {
			break
		}
		currentIndex++
	}

	return sig, nil
}

// Delta returns a chan with the operations required to update the old data to be equal the new data. It closes the RsyncOp channel when it's done. If the newData Reader returns an error, the error is sent through the error channel and then the RsyncOp channel is closed.
func Delta(oldDataSignature Signature, newData io.Reader) (chan RsyncOp, chan error) {
	errc := make(chan error, 1)
	resultChan := make(chan RsyncOp, 20)

	go func() {
		defer close(resultChan)
		weakChecksumMap := make(map[uint32][]*blockChecksum)
		for _, bc := range oldDataSignature {
			weakChecksumMap[bc.weakChecksum] = append(weakChecksumMap[bc.weakChecksum], bc)
			//log.Println("hi", bc.weakChecksum)
		}

		digestHash := newWeakChecksum()
		dataBeingProcessed := bytes.NewBuffer(make([]byte, 0, BlockSize))
		multiwriter := io.MultiWriter(dataBeingProcessed, digestHash)
		aByteSlice := make([]byte, 1)
		aBlockSizeSlice := make([]byte, BlockSize)

	startMatchSearchLoop:
		for {
			//digestHash and dataBeingProcessed is in Reset() state

			_, err := copyWithSlice(multiwriter, newData, aBlockSizeSlice)
			if err == io.EOF {
				// could not form a block, leave this big loop and send remaining data
				break startMatchSearchLoop
			}
			if err != nil {
				errc <- err
				return
			}

			weak := digestHash.Sum32()
			possibleBlocks, found := weakChecksumMap[weak]
			for {
				if found {
					// found weakChecksum match, check strongChecksum
					buf := dataBeingProcessed.Bytes()
					block := buf[len(buf)-BlockSize : len(buf)]
					strong := md5.Sum(block)
					for _, bc := range possibleBlocks {
						if bytes.Compare(bc.strongChecksum, strong[:]) == 0 {
							// found strongChecksum match, send unmatched data then block index that matched
							numberOfBytesNotMatched := len(buf) - BlockSize
							if numberOfBytesNotMatched > 0 {
								dataToSend := make([]byte, numberOfBytesNotMatched)
								copy(dataToSend, buf[0:numberOfBytesNotMatched])
								newDataRsyncOp := RsyncOp{
									opCode: RAW_DATA,
									data:   dataToSend,
								}
								resultChan <- newDataRsyncOp
							}
							newBlockRsyncOp := RsyncOp{
								opCode: BLOCK,
								index:  bc.index,
							}
							resultChan <- newBlockRsyncOp

							digestHash.Reset()
							dataBeingProcessed.Reset()
							// continue trying to find matches for the following blocks
							continue startMatchSearchLoop
						}
					}
					log.Println("false negative")
					// false negative of weakChecksum
					// continue trying to find a weakChecksum match byte by byte
				}

				// send partial data if a BlockSize of data did not match
				if dataBeingProcessed.Len() >= 2*BlockSize {
					dataToSend := make([]byte, BlockSize)
					// error here is impossible, we just asked dataBeingProcessed.Len()
					io.ReadFull(dataBeingProcessed, dataToSend)
					newDataRsyncOp := RsyncOp{
						opCode: RAW_DATA,
						data:   dataToSend,
					}
					resultChan <- newDataRsyncOp
				}

				// incremental search for match (will read one byte per time)
				_, err := copyWithSlice(multiwriter, newData, aByteSlice)
				if err == io.EOF {
					// could not read another byte to form a block, exit big loop and send remaining data
					break startMatchSearchLoop
				}
				if err != nil {
					errc <- err
					return
				}

				weak := digestHash.Sum32()
				possibleBlocks, found = weakChecksumMap[weak]
			}
		}

		// send remaining data
		if dataBeingProcessed.Len() > 0 {
			dataToSend := make([]byte, dataBeingProcessed.Len())
			copy(dataToSend, dataBeingProcessed.Bytes())
			newDataRsyncOp := RsyncOp{
				opCode: RAW_DATA,
				data:   dataToSend,
			}
			resultChan <- newDataRsyncOp
		}
		errc <- nil
	}()

	return resultChan, errc
}

// Patch applies the operations from opsChan with oldData and writes resulting data to newData. See Delta.
func Patch(oldData io.ReaderAt, opsChan <-chan RsyncOp, errc <-chan error, newData io.Writer) error {
	buf := make([]byte, BlockSize)
	for op := range opsChan {
		log.Println(op)
		switch op.opCode {
		case BLOCK:
			n, err := oldData.ReadAt(buf, int64(op.index*BlockSize))
			if err != nil {
				if err != io.EOF {
					return err
				}
			}
			_, err = newData.Write(buf[:n])
			if err != nil {
				return err
			}
		case RAW_DATA:
			_, err := newData.Write(op.data)
			if err != nil {
				return err
			}
		}
	}
	if err := <-errc; err != nil {
		return err
	}
	return nil
}

// copyWithSlice is like io.CopyN but using len(buf) as N and no buffer allocation is necessary.
func copyWithSlice(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	total := int64(len(buf))
	for {
		nr, er := src.Read(buf[0 : total-written])
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			err = io.EOF
			break
		}
		if er != nil {
			err = er
			break
		}
		if written == int64(len(buf)) {
			break
		}
	}
	return written, err
}
