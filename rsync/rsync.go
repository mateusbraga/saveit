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
	BlockSize = 1024 * 64
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

// Signature contains the block checksums used to find differences between two files
type Signature map[uint32]map[[md5.Size]byte]int

// NewSignature creates the Signature of the data.
func NewSignature(data io.Reader) (Signature, error) {
	weakHash := newWeakChecksum()
	aBlockSizeSlice := make([]byte, BlockSize)

	sig := make(map[uint32]map[[md5.Size]byte]int)
	var currentIndex int
	for {
		n, err := readFullAndCopyN(weakHash, data, aBlockSizeSlice)
		if err != nil && err != io.EOF {
			return nil, err
		}

		weak := weakHash.Sum32()
        strong := md5.Sum(aBlockSizeSlice[0:n])
		m, ok := sig[weak]
		if !ok {
			m = make(map[[md5.Size]byte]int)
			sig[weak] = m
		}
		_, ok2 := m[strong]
		if !ok2 {
			m[strong] = currentIndex
		}

		//sig = append(sig, bc)
		//strongHash.Reset()
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

		rollingWeakHash := newWeakChecksum()
		dataBeingProcessed := bytes.NewBuffer(make([]byte, 0, BlockSize))
		multiwriter := io.MultiWriter(dataBeingProcessed, rollingWeakHash)
		aByteSlice := make([]byte, 1)
		aBlockSizeSlice := make([]byte, BlockSize)

	startMatchSearchLoop:
		for {
			//rollingWeakHash and dataBeingProcessed is in Reset() state

			_, err := readFullAndCopyN(multiwriter, newData, aBlockSizeSlice)
			if err == io.EOF {
				// could not form a block, leave this big loop and send remaining data
				break startMatchSearchLoop
			}
			if err != nil {
				errc <- err
				return
			}

			weak := rollingWeakHash.Sum32()
			possibleBlocks, found := oldDataSignature[weak]
			for {
				if found {
					// found weakChecksum match, check strongChecksum
					buf := dataBeingProcessed.Bytes()
					block := buf[len(buf)-BlockSize : len(buf)]
					strong := md5.Sum(block)
					index, found2 := possibleBlocks[strong]
					if found2 {
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
							index:  index,
						}
						resultChan <- newBlockRsyncOp

						rollingWeakHash.Reset()
						dataBeingProcessed.Reset()
						// continue trying to find matches for the following blocks
						continue startMatchSearchLoop
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
				_, err := readFullAndCopyN(multiwriter, newData, aByteSlice)
				if err == io.EOF {
					// could not read another byte to form a block, exit big loop and send remaining data
					break startMatchSearchLoop
				}
				if err != nil {
					errc <- err
					return
				}

				weak := rollingWeakHash.Sum32()
				possibleBlocks, found = oldDataSignature[weak]
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
		//log.Println(op)
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

// readFullAndCopyN does both what io.CopyN and io.ReadFull does at the same time. In case of EOF, it returns io.EOF instead of io.ErrUnexpectedEOF. If err == nil || err == io.EOF, everything written to dst is in buf[0:written]. On return, written == len(buf), if and only if err == nil.
func readFullAndCopyN(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	for {
		nr, er := src.Read(buf[written:])
		if nr > 0 {
			nw, ew := dst.Write(buf[written : written+int64(nr)])
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
