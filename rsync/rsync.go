// Package rsync implements the rsync algorithm.
//
// rsync is defined in http://rsync.samba.org/tech_report/tech_report.html.
//
// To update a file from an old version to a new one using rsync involves creating a Signature of the old version, using it to create a Delta between the versions (Delta(Signature, newData)), and then applying the Delta to the old version (Patch(oldData, Delta)). This workflow allows for the files to be on different nodes, requiring the exchange of only the Signature and the Delta between the nodes.
package rsync

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log"
)

const (
    // Beware when changing BlockSize. BlockSize * a byte should never overflow an uint32, otherwise weakChecksum will misbehave.


	BlockSize = 1024 * 64
)

// Op opcodes.
const (
	// Index of matched block found on old file.
	BLOCK = iota
	// Raw data
	RAW_DATA
	// END_OF_FILE
	EOF
)

// Op describes an operation to build a file being patched/copied.
type Op struct {
	OpCode int
	Data   []byte
	Index  int
}

func (op Op) String() string {
	switch op.OpCode {
	case BLOCK:
		return fmt.Sprintf("BLOCK %v", op.Index)
	case RAW_DATA:
		return fmt.Sprintf("RAW_DATA %v bytes", len(op.Data))
    case EOF:
		return fmt.Sprintf("EOF sha1=%v", hex.EncodeToString(op.Data))
	default:
		return fmt.Sprintf("Invalid OpCode %v", op.OpCode)
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

// Delta returns a chan with the operations required to update the old data to be equal the new data. It closes the rsync.Op channel when it's done. If the newData Reader returns an error, the error is sent through the error channel before the rsync.Op channel is closed. See Patch.
func Delta(oldDataSignature Signature, newData io.Reader) (chan Op, chan error) {
	errc := make(chan error, 1)
	resultChan := make(chan Op, 20)

	go func() {
		defer close(resultChan)

		rollingWeakHash := newWeakChecksum()
        sha1Writer := sha1.New()
		dataBeingProcessed := bytes.NewBuffer(make([]byte, 0, BlockSize))
		multiwriter := io.MultiWriter(dataBeingProcessed, rollingWeakHash, sha1Writer)
		aByteSlice := make([]byte, 1)
		aBlockSizeSlice := make([]byte, BlockSize)

	startMatchSearchLoop:
		for {
			// rollingWeakHash and dataBeingProcessed is in Reset() state at this point. It means it will try to find a block match after reading a BlockSize at once, instead of byte by byte, as later
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
							newDataRsyncOp := Op{
								OpCode: RAW_DATA,
								Data:   dataToSend,
							}
							resultChan <- newDataRsyncOp
						}
						newBlockRsyncOp := Op{
							OpCode: BLOCK,
							Index:  index,
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
					newDataRsyncOp := Op{
						OpCode: RAW_DATA,
						Data:   dataToSend,
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
			newDataRsyncOp := Op{
				OpCode: RAW_DATA,
				Data:   dataToSend,
			}
			resultChan <- newDataRsyncOp
		}
		//send EOF op
        EOFOp := Op{
            OpCode: EOF,
            Data:   sha1Writer.Sum(nil),
        }
        resultChan <- EOFOp

		errc <- nil
	}()

	return resultChan, errc
}

// Patch applies the operations from opsChan with oldData and writes resulting data to newData. It also makes sure that the resulting data sha1 hash matches the original data sha1 hash, returning an error otherwise. In case of error, the newData Writer may have incomplete data. See Delta.
func Patch(oldData io.ReaderAt, opsChan <-chan Op, errc <-chan error, newData io.Writer) error {
    sha1Writer := sha1.New()
    multiwriter := io.MultiWriter(newData, sha1Writer)

	buf := make([]byte, BlockSize)
	for op := range opsChan {
        //log.Println(op)
		switch op.OpCode {
		case BLOCK:
			n, err := oldData.ReadAt(buf, int64(op.Index*BlockSize))
			if err != nil {
				if err != io.EOF {
					return err
				}
			}
			_, err = multiwriter.Write(buf[:n])
			if err != nil {
				return err
			}
		case RAW_DATA:
			_, err := multiwriter.Write(op.Data)
			if err != nil {
				return err
			}
		case EOF:
            h := sha1Writer.Sum(nil)
            if bytes.Compare(h, op.Data) != 0 {
                return fmt.Errorf("rsync: hash of data created does not match hash of original data")
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
