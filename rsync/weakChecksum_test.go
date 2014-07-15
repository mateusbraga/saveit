package rsync

import (
	"crypto/rand"
	"io"
	"io/ioutil"
	"log"
	"math"
	"testing"
)

func TestRollWeakChecksum(t *testing.T) {
	modifiedFileData, err := ioutil.ReadFile(modified)
	if err != nil {
		t.Fatal(err)
	}

	if len(modifiedFileData) < 2*BlockSize+2 {
		t.Skip("skipped, ", modified, "is too small")
	}

	d := newWeakChecksum()
	d.Write(modifiedFileData[:BlockSize])

	for i := 0; i < BlockSize+1; i++ {
		d.Write(modifiedFileData[BlockSize+i : BlockSize+i+1])
		if d1, digest1 := d.Sum32(), getWeakChecksum(modifiedFileData[i+1:BlockSize+i+1]); d1 != digest1 {
			t.Fatalf("expected (%v, %v), got (%v,%v) when i=%v", digest1>>16, digest1&0xffff, d1>>16, d1&0xffff, i)

		}
	}
}

func TestOverflow(t *testing.T) {
    result := uint64(BlockSize) * uint64(math.MaxUint8)
    if result > uint64(math.MaxUint32){
        t.Fatal("BlockSize * MaxUint8 can overflow, weakChecksum behavior will be arbitrary")
    }
}

func createFakeData(size int) []byte {
	data := make([]byte, size)

	n, err := io.ReadFull(rand.Reader, data)
	if n != len(data) || err != nil {
		log.Fatalln("error to generate data:", err)
	}
	return data
}
