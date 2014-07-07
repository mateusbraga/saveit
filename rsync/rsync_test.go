package rsync

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
    original = "test-data/a.bmp"
    modified = "test-data/a-modified.bmp"
    //original = "test-data/b.txt"
    //modified = "test-data/b-modified.txt"
)

func TestRsync(t *testing.T) {
	originalFile, err := os.Open(original)
	if err != nil {
		t.Fatal(err)
	}

	originalBuf := bufio.NewReader(originalFile)
	sig, err := NewSignature(originalBuf)
	if err != nil {
		t.Fatal(err)
	}

	modifiedFile, err := os.Open(modified)
	if err != nil {
		t.Fatal(err)
	}
	modifiedBuf := bufio.NewReader(modifiedFile)

	opsChan, cerr := Delta(sig, modifiedBuf)

	originalFile2, err := os.Open(original)
	if err != nil {
		t.Fatal(err)
	}

	fi, _ := modifiedFile.Stat()
	patchedFile := bytes.NewBuffer(make([]byte, 0, fi.Size()))
	err = Patch(originalFile2, opsChan, cerr, patchedFile)
	if err != nil {
		t.Fatal(err)
	}

	modifiedFileData, err := ioutil.ReadFile(modified)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Compare(patchedFile.Bytes(), modifiedFileData) != 0 {
		t.Errorf("Failed expected %v bytes, got %v", fi.Size(), patchedFile.Len())
	}
}

func BenchmarkRsyncComplete(b *testing.B) {
	originalFile, err := ioutil.ReadFile(original)
	if err != nil {
		b.Fatal(err)
	}
	modifiedFile, err := ioutil.ReadFile(modified)
	if err != nil {
		b.Fatal(err)
	}
    originalFile2, err := os.Open(original)
    if err != nil {
        b.Fatal(err)
    }
    buf := bytes.NewBuffer(make([]byte, 0, len(modifiedFile)))
    b.ResetTimer()

	for n := 0; n < b.N; n++ {
        originalBuf := bytes.NewBuffer(originalFile)
		sig, err := NewSignature(originalBuf)
		if err != nil {
			b.Fatal(err)
		}

        modifiedBuf := bytes.NewBuffer(modifiedFile)
		opsChan, cerr := Delta(sig, modifiedBuf)

        buf.Reset()
		Patch(originalFile2, opsChan, cerr, buf)
	}
}

func BenchmarkRsyncNewSignature(b *testing.B) {
	originalFile, err := ioutil.ReadFile(original)
	if err != nil {
		b.Fatal(err)
	}
    b.ResetTimer()

	for n := 0; n < b.N; n++ {
        originalBuf := bytes.NewBuffer(originalFile)
		_, err := NewSignature(originalBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRsyncFindDifferences(b *testing.B) {
	originalFile, err := ioutil.ReadFile(original)
	if err != nil {
		b.Fatal(err)
	}
	modifiedFile, err := ioutil.ReadFile(modified)
	if err != nil {
		b.Fatal(err)
	}

    originalBuf := bytes.NewBuffer(originalFile)
    sig, err := NewSignature(originalBuf)
    if err != nil {
        b.Fatal(err)
    }
    b.ResetTimer()

    for n := 0; n < b.N; n++ {
        modifiedBuf := bytes.NewBuffer(modifiedFile)
        opsChan, _ := Delta(sig, modifiedBuf)
        for _ = range opsChan { }
    }
}

func Example() {
	originalFile, err := os.Open(original)
	if err != nil {
		log.Fatal(err)
	}
	originalBuf := bufio.NewReader(originalFile)

	sig, err := NewSignature(originalBuf)
	if err != nil {
		log.Fatal(err)
	}

	modifiedFile, err := os.Open(modified)
	if err != nil {
		log.Fatal(err)
	}
	modifiedBuf := bufio.NewReader(modifiedFile)

	opsChan, cerr := Delta(sig, modifiedBuf)

	originalFile2, err := os.Open(original)
	if err != nil {
		log.Fatal(err)
	}

	fi, _ := modifiedFile.Stat()
	patchedFile := bytes.NewBuffer(make([]byte, 0, fi.Size()))
	Patch(originalFile2, opsChan, cerr, patchedFile)
}
