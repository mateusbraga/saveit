package main

import (
	"log"
	"os"

	"github.com/mateusbraga/saveit/storage"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: backup src dst\n")
	}

	srcRawUrl := os.Args[1]
	dstRawUrl := os.Args[2]

	//log.Printf("Src: %v\n", srcRawUrl)
	//log.Printf("Dst: %v\n", dstRawUrl)
	err := storage.Copy(srcRawUrl, dstRawUrl)
	if err != nil {
		log.Println(err)
	}
}
