package main

import (
    "flag"
    "github.com/mateusbraga/saveit/rsync/rsyncutil"
    "log"
)

func main() {
    flag.Parse()

    switch flag.Arg(0) {
    case "signature":
        switch flag.NArg() {
        case 3:
            rsyncutil.CreateSignatureFile(flag.Arg(2), flag.Arg(1))
        default:
            log.Fatal("Usage: saveit-rdiff signature BASIS SIGNATURE")
        }
    case "delta":
        switch flag.NArg() {
        case 4:
            rsyncutil.CreateDeltaFile(flag.Arg(3), flag.Arg(1), flag.Arg(2))
        default:
            log.Fatal("Usage: saveit-rdiff delta SIGNATURE NEWFILE DELTA")
        }
    case "patch":
        switch flag.NArg() {
        case 4:
            rsyncutil.PatchFile(flag.Arg(3), flag.Arg(1), flag.Arg(2))
        default:
            log.Fatal("Usage: saveit-rdiff patch BASIS DELTA NEWFILE")
        }
    default:
        log.Fatal("You must specify one of the following action: 'signature', 'delta', or 'patch'.")
    }
}
