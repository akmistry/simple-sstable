package main

import (
	"flag"
	"log"
	"os"

	sstable "github.com/akmistry/simple-sstable"
)

var (
	file = flag.String("file", "", "File to inspect")
)

func main() {
	flag.Parse()

	if *file == "" {
		log.Println("-file argument must be provided")
		return
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}

	table, err := sstable.Load(f)
	if err != nil {
		log.Println("Error loading sstable:", err)
		return
	}

	log.Println("NumKeys:", table.NumKeys())
	log.Println("DataSize:", table.DataSize())
	log.Println("[]Keys:")
	keys := table.Keys()
	for _, k := range keys {
		l, _, _ := table.GetInfo(k)
		log.Println(k, "\t", l)
	}
}
