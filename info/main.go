package main

import (
	"flag"
	"log"
	"os"
	"time"

	sstable "github.com/akmistry/simple-sstable"
)

var (
	file = flag.String("file", "", "File to inspect")
)

func commonPrefix(a, b []byte) int {
	maxLen := len(a)
	if len(b) < maxLen {
		maxLen = len(b)
	}
	for i := 0; i < maxLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return maxLen
}

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

	startTime := time.Now()
	table, err := sstable.Load(f)
	if err != nil {
		log.Println("Error loading sstable:", err)
		return
	}
	log.Println("Table load time:", time.Now().Sub(startTime))

	stats := table.Stats()
	log.Println("Header size:", stats.HeaderSize)
	log.Println("Index size:", stats.IndexSize)
	log.Println("Num keys:", stats.NumKeys)
	log.Println("Keys size:", stats.KeysSize)
	log.Println("Values size:", stats.ValuesSize)
	log.Println("[]Keys:")
	keys := table.Keys()

	var prev []byte
	prefixSaved := 0
	for _, k := range keys {
		l, _, _ := table.GetInfo(k)
		log.Println(k, "\t", l)

		common := commonPrefix(prev, k)
		if common > 1 {
			prefixSaved += common - 1
		}
		prev = k
	}
	log.Println("Bytes saved if prefix encoded keys:", prefixSaved)
}
