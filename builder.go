package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"

	"github.com/golang/protobuf/proto"

	pb "github.com/akmistry/simple-sstable/proto"
)

type keyLengthPair struct {
	key    []byte
	length uint32
}

type Builder struct {
	w  io.Writer
	vf ValueWriter

	indexBuf proto.Buffer
	keys     []keyLengthPair
	valuePos uint64

	started bool
	prev    []byte
}

type ValueWriter func(key []byte, w io.Writer) (int, error)

const (
	MaxKeyLength   = 256
	MaxValueLength = 1024 * 1024 * 1024 // 1GiB
)

func NewBuilder(w io.Writer, vf ValueWriter) *Builder {
	return &Builder{w: w, vf: vf}
}

func (b *Builder) Add(key []byte, valueLength uint32, meta []byte) {
	if !b.started {
		b.started = true
	} else if bytes.Compare(b.prev, key) != -1 {
		log.Panicf("Key %d is before previous %d", key, b.prev)
	}

	if len(key) > MaxKeyLength {
		log.Panicf("Key length %d > 256", len(key))
	} else if valueLength > MaxValueLength {
		log.Panicf("Value length %d > 1GiB", valueLength)
	}

	keyDup := dup(key)
	b.prev = keyDup

	var entry pb.IndexEntry
	entry.Key = key
	entry.Offset = b.valuePos
	b.valuePos += uint64(valueLength)
	entry.Length = valueLength
	entry.Extra = meta
	b.keys = append(b.keys, keyLengthPair{keyDup, valueLength})
	if err := b.indexBuf.EncodeMessage(&entry); err != nil {
		log.Panicln("Unexpected error encoding index", err)
	}
}

func (b *Builder) Build() error {
	var header pb.TableHeader
	header.Version = 1
	header.IndexLength = uint32(len(b.indexBuf.Bytes()))
	header.IndexEntries = uint32(len(b.keys))
	// TODO: Implement index compression.

	headerBuf, err := proto.Marshal(&header)
	if err != nil {
		return err
	}

	var headerSize [4]byte
	binary.LittleEndian.PutUint32(headerSize[:], uint32(len(headerBuf)))
	_, err = writeMany(b.w, headerSize[:], headerBuf, b.indexBuf.Bytes())
	if err != nil {
		return err
	}

	for _, pair := range b.keys {
		if pair.length == 0 {
			continue
		}
		n, err := b.vf(pair.key, b.w)
		if err != nil {
			return err
		} else if n != int(pair.length) {
			log.Panicf("Unexpected value write length %d, expected %d", n, pair.length)
		}
	}

	return nil
}
