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
	vf ValueFunc

	indexBuf proto.Buffer
	keys     []keyLengthPair
	valuePos uint64

	started bool
	prev    []byte
}

type ValueFunc func(key []byte, p []byte) error

func NewBuilder(w io.Writer, vf ValueFunc) *Builder {
	return &Builder{w: w, vf: vf}
}

func (b *Builder) Add(key []byte, valueLength uint32, meta []byte) {
	if !b.started {
		b.started = true
	} else if bytes.Compare(b.prev, key) != -1 {
		log.Panicf("Key %d is before previous %d", key, b.prev)
	}

	if len(key) > 256 {
		log.Panicf("Key length %d > 256", len(key))
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

	var val []byte
	for _, pair := range b.keys {
		if pair.length == 0 {
			continue
		}
		if uint32(cap(val)) < pair.length {
			val = make([]byte, pair.length)
		} else {
			val = val[:pair.length]
		}
		err := b.vf(pair.key, val)
		if err != nil {
			return err
		}

		_, err = b.w.Write(val)
		if err != nil {
			return err
		}
	}

	return nil
}
