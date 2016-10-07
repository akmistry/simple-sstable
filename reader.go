package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/google/btree"

        pb "github.com/akmistry/simple-sstable/proto"
)

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type indexEntry pb.IndexEntry

func (e *indexEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.Key, than.(*indexEntry).Key) == -1
}

type Table struct {
	r ReadAtCloser

	dataOffset uint64
	index *btree.BTree
}

var ErrNotFound = errors.New("Not found")

func Load(r ReadAtCloser) (*Table, error) {
	reader := &Table{r: r, index: btree.New(2)}
	err := reader.readIndex()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (t *Table) Close() error {
	t.index = nil
	return t.r.Close()
}

func (t *Table) readIndex() error {
	var headerSize [4]byte
	_, err := t.r.ReadAt(headerSize[:], 0)
	if err != nil {
		return err
	}
	hs := binary.LittleEndian.Uint32(headerSize[:])
	headerBuf := make([]byte, hs)
	_, err = t.r.ReadAt(headerBuf, 4)
	if err != nil {
		return err
	}
	var header pb.TableHeader
	err = proto.Unmarshal(headerBuf, &header)
	if err != nil {
		return err
	}

	if header.Version != 1 {
		return fmt.Errorf("Unsupported verison %d", header.Version)
	}

	indexOffset := 4 + hs
	indexBuf := make([]byte, header.IndexLength)
	_, err = t.r.ReadAt(indexBuf, int64(indexOffset))
	if err != nil {
		return err
	}
	t.dataOffset = uint64(indexOffset + header.IndexLength)

	for len(indexBuf) > 0 {
		entryLen, consumed := proto.DecodeVarint(indexBuf)
		if consumed == 0 {
			return fmt.Errorf("Invalid index encoding")
		}
		entry := new(pb.IndexEntry)
		err = proto.Unmarshal(indexBuf[consumed:consumed+int(entryLen)], entry)
		if err != nil {
			return err
		}

		if existing := t.index.ReplaceOrInsert((*indexEntry)(entry)); existing != nil {
			return fmt.Errorf("Duplicate item in index: %v", existing)
		}

		indexBuf = indexBuf[consumed+int(entryLen):]
	}

	return nil
}

func (t *Table) Has(key []byte) bool {
	keyItem := indexEntry{Key: key}
	return t.index.Has(&keyItem)
}

func (t *Table) getEntry(key []byte) *indexEntry {
	keyItem := indexEntry{Key: key}
	if ie, ok := t.index.Get(&keyItem).(*indexEntry); ok {
		return ie
	}
	return nil
}

func (t *Table) Get(key []byte) (value []byte, extra []byte, e error) {
	ie := t.getEntry(key)
	if ie == nil {
		return nil, nil, ErrNotFound
	}
	if ie.Length == 0 {
		return nil, ie.Extra, nil
	}
	value = make([]byte, int(ie.Length))
	_, err := t.r.ReadAt(value, int64(t.dataOffset + ie.Offset))
	if err != nil {
		return nil, nil, err
	}
	return value, ie.Extra, nil
}

func (t *Table) GetPartial(key []byte, off uint, p []byte) error {
	ie := t.getEntry(key)
	if ie == nil {
		return ErrNotFound
	}
	if uint32(off + uint(len(p))) > ie.Length {
		return io.ErrUnexpectedEOF
	}
	if ie.Length == 0 || len(p) == 0 {
		// The user really shouldn't be doing this.
		return nil
	}
	_, err := t.r.ReadAt(p, int64(t.dataOffset + ie.Offset + uint64(off)))
	return err
}

// Gets the key (and extra and value length) in the table that is less than or
// equal to the given key. Will return nil if no such key exists.
func (t *Table) LowerKey(key []byte) (k []byte, e []byte, n uint) {
	keyItem := indexEntry{Key: key}
	var ie *indexEntry
	iter := func(i btree.Item) bool {
		ie = i.(*indexEntry)
		return false
	}
	t.index.DescendLessOrEqual(&keyItem, iter)
	if ie == nil {
		return nil, nil, 0
	}
	return ie.Key, ie.Extra, uint(ie.Length)
}
