package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

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
	dataSize   uint64
	index      *btree.BTree

	indexEntries []indexEntry
}

var ErrNotFound = errors.New("Not found")

const useBtree = false

func Load(r ReadAtCloser) (*Table, error) {
	reader := &Table{r: r}
	if useBtree {
		reader.index = btree.New(2)
	}
	err := reader.readIndex()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (t *Table) NumKeys() int {
	if useBtree {
		return t.index.Len()
	}
	return len(t.indexEntries)
}

func (t *Table) DataSize() uint64 {
	return t.dataSize
}

func (t *Table) Close() error {
	t.index = nil
	t.indexEntries = nil
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
	t.dataOffset = uint64(indexOffset + header.IndexLength)
	if header.IndexLength == 0 {
		// No index, table is empty, done loading.
		return nil
	}
	indexBuf := make([]byte, header.IndexLength)
	_, err = t.r.ReadAt(indexBuf, int64(indexOffset))
	if err != nil {
		return err
	}

	if header.IndexEntries != 0 {
		t.indexEntries = make([]indexEntry, 0, int(header.IndexEntries))
	}
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

		if useBtree {
			if existing := t.index.ReplaceOrInsert((*indexEntry)(entry)); existing != nil {
				return fmt.Errorf("Duplicate item in index: %v", existing)
			}
		} else {
			t.indexEntries = append(t.indexEntries, indexEntry(*entry))
		}

		t.dataSize += uint64(entry.Length)
		indexBuf = indexBuf[consumed+int(entryLen):]
	}

	// Check t.indexEntries is sorted.
	for i := 0; i < len(t.indexEntries)-1; i++ {
		if bytes.Compare(t.indexEntries[i].Key, t.indexEntries[i+1].Key) != -1 {
			return fmt.Errorf("Unexpected sort order, %v >= %v", t.indexEntries[i].Key, t.indexEntries[i+1].Key)
		}
	}

	return nil
}

func (t *Table) Has(key []byte) bool {
	return t.getEntry(key) != nil
}

func (t *Table) getEntry(key []byte) *indexEntry {
	if useBtree {
		keyItem := indexEntry{Key: key}
		if ie, ok := t.index.Get(&keyItem).(*indexEntry); ok {
			return ie
		}
	} else {
		i := sort.Search(len(t.indexEntries), func(i int) bool {
			cmp := bytes.Compare(key, t.indexEntries[i].Key)
			return cmp <= 0
		})
		if i < len(t.indexEntries) && bytes.Compare(key, t.indexEntries[i].Key) == 0 {
			return &t.indexEntries[i]
		}
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
	_, err := t.r.ReadAt(value, int64(t.dataOffset+ie.Offset))
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
	if uint32(off+uint(len(p))) > ie.Length {
		return io.ErrUnexpectedEOF
	}
	if ie.Length == 0 || len(p) == 0 {
		// The user really shouldn't be doing this.
		return nil
	}
	_, err := t.r.ReadAt(p, int64(t.dataOffset+ie.Offset+uint64(off)))
	return err
}

func (t *Table) GetInfo(key []byte) (length uint, extra []byte, e error) {
	ie := t.getEntry(key)
	if ie == nil {
		return 0, nil, ErrNotFound
	}
	return uint(ie.Length), ie.Extra, nil
}

func (t *Table) Keys() (keys [][]byte) {
	if useBtree {
		if t.index.Len() == 0 {
			return
		}

		keys = make([][]byte, 0, t.index.Len())
		iter := func(i btree.Item) bool {
			ie := i.(*indexEntry)
			keys = append(keys, ie.Key)
			return true
		}
		t.index.Ascend(iter)
	} else {
		if len(t.indexEntries) == 0 {
			return
		}

		keys = make([][]byte, 0, len(t.indexEntries))
		for i, _ := range t.indexEntries {
			keys = append(keys, t.indexEntries[i].Key)
		}
	}
	return
}

// Gets the key (and extra and value length) in the table that is less than or
// equal to the given key. Will return nil if no such key exists.
func (t *Table) LowerKey(key []byte) (k []byte, e []byte, n uint) {
	if useBtree {
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
	} else {
		i := sort.Search(len(t.indexEntries), func(i int) bool {
			cmp := bytes.Compare(key, t.indexEntries[i].Key)
			return cmp <= 0
		})
		if i == len(t.indexEntries) {
			i--
		} else if bytes.Compare(key, t.indexEntries[i].Key) != 0 {
			i--
		}
		if i < 0 {
			return nil, nil, 0
		}
		return t.indexEntries[i].Key, t.indexEntries[i].Extra, uint(t.indexEntries[i].Length)
	}
	return nil, nil, 0
}

func (t *Table) UpperKey(key []byte) (k []byte, e []byte, n uint) {
	if useBtree {
		keyItem := indexEntry{Key: key}
		var ie *indexEntry
		iter := func(i btree.Item) bool {
			ie = i.(*indexEntry)
			return false
		}
		t.index.AscendGreaterOrEqual(&keyItem, iter)
		if ie == nil {
			return nil, nil, 0
		}
		return ie.Key, ie.Extra, uint(ie.Length)
	} else {
		i := sort.Search(len(t.indexEntries), func(i int) bool {
			cmp := bytes.Compare(key, t.indexEntries[i].Key)
			return cmp <= 0
		})
		if i >= len(t.indexEntries) {
			return nil, nil, 0
		}
		return t.indexEntries[i].Key, t.indexEntries[i].Extra, uint(t.indexEntries[i].Length)
	}
	return nil, nil, 0
}
