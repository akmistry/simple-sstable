package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/golang/protobuf/proto"

	pb "github.com/akmistry/simple-sstable/proto"
)

type indexEntry pb.IndexEntry

type TableStats struct {
	// Number of keys in the table
	NumKeys int

	// Total size of keys (bytes)
	KeysSize int

	// Total size of values (bytes)
	ValuesSize int64

	// Size of header (bytes)
	HeaderSize int

	// Size of index (bytes)
	IndexSize int
}

type Table struct {
	r     io.ReaderAt
	stats TableStats

	dataOffset uint64
	dataSize   uint64

	indexEntries []indexEntry
}

var ErrNotFound = errors.New("Not found")

func Load(r io.ReaderAt) (*Table, error) {
	reader := &Table{r: r}
	err := reader.readIndex()
	if err != nil {
		return nil, err
	}
	log.Println("Loaded table with num keys:", reader.NumKeys())
	return reader, nil
}

func (t *Table) NumKeys() int {
	return t.stats.NumKeys
}

func (t *Table) DataSize() uint64 {
	return t.dataSize
}

func (t *Table) Stats() TableStats {
	return t.stats
}

func (t *Table) Close() error {
	t.indexEntries = nil
	return nil
}

func (t *Table) readIndex() error {
	var headerSize [4]byte
	_, err := t.r.ReadAt(headerSize[:], 0)
	if err != nil {
		return err
	}
	hs := binary.LittleEndian.Uint32(headerSize[:])
	t.stats.HeaderSize = int(hs)
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

		t.indexEntries = append(t.indexEntries, indexEntry(*entry))

		t.dataSize += uint64(entry.Length)
		indexBuf = indexBuf[consumed+int(entryLen):]

		t.stats.NumKeys++
		t.stats.KeysSize += len(entry.Key)
		t.stats.ValuesSize += int64(entry.Length)
		t.stats.IndexSize += consumed + int(entryLen)
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
	i := sort.Search(len(t.indexEntries), func(i int) bool {
		cmp := bytes.Compare(key, t.indexEntries[i].Key)
		return cmp <= 0
	})
	if i < len(t.indexEntries) && bytes.Compare(key, t.indexEntries[i].Key) == 0 {
		return &t.indexEntries[i]
	}
	return nil
}

type ValueReader struct {
	t *Table

	extra []byte

	offset int64
	length uint32
}

func (r *ValueReader) Extra() []byte {
	return r.extra
}

// Returns int64 to match various other Size() methods (i.e. FileInfo.Size())
func (r *ValueReader) Size() int64 {
	return int64(r.length)
}

func (r *ValueReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		panic("off < 0")
	}
	if off >= int64(r.length) {
		return 0, io.EOF
	} else if len(p) == 0 {
		return 0, nil
	}

	readLen := len(p)
	if off+int64(readLen) > int64(r.length) {
		readLen = int(int64(r.length) - off)
	}
	n, err := r.t.r.ReadAt(p[:readLen], r.offset+off)
	if err == io.EOF && n < readLen {
		// Read was shorter than the expected value length, suggesting the file
		// has been truncated. This is unexpected.
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return n, err
	}
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (t *Table) GetReader(key []byte) (*ValueReader, error) {
	ie := t.getEntry(key)
	if ie == nil {
		return nil, ErrNotFound
	}

	r := &ValueReader{
		t:      t,
		extra:  ie.Extra,
		offset: int64(t.dataOffset + ie.Offset),
		length: uint32(ie.Length),
	}
	return r, nil
}

func (t *Table) Get(key []byte) (value []byte, extra []byte, e error) {
	r, err := t.GetReader(key)
	if err != nil {
		return nil, nil, err
	}

	value = make([]byte, int(r.Size()))
	n, err := r.ReadAt(value, 0)
	if err == io.EOF && n == len(value) {
		// All data was read, so not an error.
	} else if err != nil {
		return nil, nil, err
	}
	return value, r.Extra(), nil
}

// Deprecated: Use GetReader instead.
func (t *Table) GetPartial(key []byte, off uint, p []byte) error {
	r, err := t.GetReader(key)
	if err != nil {
		return err
	}

	n, err := r.ReadAt(p, int64(off))
	if err == io.EOF && len(p) > 0 && n == len(p) {
		// Normalise error so that EOF is only returned if n < len(p). ReadAt()
		// allows for io.EOF to be returned even when all bytes have been read.
		err = nil
	}
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
	if len(t.indexEntries) == 0 {
		return
	}

	keys = make([][]byte, 0, len(t.indexEntries))
	for i, _ := range t.indexEntries {
		keys = append(keys, t.indexEntries[i].Key)
	}
	return
}

type Iter struct {
	t *Table
	i int
}

func (i *Iter) Value() []byte {
	if i.i >= len(i.t.indexEntries) {
		return nil
	}
	return i.t.indexEntries[i.i].Key
}

func (i *Iter) Key() []byte {
	if i.i >= len(i.t.indexEntries) {
		return nil
	}
	return i.t.indexEntries[i.i].Key
}

func (i *Iter) ValueSize() int64 {
	if i.i >= len(i.t.indexEntries) {
		return 0
	}
	return int64(i.t.indexEntries[i.i].Length)
}

func (i *Iter) Next() bool {
	i.i++
	return i.i < len(i.t.indexEntries)
}

// Deprecated: Less bad interface TBD
func (t *Table) KeyIter() *Iter {
	return &Iter{t: t}
}

// Gets the key (and extra and value length) in the table that is less than or
// equal to the given key. Will return nil if no such key exists.
func (t *Table) LowerKey(key []byte) (k []byte, e []byte, n uint) {
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

func (t *Table) UpperKey(key []byte) (k []byte, e []byte, n uint) {
	i := sort.Search(len(t.indexEntries), func(i int) bool {
		cmp := bytes.Compare(key, t.indexEntries[i].Key)
		return cmp <= 0
	})
	if i >= len(t.indexEntries) {
		return nil, nil, 0
	}
	return t.indexEntries[i].Key, t.indexEntries[i].Extra, uint(t.indexEntries[i].Length)
}
