package sstable

import (
	"bytes"
	"io"
	"sort"
	"testing"
)

func TestReader(t *testing.T) {
	table, err := buildReader(t, buildTable(t, testValues))
	if err != nil {
		t.Fatal("Error building table", err)
	}
	checkTable(t, table, testValues)

	checkPrev(t, table, testValues, "", "")
	checkPrev(t, table, testValues, "fo", "")
	checkPrev(t, table, testValues, "foo4", "foo3")
	checkPrev(t, table, testValues, "go", "foo3")
	checkPrev(t, table, testValues, "zzzzz", "zzz")

	checkNext(t, table, testValues, "", "foo")
	checkNext(t, table, testValues, "fo", "foo")
	checkNext(t, table, testValues, "foo4", "goo")
	checkNext(t, table, testValues, "g", "goo")
	checkNext(t, table, testValues, "iii", "zzz")
	checkNext(t, table, testValues, "zzzz", "")

	_, _, err = table.Get([]byte("fghjghjk"))
	if err != ErrNotFound {
		t.Error("Unexpected error getting non-existent key", err)
	}

	if table.NumKeys() != 9 {
		t.Error("Incorrect number of keys", table.NumKeys())
	}
	if table.DataSize() != 39 {
		t.Error("Incorrect data size", table.DataSize())
	}
}

func TestReader_Keys(t *testing.T) {
	table, err := buildReader(t, buildTable(t, testValues))
	if err != nil {
		t.Fatal("Error building table", err)
	}

	expectedKeys := make([]string, 0, len(testValues))
	for k, _ := range testValues {
		expectedKeys = append(expectedKeys, k)
	}
	sort.Strings(expectedKeys)

	tableKeys := table.Keys()
	if len(tableKeys) != len(expectedKeys) {
		t.Error("Incorrect number of keys, expected", len(expectedKeys),
			"actual", len(tableKeys))
	}

	for i, k := range tableKeys {
		if string(k) != expectedKeys[i] {
			t.Error("Incorrect key, expected", expectedKeys[i],
				"actual", string(k))
		}
	}
}

func TestReader_KeyIter(t *testing.T) {
	table, err := buildReader(t, buildTable(t, testValues))
	if err != nil {
		t.Fatal("Error building table", err)
	}

	expectedKeys := make([]string, 0, len(testValues))
	for k, _ := range testValues {
		expectedKeys = append(expectedKeys, k)
	}
	sort.Strings(expectedKeys)

	iter := table.KeyIter()
	last := len(expectedKeys) - 1
	for i := 0; i < len(expectedKeys); i++ {
		k := iter.Value()
		if string(k) != expectedKeys[i] {
			t.Error("Incorrect key, expected", expectedKeys[i],
				"actual", string(k))
		}
		valid := iter.Next()
		if valid != (i != last) {
			t.Error("Unexpected return value from Next(), expected", i != last, "actual", valid)
		}
	}
	if iter.Value() != nil {
		t.Error("Expected nil iterator value, actual", iter.Value())
	}
}

func TestReader_EmptyTable(t *testing.T) {
	table, err := buildReader(t, buildTable(t, emptyTable))
	if err != nil {
		t.Fatal("Error building table", err)
	}

	k, e, _ := table.LowerKey([]byte("foo"))
	if k != nil || e != nil {
		t.Error("Unexpected key or extra", k, e)
	}

	k, e, _ = table.UpperKey([]byte("foo"))
	if k != nil || e != nil {
		t.Error("Unexpected key or extra", k, e)
	}
}

func TestReader_TruncatedValue(t *testing.T) {
	tableBuf := buildTable(t, testValues)
	// Read the buffer, truncated by 1
	table, err := buildReader(t, tableBuf[:len(tableBuf)-1])
	if err != nil {
		t.Fatal("Error building table", err)
	}

	_, _, err = table.Get([]byte("zzz"))
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected ErrUnexpectedEOF, got: %v", err)
	}

	r, err := table.GetReader([]byte("zzz"))
	if err != nil {
		t.Error("Unexpected error:", err)
	}

	buf := make([]byte, 4)
	n, err := r.ReadAt(buf, 0)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected ErrUnexpectedEOF, got: %v", err)
	} else if n != 3 {
		t.Errorf("Expected 3 bytes read, actual: %d", n)
	} else if !bytes.Equal(buf[:n], []byte("las")) {
		t.Errorf("Unexpected bytes read: %v", buf[:n])
	}

	n, err = r.ReadAt(buf, 1)
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected ErrUnexpectedEOF, got: %v", err)
	} else if n != 2 {
		t.Errorf("Expected 3 bytes read, actual: %d", n)
	} else if !bytes.Equal(buf[:n], []byte("as")) {
		t.Errorf("Unexpected bytes read: %v", buf[:n])
	}
}
