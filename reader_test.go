package sstable

import (
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
	checkPrev(t, table, testValues, "zzzzz", "i")

	checkNext(t, table, testValues, "", "foo")
	checkNext(t, table, testValues, "fo", "foo")
	checkNext(t, table, testValues, "foo4", "goo")
	checkNext(t, table, testValues, "g", "goo")
	checkNext(t, table, testValues, "iii", "")

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
