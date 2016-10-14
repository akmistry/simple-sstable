package sstable

import (
	"sort"
	"testing"
)

func TestReader(t *testing.T) {
	table, err := buildReader(t, buildTable(t, testValues))
	if err != nil {
		t.Error("Error building table", err)
	}
	checkTable(t, table, testValues)

	checkPrev(t, table, testValues, "fo", "")
	checkPrev(t, table, testValues, "foo4", "foo3")
	checkPrev(t, table, testValues, "go", "foo3")

	_, _, err = table.Get([]byte("fghjghjk"))
	if err != ErrNotFound {
		t.Error("Unexpected error getting non-existent key", err)
	}
}

func TestReader_Keys(t *testing.T) {
        table, err := buildReader(t, buildTable(t, testValues))
        if err != nil {
                t.Error("Error building table", err)
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
