package sstable

import (
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
