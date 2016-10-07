package sstable

import (
	"testing"
)

func TestBuilder(t *testing.T) {
	buildTable(t, testValues)
}

func TestBuilderKeyTooLong(t *testing.T) {
	defer func() {
		x := recover()
		if x == nil {
			t.Error("Expected panic")
		} else {
			t.Log("Expected panic", x)
		}
	}()
	buildTable(t, testValuesKeyTooLong)
}
