package sstable

import (
	"bytes"
	"io"
	"sort"
	"testing"
)

type testValuePair struct {
	val   string
	extra []byte
}

var testValues = map[string]testValuePair{
	"foo":  {"bar1", nil},
	"foo1": {"bar2", nil},
	"foo2": {"bar3", nil},
	"foo3": {"", nil},
	"goo":  {"bar4", nil},
	"goo1": {"bar5", nil},
	"hoo":  {"randomstuff", []byte{1, 2, 3, 4, 5}},
	// 256 characters.
	"hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh": {"bar6", nil},
	"zzz": {"last", nil},
}

var testValuesKeyTooLong = map[string]testValuePair{
	// 257 characters.
	"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {"bar1", nil},
}

var emptyTable = map[string]testValuePair{}

func buildTable(t *testing.T, entries map[string]testValuePair) []byte {
	w := new(bytes.Buffer)

	vf := func(key []byte, w io.Writer) (int, error) {
		return w.Write([]byte(entries[string(key)].val))
	}
	b := NewBuilder(w, vf)

	var sortedKeys []string
	for k, _ := range entries {
		sortedKeys = append(sortedKeys, string(k))
	}
	sort.Strings(sortedKeys)

	for _, k := range sortedKeys {
		b.Add([]byte(k), uint32(len(entries[k].val)), entries[k].extra)
	}

	err := b.Build()
	if err != nil {
		t.Error(err)
	}

	return w.Bytes()
}

func buildReader(t *testing.T, buf []byte) (*Table, error) {
	r := bytes.NewReader(buf)
	return Load(r)
}

func checkTable(t *testing.T, table *Table, entries map[string]testValuePair) {
	for k, p := range entries {
		t.Log("Key:", k, "pair:", p)
		if !table.Has([]byte(k)) {
			t.Error("Table missing key", k)
		}

		v, e, err := table.Get([]byte(k))
		if err != nil {
			t.Error("Unexpected error in Get()", err)
		}
		if !bytes.Equal([]byte(p.val), v) {
			t.Error("Incorrect value", p.val, v)
		}
		if !bytes.Equal(p.extra, e) {
			t.Error("Incorrect extra", p.extra, e)
		}

		v = make([]byte, len(p.val))
		err = table.GetPartial([]byte(k), 0, v)
		if err != nil && len(p.val) > 0 {
			t.Error("Unexpected error in GetPartial()", err)
		}
		if !bytes.Equal([]byte(p.val), v) {
			t.Error("Incorrect value", p.val, v)
		}
		err = table.GetPartial([]byte(k), 1, v)
		if err == nil {
			t.Error("Unexpected non-error in GetPartial() overflow")
		} else {
			t.Log("GetPartial() overflow error", err)
		}

		// These should always get the key.
		key, e, n := table.LowerKey([]byte(k))
		if !bytes.Equal([]byte(k), key) {
			t.Error("Incorrect key", k, key)
		}
		if !bytes.Equal(p.extra, e) {
			t.Error("Incorrect extra", p.extra, e)
		}
		if uint(len(p.val)) != n {
			t.Error("Incorrect value length", len(p.val), n)
		}

		key, e, n = table.UpperKey([]byte(k))
		if !bytes.Equal([]byte(k), key) {
			t.Error("Incorrect key", k, key)
		}
		if !bytes.Equal(p.extra, e) {
			t.Error("Incorrect extra", p.extra, e)
		}
		if uint(len(p.val)) != n {
			t.Error("Incorrect value length", len(p.val), n)
		}
	}
}

func checkPrev(t *testing.T, table *Table, entries map[string]testValuePair, key, expected string) {
	t.Log("Search:", key, "expecting:", expected)
	k, e, n := table.LowerKey([]byte(key))
	if !bytes.Equal([]byte(expected), k) {
		t.Error("Unexpected key", []byte(expected), k)
	}
	if k == nil {
		return
	}

	expectedExtra := entries[expected].extra
	if !bytes.Equal(expectedExtra, e) {
		t.Error("Incorrect extra", expectedExtra, e)
	}
	expectedVal := entries[expected].val
	if uint(len(expectedVal)) != n {
		t.Error("Incorrect value length", len(expectedVal), n)
	}
}

func checkNext(t *testing.T, table *Table, entries map[string]testValuePair, key, expected string) {
	t.Log("Search:", key, "expecting:", expected)
	k, e, n := table.UpperKey([]byte(key))
	if !bytes.Equal([]byte(expected), k) {
		t.Error("Unexpected key", []byte(expected), k)
	}
	if k == nil {
		return
	}

	expectedExtra := entries[expected].extra
	if !bytes.Equal(expectedExtra, e) {
		t.Error("Incorrect extra", expectedExtra, e)
	}
	expectedVal := entries[expected].val
	if uint(len(expectedVal)) != n {
		t.Error("Incorrect value length", len(expectedVal), n)
	}
}
