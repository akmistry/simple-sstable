package sstable

func dup(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
