package sstable

import "io"

func writeMany(w io.Writer, bufs ...[]byte) (int, error) {
	var written int
	for _, b := range bufs {
		n, err := w.Write(b)
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func dup(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
