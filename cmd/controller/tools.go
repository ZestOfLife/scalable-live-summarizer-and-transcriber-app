package main;

import (
	"githut.com/klauspost/compress/zstd"
)

var encoder, _ = zstd.NewWriter(nil)

func CompressData(bytes[] data) bytes[] {
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed
}
