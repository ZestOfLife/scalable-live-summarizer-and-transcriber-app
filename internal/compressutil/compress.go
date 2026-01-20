package compressutil

import (
	"github.com/klauspost/compress/zstd"
)

var encoder, _ = zstd.NewWriter(nil)

func CompressData(data []byte) []byte {
	compressed := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return compressed
}
