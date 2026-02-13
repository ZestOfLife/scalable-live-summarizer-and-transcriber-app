package orchestrator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"image"

	triton "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/triton_proto/v1"
	"github.com/corona10/goimagehash"
)

func parseTritonResponse(resp *triton.ModelInferResponse) (string, error) {
	rawContents := resp.RawOutputContents[0]

	if len(rawContents) < 4 {
		return "", fmt.Errorf("response too short to contain length prefix")
	}

	stringLen := binary.LittleEndian.Uint32(rawContents[0:4])
	ret := string(rawContents[4 : 4+stringLen])

	return ret, nil
}

func (s *UserStream) checkDifference(newFrame []byte, threshold int) bool {
	// Decode to image.Image
	img, _, err := image.Decode(bytes.NewReader(newFrame))
	if err != nil {
		return false
	}

	// Generate hash
	newHash, _ := goimagehash.PerceptionHash(img)
	if s.lastHash == nil {
		s.lastHash = newHash
		return true
	}

	distance, _ := newHash.Distance(s.lastHash)

	if distance > threshold {
		s.lastHash = newHash
		return true
	}

	return false
}
