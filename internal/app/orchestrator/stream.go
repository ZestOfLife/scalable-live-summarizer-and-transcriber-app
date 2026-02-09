package orchestrator

import (
	"bytes"
	"container/list"
	"context"
	"image"
	_ "image/jpeg"

	"github.com/corona10/goimagehash"
)

const SIMILAR_THRESHOLD = 10

type UserStream struct {
	In             chan []byte
	Ctx            context.Context
	Cancel         context.CancelFunc
	lastHash       *goimagehash.ImageHash
	id             string
	timestampStart int64
	timestamps     *list.List
}

func (s *UserStream) checkDifference(newFrame []byte) bool {
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

	if distance > SIMILAR_THRESHOLD {
		s.lastHash = newHash
		return true
	}

	return false
}
