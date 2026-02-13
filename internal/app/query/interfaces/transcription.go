package query

type TranscriptionResponse struct {
	TimestampStart int64  `json:"timestamp_start"`
	TimestampEnd   int64  `json:"timestamp_end"`
	Transcription  string `json:"transcription"`
}
