package query

type SummaryResponse struct {
	TimestampStart int64  `json:"timestamp_start"`
	TimestampEnd   int64  `json:"timestamp_end"`
	Summary        string `json:"summary"`
}
