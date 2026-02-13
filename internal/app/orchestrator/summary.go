package orchestrator

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	triton "github.com/ZestOfLife/scalable-live-summarizer-and-transcriber-app/pkg/gen/triton_proto/v1"
	"github.com/redis/go-redis/v9"
)

const (
	CORN_JOB_TIMER       = 3 * time.Second // Check every 3s
	MIN_AUDIO_THRESHOLD  = 15
	MIN_VISUAL_THRESHOLD = 3
)

var cronScript *redis.Script

func (w *Worker) loadRedisScript() error {
	cronContent, err := os.ReadFile("/script/cronjob.lua")
	if err != nil {
		return err
	}
	cronScript = redis.NewScript(string(cronContent))
	return nil
}

func (w *Worker) startSummaryManager(ctx context.Context) {
	if cronScript == nil {
		err := w.loadRedisScript()
		if err != nil {
			fmt.Errorf("Failed loading script. Error: %v", err)
		}
	}
	ticker := time.NewTicker(CORN_JOB_TIMER)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			id, _ := w.RedisClient.SMembers(ctx, "active_sessions").Result()

			for _, id := range id {
				go w.processSummary(ctx, id)
			}
		}
	}
}

func (w *Worker) processSummary(ctx context.Context, id string) {
	audioKey := fmt.Sprintf("id:%s:type:audio", id)
	visualKey := fmt.Sprintf("id:%s:type:visual", id)

	// Get after meeting audio and visual count thresholds
	res, err := cronScript.Run(ctx, w.RedisClient, []string{audioKey, visualKey}, MIN_AUDIO_THRESHOLD, MIN_VISUAL_THRESHOLD).Result()
	if err != nil || res == nil {
		return
	}

	// Process data
	data := res.([]interface{})
	if len(data) != 2 {
		fmt.Errorf("Something has went wrong fetching summary")
		return
	}
	audioItems := data[0].([]interface{})
	visualItems := data[1].([]interface{})
	// Prompt
	prompt := fmt.Sprintf("<|im_start|>user\nTranscript: %s\nVisuals: %s\nSummarize this:<|im_end|>\n<|im_start|>assistant\n",
		audioItems, visualItems)

	// Text
	textData := make([]byte, 4+len(prompt))
	binary.LittleEndian.PutUint32(textData[0:4], uint32(len(prompt)))
	copy(textData[4:], []byte(prompt))

	request := &triton.ModelInferRequest{
		ModelName: os.Getenv("TRITON_SUMMARY_NAME"),
		Inputs: []*triton.ModelInferRequest_InferInputTensor{
			{
				Name:     "text_input",
				Datatype: "BYTES",
				Shape:    []int64{1},
			},
		},
		RawInputContents: [][]byte{textData},
	}

	// Inference
	resp, _ := w.TritonClient.ModelInfer(context.Background(), request)

	// Publish to query redis
	summary, _ := parseTritonResponse(resp)
	w.RedisClient.Publish(ctx, "summary-"+id, summary)
}
