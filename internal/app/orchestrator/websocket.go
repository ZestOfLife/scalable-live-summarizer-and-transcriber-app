package orchestrator

import (
	"container/list"
	"encoding/json"
	"log"
	"os"

	"github.com/gorilla/websocket"
)

type WhisperResponse struct {
	UID      string `json:"uid"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	Backend  string `json:"backend"`
	Language string `json:"language"`
	Segments []struct {
		Text  string  `json:"text"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"segments"`
}

func (w *Worker) readResults(id string) {
	user := w.sessions[id]
	defer func() {
		log.Printf("Closing read for id: %s", id)
		user.conn.Close()
		w.mu.Lock()
		delete(w.sessions, id) // Cleanup
		w.mu.Unlock()
	}()

	for {
		_, msg, err := user.conn.ReadMessage()
		if err != nil {
			log.Println("ERROR: %v", err)
			return
		}

		var resp WhisperResponse
		if err := json.Unmarshal(msg, &resp); err == nil && len(resp.Segments) > 0 {
			currentIndex := len(resp.Segments) - 1
			latestSeg := resp.Segments[currentIndex]

			// Same segment
			if currentIndex == user.lastCommittedIndex+1 {
				user.activeText = latestSeg.Text
				timestamp_start := user.timestampStart
				if user.timestamps.Len() < 2 {
					log.Println("ERROR: timestamp list does not have at least two elements")
					return
				}
				user.timestamps.Remove(user.timestamps.Front())
				timestamp_end := user.timestamps.Front().Value.(int64)
				user.timestamps.Remove(user.timestamps.Front())
				// TODO: Write to redis
			}

			// Pause detected (i.e. new segment)
			if currentIndex > user.lastCommittedIndex+1 {
				finalizedSegment := resp.Segments[currentIndex-1]

				timestamp_start := user.timestampStart
				timestamp_end := user.timestampLastRemoved
				user.timestampStart = -1
				if user.timestamps.Front() != nil {
					user.timestampStart = user.timestamps.Front().Value.(int64)
				}

				// TODO: write to redis

				user.lastCommittedIndex = currentIndex - 1
				user.activeText = latestSeg.Text
			}
		}
	}
}

func (w *Worker) GetSession(id string, timestamp_start int64, timestamp_end int64) (*websocket.Conn, error) {
	w.mu.RLock()
	user, exists := w.sessions[id]
	w.mu.RUnlock()

	if exists {
		user.timestamps.PushBack(timestamp_start)
		user.timestamps.PushBack(timestamp_end)
		return user.conn, nil
	}

	// If no session exists
	return w.CreateSession(id, timestamp_start, timestamp_end)
}

func (w *Worker) CreateSession(id string, timestamp_start int64, timestamp_end int64) (*websocket.Conn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Race cond
	if user, exists := w.sessions[id]; exists {
		return user.conn, nil
	}

	log.Printf("Creating new session for id: %s", id)
	wsURL := "ws://" + os.Getenv("WHISPERLIVE_ADDR_IP") + ":" + os.Getenv("WHISPERLIVE_ADDR_PORT")
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, err
	}
	timestamps := list.New()
	timestamps.PushBack(timestamp_start)
	timestamps.PushBack(timestamp_end)
	w.sessions[id] = &UserSession{conn: ws, timestampStart: timestamp_start, timestamps: timestamps, activeText: "", lastCommittedIndex: -1}

	config := map[string]interface{}{
		"uid":      id,
		"language": "en",
		"task":     "transcribe",
		"model":    "small",
		"use_vad":  true,
	}
	ws.WriteJSON(config)

	go w.readResults(id)
	return ws, nil
}
