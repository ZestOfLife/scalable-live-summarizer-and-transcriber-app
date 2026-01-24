package query_server

import (
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func RegisterQueryRoute(mux *http.ServeMux, redisClient *redis.Client) {
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		serveWebSocket(w, r, redisClient)
	})
}

func serveWebSocket(w http.ResponseWriter, r *http.Request, c *redis.Client) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	id := r.URL.Query().Get("id")
	ctx := r.Context()

	pubsub := c.Subscribe(ctx, "transcription-"+id, "summary-"+id)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		err := conn.WriteJSON(msg.Payload)
		if err != nil {
			break // Con closed
		}
	}
}
