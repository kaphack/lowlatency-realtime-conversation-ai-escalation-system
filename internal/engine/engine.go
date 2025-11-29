package engine

import (
	"log"

	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

type Engine struct{}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) ProcessChunk(chunk *conversationv1.ConversationChunk) {
	// Later: evaluate rules, trigger escalation, save event to MySQL.
	log.Printf("[engine] session=%s msg_id=%s text=%s",
		chunk.SessionId, chunk.MessageId, chunk.Text)
}
