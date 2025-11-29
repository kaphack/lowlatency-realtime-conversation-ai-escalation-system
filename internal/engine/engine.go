package engine

//nice
import (
	"fmt"
	"log"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

type Engine struct {
	analyzer *core.Analyzer
}

func NewEngine() *Engine {
	repo, err := db.NewRepository()
	if err != nil {
		fmt.Printf("err in repo init")
	}

	return &Engine{
		analyzer: core.NewAnalyzer(repo),
	}
}

func (e *Engine) ProcessChunk(chunk *conversationv1.ConversationChunk) {
	// Later: evaluate rules, trigger escalation, save event to MySQL.
	e.analyzer.Analyze(chunk)
	log.Printf("[engine] session=%s msg_id=%s text=%s",
		chunk.SessionId, chunk.MessageId, chunk.Text)
}
