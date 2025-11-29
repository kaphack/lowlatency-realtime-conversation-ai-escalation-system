package engine

//nice
import (
	"log"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	conversationv1 "github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/proto"
)

type Engine struct {
	analyzer *core.Analyzer
}

func NewEngine() *Engine {
	return &Engine{
		analyzer: core.NewAnalyzer(),
	}
}

func (e *Engine) ProcessChunk(chunk *conversationv1.ConversationChunk) {
	// Later: evaluate rules, trigger escalation, save event to MySQL.
	e.analyzer.Analyze(chunk)
	log.Printf("[engine] session=%s msg_id=%s text=%s",
		chunk.SessionId, chunk.MessageId, chunk.Text)
}
