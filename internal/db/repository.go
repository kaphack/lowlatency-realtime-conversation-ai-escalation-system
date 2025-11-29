package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
)

type Repository struct {
	db *sql.DB
}

func NewRepository() (*Repository, error) {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&loc=Local",
		user, pass, host, port, name,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	repo := &Repository{db: db}
	if err := repo.initSchema(); err != nil {
		return nil, err
	}

	return repo, nil
}

func (r *Repository) initSchema() error {
	log.Println("Initializing schema...")
	queryRules := `
	CREATE TABLE IF NOT EXISTS rules (
		id VARCHAR(36) PRIMARY KEY,
		name TEXT NOT NULL,
		conditions JSON NOT NULL,
		action TEXT NOT NULL
	);
	`
	if _, err := r.db.Exec(queryRules); err != nil {
		return fmt.Errorf("failed to create rules table: %w", err)
	}

	queryMessages := `
	CREATE TABLE IF NOT EXISTS messages (
		id VARCHAR(36) PRIMARY KEY,
		conversation_id VARCHAR(255),
		content TEXT,
		timestamp BIGINT
	);
	`
	if _, err := r.db.Exec(queryMessages); err != nil {
		return fmt.Errorf("failed to create messages table: %w", err)
	}

	return nil
}

func (r *Repository) SaveMessage(conversationID, content string, timestamp int64) error {
	id := uuid.New().String()
	query := `INSERT INTO messages (id, conversation_id, content, timestamp) VALUES (?, ?, ?, ?)`
	_, err := r.db.Exec(query, id, conversationID, content, timestamp)
	if err != nil {
		return fmt.Errorf("failed to save message: %w", err)
	}
	return nil
}

// GetWordCounts simulates Spark aggregation by counting words in recent messages for a conversation
func (r *Repository) GetWordCounts(conversationID string) (map[string]int, error) {
	// In a real scenario with Spark, this would query the Spark cluster or a pre-aggregated view.
	// Here we aggregate from the messages table directly.
	query := `SELECT content FROM messages WHERE conversation_id = ?`
	rows, err := r.db.Query(query, conversationID)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages: %w", err)
	}
	defer rows.Close()

	wordCounts := make(map[string]int)
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			continue
		}
		// Simple tokenization (split by space)
		// In production, use a proper tokenizer and normalizer
		words := core.Tokenize(content)
		for _, word := range words {
			wordCounts[word]++
		}
	}
	return wordCounts, nil
}

func (r *Repository) CreateRule(name string, conditions []core.Condition, action string) (*core.Rule, error) {
	id := uuid.New().String()
	condBytes, err := json.Marshal(conditions)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal conditions: %w", err)
	}

	query := `INSERT INTO rules (id, name, conditions, action) VALUES (?, ?, ?, ?)`
	_, err = r.db.Exec(query, id, name, condBytes, action)
	if err != nil {
		return nil, fmt.Errorf("failed to insert rule: %w", err)
	}

	return &core.Rule{
		ID:         id,
		Name:       name,
		Conditions: json.RawMessage(condBytes),
		Action:     action,
	}, nil
}

func (r *Repository) GetAllRules() ([]core.ParsedRule, error) {
	query := `SELECT id, name, conditions, action FROM rules`
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query rules: %w", err)
	}
	defer rows.Close()

	var rules []core.ParsedRule
	for rows.Next() {
		var rule core.Rule
		var condBytes []byte
		if err := rows.Scan(&rule.ID, &rule.Name, &condBytes, &rule.Action); err != nil {
			log.Printf("failed to scan rule: %v", err)
			continue
		}
		rule.Conditions = json.RawMessage(condBytes)

		var conditions []core.Condition
		if err := json.Unmarshal(condBytes, &conditions); err != nil {
			log.Printf("failed to unmarshal conditions for rule %s: %v", rule.ID, err)
			continue
		}

		rules = append(rules, core.ParsedRule{
			Rule:             rule,
			ParsedConditions: conditions,
		})
	}
	return rules, nil
}
