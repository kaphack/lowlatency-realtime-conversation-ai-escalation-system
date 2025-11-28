package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rba1aji/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/google/uuid"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(dbPath string) (*Repository, error) {
	db, err := sql.Open("sqlite3", dbPath)
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
	query := `
	CREATE TABLE IF NOT EXISTS rules (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		conditions JSON NOT NULL,
		action TEXT NOT NULL
	);
	`
	_, err := r.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	return nil
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
