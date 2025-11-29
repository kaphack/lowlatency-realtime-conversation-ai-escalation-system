package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/core"
	"github.com/kaphack/lowlatency-realtime-conversation-ai-escalation-system/internal/db"
)

type Handler struct {
	repo *db.Repository
}

func NewHandler(repo *db.Repository) *Handler {
	return &Handler{repo: repo}
}

type CreateRuleRequest struct {
	Name       string           `json:"name"`
	Conditions []core.Condition `json:"conditions"`
	Action     string           `json:"action"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) CreateRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Method not allowed"})
		return
	}

	var req CreateRuleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid request body: " + err.Error()})
		return
	}

	// Validate input
	if req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Rule name is required"})
		return
	}

	if len(req.Conditions) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "At least one condition is required"})
		return
	}

	if req.Action == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Action is required"})
		return
	}

	// Create rule
	rule, err := h.repo.CreateRule(req.Name, req.Conditions, req.Action)
	if err != nil {
		log.Printf("Failed to create rule: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to create rule"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(rule)
}

type RuleResponse struct {
	ID         string           `json:"id"`
	Name       string           `json:"name"`
	Conditions []core.Condition `json:"conditions"`
	Action     string           `json:"action"`
}

func (h *Handler) GetAllRules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Method not allowed"})
		return
	}

	rules, err := h.repo.GetAllRules()
	if err != nil {
		log.Printf("Failed to fetch rules: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to fetch rules"})
		return
	}

	var response []RuleResponse
	for _, rule := range rules {
		response = append(response, RuleResponse{
			ID:         rule.ID,
			Name:       rule.Name,
			Conditions: rule.ParsedConditions,
			Action:     rule.Action,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) HandleRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.GetAllRules(w, r)
	case http.MethodPost:
		h.CreateRule(w, r)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Method not allowed"})
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/rules", h.HandleRules)
}
