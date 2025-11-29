package api

import (
	"bytes"
	"encoding/json"
	"fmt"
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

type ExecuteFlowRequest struct {
	TicketID         string `json:"ticketId"`
	FlowID           string `json:"flowId"`
	RuleID           string `json:"ruleId"`
	EventDataVarName string `json:"eventDataVarName"`
	EventType        string `json:"eventType"`
	EventData        struct {
		X string `json:"x"`
	} `json:"eventData"`
}

func (h *Handler) ExecuteFlow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Method not allowed"})
		return
	}

	var req ExecuteFlowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid request body: " + err.Error()})
		return
	}

	// 1. Fetch Rule
	rules, err := h.repo.GetAllRules()
	if err != nil {
		log.Printf("Failed to fetch rules: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to fetch rules"})
		return
	}

	var targetRule *core.ParsedRule
	for _, rule := range rules {
		if rule.ID == req.RuleID {
			targetRule = &rule
			break
		}
	}

	if targetRule == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Rule not found"})
		return
	}

	// 2. Get Word Counts (Simulating Spark)
	// Using a default conversation ID or deriving from ticket ID if applicable
	// For this demo, we assume ticketID maps to conversationID or we use a global aggregation
	wordCounts, err := h.repo.GetWordCounts("default_conversation")
	if err != nil {
		log.Printf("Failed to get word counts: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to get word counts"})
		return
	}

	// 3. Evaluate Rule
	// We need to adapt the core.Engine to evaluate a single rule against a map
	// For now, we'll do a manual check here or use the engine if possible.
	// The core.Engine expects map[string]int result.
	engine := core.NewEngine()
	actions := engine.Evaluate(wordCounts, []core.ParsedRule{*targetRule})

	if len(actions) > 0 {
		// Match found! Call external API
		if err := h.callExternalFlowAPI(req); err != nil {
			log.Printf("Failed to call external flow API: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to execute flow: " + err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Flow executed"})
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ignored", "message": "Rule conditions not met"})
	}
}

func (h *Handler) callExternalFlowAPI(req ExecuteFlowRequest) error {
	url := "http://bb.kapdesk.com:8080/ms/lfs/execute-flow"

	// Construct payload for external API
	// The user request has the same structure as the external API payload, minus RuleID
	payload := map[string]interface{}{
		"ticketId":         req.TicketID,
		"flowId":           req.FlowID,
		"eventDataVarName": req.EventDataVarName,
		"eventType":        req.EventType,
		"eventData":        req.EventData,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	externalReq, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}

	externalReq.Header.Set("Content-Type", "application/json")
	externalReq.Header.Set("Cookie", "_KAPTURECRM_SESSION=disz1kkhjbm0vaaf5cxn")

	client := &http.Client{}
	resp, err := client.Do(externalReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("external API returned status: %d", resp.StatusCode)
	}

	return nil
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/rules", h.HandleRules)
	mux.HandleFunc("/api/test-rule", h.ExecuteFlow)
}
