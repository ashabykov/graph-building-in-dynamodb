package domain

import (
	"testing"
	"time"
)

func TestEdgeCreation(t *testing.T) {
	now := time.Now()
	edge := &Edge{
		SourceID:  "node-1",
		TargetID:  "node-2",
		Weight:    1.5,
		Label:     "connects",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if edge.SourceID != "node-1" {
		t.Errorf("Expected SourceID to be 'node-1', got %s", edge.SourceID)
	}

	if edge.TargetID != "node-2" {
		t.Errorf("Expected TargetID to be 'node-2', got %s", edge.TargetID)
	}

	if edge.Weight != 1.5 {
		t.Errorf("Expected Weight to be 1.5, got %f", edge.Weight)
	}

	if edge.Label != "connects" {
		t.Errorf("Expected Label to be 'connects', got %s", edge.Label)
	}
}

func TestEdgeWithoutOptionalFields(t *testing.T) {
	now := time.Now()
	edge := &Edge{
		SourceID:  "node-a",
		TargetID:  "node-b",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if edge.Weight != 0 {
		t.Errorf("Expected Weight to be 0 (default), got %f", edge.Weight)
	}

	if edge.Label != "" {
		t.Errorf("Expected Label to be empty (default), got %s", edge.Label)
	}
}