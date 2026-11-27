package main

import (
	"encoding/json"
	"fmt"
)

// Tab is the slice of a herdr tab the plugin cares about.
type Tab struct {
	TabID     string
	Label     string
	PaneCount int
	Focused   bool
}

// Pane is the slice of a herdr pane the plugin cares about.
type Pane struct {
	PaneID  string
	TabID   string
	Focused bool
	Title   string // terminal_title_stripped: the pane's terminal title
}

// Snapshot is the slice of herdr's session.snapshot the plugin cares about.
type Snapshot struct {
	Tabs     []Tab
	Panes    []Pane
	TabFocus map[string]string // tab_id -> the pane the tab's layout has focused
}

// decodeSnapshot parses a session.snapshot RESULT payload: {"snapshot":{...}}.
func decodeSnapshot(data []byte) (*Snapshot, error) {
	var payload struct {
		Snapshot *struct {
			Tabs []struct {
				TabID     string `json:"tab_id"`
				Label     string `json:"label"`
				PaneCount int    `json:"pane_count"`
				Focused   bool   `json:"focused"`
			} `json:"tabs"`
			Panes []struct {
				PaneID        string `json:"pane_id"`
				TabID         string `json:"tab_id"`
				Focused       bool   `json:"focused"`
				TitleStripped string `json:"terminal_title_stripped"`
			} `json:"panes"`
			Layouts []struct {
				TabID         string `json:"tab_id"`
				FocusedPaneID string `json:"focused_pane_id"`
			} `json:"layouts"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	raw := payload.Snapshot
	if raw == nil {
		return nil, fmt.Errorf("decode snapshot: no snapshot payload in response")
	}

	snap := &Snapshot{TabFocus: map[string]string{}}
	for _, l := range raw.Layouts {
		if l.FocusedPaneID != "" {
			snap.TabFocus[l.TabID] = l.FocusedPaneID
		}
	}
	for _, t := range raw.Tabs {
		snap.Tabs = append(snap.Tabs, Tab{
			TabID: t.TabID, Label: t.Label, PaneCount: t.PaneCount, Focused: t.Focused,
		})
	}
	for _, p := range raw.Panes {
		snap.Panes = append(snap.Panes, Pane{
			PaneID: p.PaneID, TabID: p.TabID, Focused: p.Focused, Title: p.TitleStripped,
		})
	}
	return snap, nil
}

// FetchSnapshot asks the session socket for a full state snapshot.
func FetchSnapshot(sockPath string) (*Snapshot, error) {
	result, err := apiRequest(sockPath, "session.snapshot", nil)
	if err != nil {
		return nil, err
	}
	return decodeSnapshot(result)
}
