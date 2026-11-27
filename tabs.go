package main

import (
	"errors"
	"strings"
)

// activePane resolves which pane a tab is named after: a single-pane tab by
// its sole pane; a multi-pane tab by the pane its layout has focused, which
// is the pane herdr re-focuses when the tab is next selected. Without layout
// data, the focused tab falls back to the globally focused pane, and a
// background tab has no answer.
func activePane(tab Tab, snap *Snapshot) (Pane, bool) {
	var panes []Pane
	for _, p := range snap.Panes {
		if p.TabID == tab.TabID {
			panes = append(panes, p)
		}
	}
	if len(panes) == 1 {
		return panes[0], true
	}
	if focus := snap.TabFocus[tab.TabID]; focus != "" {
		for _, p := range panes {
			if p.PaneID == focus {
				return p, true
			}
		}
	}
	if tab.Focused {
		for _, p := range panes {
			if p.Focused {
				return p, true
			}
		}
	}
	return Pane{}, false
}

// truncateRunes cuts s to max characters. max <= 0 means no limit.
func truncateRunes(s string, max int) string {
	if max <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) > max {
		return string(runes[:max])
	}
	return s
}

// tabName is the label a tab should carry: its active pane's terminal title.
// "" means the tab must be left alone — no active pane is known, or that pane
// has set no title.
func tabName(tab Tab, snap *Snapshot, maxLen int) string {
	pane, ok := activePane(tab, snap)
	if !ok {
		return ""
	}
	return truncateRunes(strings.TrimSpace(pane.Title), maxLen)
}

// SyncTabs walks every tab once and renames each one whose label differs from
// its active pane's terminal title. It is idempotent: a second call with the
// same state sends nothing. Every tab is attempted; the errors are joined.
func SyncTabs(sockPath string, snap *Snapshot, maxLen int) error {
	var errs []error
	for _, tab := range snap.Tabs {
		name := tabName(tab, snap, maxLen)
		if name == "" || name == tab.Label {
			continue
		}
		_, err := apiRequest(sockPath, "tab.rename", map[string]string{"tab_id": tab.TabID, "label": name})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
