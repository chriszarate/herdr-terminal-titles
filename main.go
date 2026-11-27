// herdr-titles sets each herdr tab's label to the terminal title of the tab's
// active pane, and does nothing else. A per-session daemon (watch.go) follows
// herdr's event stream and runs one sync pass after every burst of events.
//
// Forked from davidolrik/herdr-titles, with the window title, program naming,
// agent titles, icons, shell hooks, and manual-rename tracking removed.
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

const pluginID = "chriszarate.titles"

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultDir(kind, name string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, kind, name)
}

// The binary can also be run by hand, without herdr's HERDR_PLUGIN_*
// environment, so the fallback names the same directory herdr passes to
// plugin-spawned runs: its managed per-plugin state path.
func pluginStateDir() string {
	return envOr("HERDR_PLUGIN_STATE_DIR",
		defaultDir(".local/state", filepath.Join("herdr", "plugins", pluginID)))
}

// runSync is one idempotent pass: snapshot the session, and rename every tab
// whose label differs from its active pane's terminal title.
func runSync() error {
	sock := sessionSocketPath()
	snap, err := FetchSnapshot(sock)
	if err != nil {
		return err
	}
	return SyncTabs(sock, snap, maxLabelLen)
}

func main() {
	command := "sync"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	var err error
	switch command {
	case "sync":
		err = runSync()
	case "watch":
		if len(os.Args) > 2 && os.Args[2] == "--detached" {
			err = runWatchDetached()
		} else {
			err = runWatchParent()
		}
	case "watchdog":
		err = runWatchdog()
	default:
		err = fmt.Errorf("unknown command (want sync, watch, or watchdog)")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "herdr-titles %s: %v\n", command, err)
		os.Exit(1)
	}
}
