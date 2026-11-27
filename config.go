package main

import "time"

// Every choice the plugin makes is a constant in this file. There is no
// config file: change a value here and rebuild.

const (
	// maxLabelLen cuts tab labels to this many characters. 0 means no limit.
	maxLabelLen = 40

	// syncDebounce is the wait between the first event of a burst and the
	// sync pass that handles the burst.
	syncDebounce = 100 * time.Millisecond

	// syncRetry is the wait before the daemon retries a failed pass.
	syncRetry = 2 * time.Second

	// streamQuietLimit is how long the event stream can be silent before the
	// daemon pings the server to learn whether it is still alive.
	streamQuietLimit = 10 * time.Minute

	// binaryPoll is how often the daemon checks its own binary for a rebuild.
	binaryPoll = 15 * time.Second

	// socketTimeout bounds every dial and every request/response exchange.
	socketTimeout = 5 * time.Second
)

// watchSubscriptions is every event that can change which title a tab should
// show: a pane's title changed (pane.updated), a tab's active pane changed
// (pane.focused, layout.updated, and the pane lifecycle events), or a tab's
// label changed under us (tab.created, tab.renamed).
var watchSubscriptions = []string{
	"pane.updated",
	"pane.focused", "layout.updated",
	"pane.created", "pane.closed", "pane.exited", "pane.moved",
	"tab.created", "tab.renamed",
}
