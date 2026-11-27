package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// fakeHerdr is a minimal stand-in for herdr's session socket. It serves
// session.snapshot from the current tabs and panes, applies tab.rename to
// them, and pushes event lines to every subscriber.
type fakeHerdr struct {
	t        *testing.T
	path     string
	listener net.Listener

	mu       sync.Mutex
	tabs     []map[string]any
	panes    []map[string]any
	layouts  []map[string]any
	renames  []string // "tab_id=label", in order
	subs     []net.Conn
	subbed   chan struct{}
	snapFail int // fail this many snapshot requests first
}

func newFakeHerdr(t *testing.T) *fakeHerdr {
	t.Helper()
	// A unix socket path has a short length limit, so not t.TempDir().
	dir, err := os.MkdirTemp("/tmp", "ht")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeHerdr{t: t, path: path, listener: l, subbed: make(chan struct{}, 8)}
	t.Cleanup(f.close)
	go f.serve()
	return f
}

func (f *fakeHerdr) close() {
	f.listener.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.subs {
		c.Close()
	}
	f.subs = nil
}

func (f *fakeHerdr) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeHerdr) handle(conn net.Conn) {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return
	}
	var req struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	_ = json.Unmarshal([]byte(line), &req)

	f.mu.Lock()
	defer f.mu.Unlock()
	reply := func(v any) {
		data, _ := json.Marshal(v)
		conn.Write(append(data, '\n'))
	}
	switch req.Method {
	case "events.subscribe":
		reply(map[string]any{"result": map[string]any{"type": "subscription_started"}})
		f.subs = append(f.subs, conn)
		f.subbed <- struct{}{}
		return // keep the connection open
	case "session.snapshot":
		if f.snapFail > 0 {
			f.snapFail--
			reply(map[string]any{"error": map[string]any{"code": "busy", "message": "try later"}})
			break
		}
		reply(map[string]any{"result": map[string]any{"snapshot": map[string]any{
			"tabs": f.tabs, "panes": f.panes, "layouts": f.layouts,
		}}})
	case "tab.rename":
		id, label := req.Params["tab_id"].(string), req.Params["label"].(string)
		f.renames = append(f.renames, id+"="+label)
		for _, tab := range f.tabs {
			if tab["tab_id"] == id {
				tab["label"] = label
			}
		}
		reply(map[string]any{"result": map[string]any{}})
	default:
		reply(map[string]any{"result": map[string]any{}})
	}
	conn.Close()
}

// emit pushes one event line to every subscriber.
func (f *fakeHerdr) emit(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.subs {
		c.Write([]byte(`{"event":"` + event + `","data":{}}` + "\n"))
	}
}

func (f *fakeHerdr) update(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeHerdr) renameLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.renames...)
}

// waitRenames waits until the rename log equals want.
func (f *fakeHerdr) waitRenames(want ...string) {
	f.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := f.renameLog()
		if reflect.DeepEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("renames = %q, want %q", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func tab(id, label string, panes int, focused bool) map[string]any {
	return map[string]any{"tab_id": id, "label": label, "pane_count": panes, "focused": focused}
}

func pane(id, tabID, title string, focused bool) map[string]any {
	p := map[string]any{"pane_id": id, "tab_id": tabID, "focused": focused}
	if title != "" {
		p["terminal_title_stripped"] = title
	}
	return p
}

func layout(tabID, focusedPane string) map[string]any {
	return map[string]any{"tab_id": tabID, "focused_pane_id": focusedPane}
}

func snapshotOf(t *testing.T, tabs, panes, layouts []map[string]any) *Snapshot {
	t.Helper()
	data, err := json.Marshal(map[string]any{"snapshot": map[string]any{
		"tabs": tabs, "panes": panes, "layouts": layouts,
	}})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := decodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestDecodeSnapshotRejectsMissingPayload(t *testing.T) {
	if _, err := decodeSnapshot([]byte(`{}`)); err == nil {
		t.Fatal("want an error for a response without a snapshot")
	}
}

func TestTabName(t *testing.T) {
	cases := []struct {
		name    string
		tab     map[string]any
		panes   []map[string]any
		layouts []map[string]any
		maxLen  int
		want    string
	}{
		{
			name:  "single pane names the tab, focused or not",
			tab:   tab("t1", "1", 1, false),
			panes: []map[string]any{pane("p1", "t1", "fish ~/code", false)},
			want:  "fish ~/code",
		},
		{
			name:  "pane without a title leaves the tab alone",
			tab:   tab("t1", "old", 1, true),
			panes: []map[string]any{pane("p1", "t1", "", true)},
			want:  "",
		},
		{
			name:  "whitespace title counts as no title",
			tab:   tab("t1", "old", 1, true),
			panes: []map[string]any{pane("p1", "t1", "   ", true)},
			want:  "",
		},
		{
			name: "multi-pane tab follows the layout's focused pane",
			tab:  tab("t1", "old", 2, false),
			panes: []map[string]any{
				pane("p1", "t1", "vim a.go", false),
				pane("p2", "t1", "fish ~", false),
			},
			layouts: []map[string]any{layout("t1", "p2")},
			want:    "fish ~",
		},
		{
			name: "another tab's pane never names this tab",
			tab:  tab("t1", "old", 2, true),
			panes: []map[string]any{
				pane("p1", "t1", "vim a.go", false),
				pane("p2", "t1", "fish ~", false),
				pane("p9", "t9", "other", true),
			},
			layouts: []map[string]any{layout("t1", "p9")},
			want:    "",
		},
		{
			name: "without layout data the focused tab uses the focused pane",
			tab:  tab("t1", "old", 2, true),
			panes: []map[string]any{
				pane("p1", "t1", "vim a.go", false),
				pane("p2", "t1", "fish ~", true),
			},
			want: "fish ~",
		},
		{
			name: "without layout data a background multi-pane tab is left alone",
			tab:  tab("t1", "old", 2, false),
			panes: []map[string]any{
				pane("p1", "t1", "vim a.go", false),
				pane("p2", "t1", "fish ~", false),
			},
			want: "",
		},
		{
			name:   "max_len cuts by character, not by byte",
			tab:    tab("t1", "1", 1, true),
			panes:  []map[string]any{pane("p1", "t1", "héllo wörld", true)},
			maxLen: 7,
			want:   "héllo w",
		},
		{
			name:  "no limit by default",
			tab:   tab("t1", "1", 1, true),
			panes: []map[string]any{pane("p1", "t1", "a very long terminal title that is not cut", true)},
			want:  "a very long terminal title that is not cut",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snap := snapshotOf(t, []map[string]any{c.tab}, c.panes, c.layouts)
			if got := tabName(snap.Tabs[0], snap, c.maxLen); got != c.want {
				t.Fatalf("tabName = %q, want %q", got, c.want)
			}
		})
	}
}

func TestSyncTabsRenamesOnlyTabsThatDiffer(t *testing.T) {
	f := newFakeHerdr(t)
	f.tabs = []map[string]any{
		tab("t1", "stale", 1, true),
		tab("t2", "fish ~", 1, false),  // already correct
		tab("t3", "keep me", 1, false), // pane has no title
	}
	f.panes = []map[string]any{
		pane("p1", "t1", "vim a.go", true),
		pane("p2", "t2", "fish ~", false),
		pane("p3", "t3", "", false),
	}

	for i := 0; i < 2; i++ { // the second pass must send nothing
		snap, err := FetchSnapshot(f.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := SyncTabs(f.path, snap, 0); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := f.renameLog(), []string{"t1=vim a.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("renames = %q, want %q", got, want)
	}
}

func testTimings() watchTimings {
	return watchTimings{
		Debounce:     10 * time.Millisecond,
		Retry:        20 * time.Millisecond,
		ReadDeadline: 5 * time.Second,
		BinaryPoll:   time.Hour,
	}
}

// startDaemon runs watchDaemon against the fake server and returns a channel
// that yields its result when it exits.
func startDaemon(t *testing.T, f *fakeHerdr, stateDir string) <-chan error {
	t.Helper()
	sync := func() error {
		snap, err := FetchSnapshot(f.path)
		if err != nil {
			return err
		}
		return SyncTabs(f.path, snap, 0)
	}
	done := make(chan error, 1)
	go func() { done <- watchDaemon(f.path, stateDir, "test", "", sync, testTimings()) }()
	return done
}

func waitDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not exit")
		return nil
	}
}

func TestWatchDaemonFollowsTitleAndPaneFocus(t *testing.T) {
	f := newFakeHerdr(t)
	f.tabs = []map[string]any{tab("t1", "1", 2, true)}
	f.panes = []map[string]any{
		pane("p1", "t1", "vim a.go", true),
		pane("p2", "t1", "fish ~", false),
	}
	f.layouts = []map[string]any{layout("t1", "p1")}

	done := startDaemon(t, f, t.TempDir())
	<-f.subbed

	// The initial pass, with no event at all.
	f.waitRenames("t1=vim a.go")

	// The active pane's title changes.
	f.update(func() { f.panes[0]["terminal_title_stripped"] = "vim b.go" })
	f.emit("pane_updated")
	f.waitRenames("t1=vim a.go", "t1=vim b.go")

	// Focus moves to the other pane of the same tab.
	f.update(func() { f.layouts[0]["focused_pane_id"] = "p2" })
	f.emit("pane_focused")
	f.waitRenames("t1=vim a.go", "t1=vim b.go", "t1=fish ~")

	// The background pane's title changes: the tab keeps the active pane's.
	f.update(func() { f.panes[0]["terminal_title_stripped"] = "vim c.go" })
	f.emit("pane_updated")
	// Something else renames the tab: the next pass puts the title back.
	f.update(func() { f.tabs[0]["label"] = "by hand" })
	f.emit("tab_renamed")
	f.waitRenames("t1=vim a.go", "t1=vim b.go", "t1=fish ~", "t1=fish ~")

	// The server goes away: the daemon exits cleanly.
	f.close()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daemon exit: %v", err)
	}
}

func TestWatchDaemonRetriesAFailedPass(t *testing.T) {
	f := newFakeHerdr(t)
	f.tabs = []map[string]any{tab("t1", "1", 1, true)}
	f.panes = []map[string]any{pane("p1", "t1", "fish ~", true)}
	f.snapFail = 2

	done := startDaemon(t, f, t.TempDir())
	<-f.subbed
	// No event is sent: only the retry can deliver the rename.
	f.waitRenames("t1=fish ~")

	f.close()
	if err := waitDone(t, done); err != nil {
		t.Fatalf("daemon exit: %v", err)
	}
}

func TestWatchDaemonIsASingleton(t *testing.T) {
	f := newFakeHerdr(t)
	stateDir := t.TempDir()

	first := startDaemon(t, f, stateDir)
	<-f.subbed
	if !daemonAlive(stateDir, "test") {
		t.Fatal("daemonAlive = false while a daemon holds the lock")
	}

	// A second daemon for the same session exits at once, without subscribing.
	if err := waitDone(t, startDaemon(t, f, stateDir)); err != nil {
		t.Fatalf("second daemon: %v", err)
	}
	select {
	case <-f.subbed:
		t.Fatal("second daemon subscribed")
	default:
	}

	f.close()
	if err := waitDone(t, first); err != nil {
		t.Fatalf("first daemon exit: %v", err)
	}
	if daemonAlive(stateDir, "test") {
		t.Fatal("daemonAlive = true after the daemon exited")
	}
}

func TestWatchDaemonReportsARejectedSubscription(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ht")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		bufio.NewReader(conn).ReadString('\n')
		conn.Write([]byte(`{"error":{"code":"invalid_params","message":"unknown event type"}}` + "\n"))
		conn.Close()
	}()

	err = watchDaemon(path, t.TempDir(), "test", "", func() error { return nil }, testTimings())
	if err == nil {
		t.Fatal("want an error when the server rejects the subscription")
	}
}

func TestErrorLogSkipsRepeats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watch.log")
	log := &errorLog{path: path}
	boom := os.ErrDeadlineExceeded
	log.record(boom)
	log.record(boom) // repeat: not written
	log.record(nil)  // success resets the memory
	log.record(boom) // written again

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Fatalf("log has %d lines, want 2:\n%s", lines, data)
	}
}
