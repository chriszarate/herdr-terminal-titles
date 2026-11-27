package main

// The per-session watch daemon: subscribes to herdr's socket event stream
// (including pane.updated, which herdr refuses to deliver to plugin [[events]]
// hooks) and runs one sync pass after every burst of events.
//
// Lifecycle: herdr's [[startup]] spawns `watch`, which re-execs itself as
// `watch --detached` (setsid, /dev/null stdio) so herdr's wait() returns and
// no in-flight plugin slot stays pinned. The detached daemon holds a
// per-session flock for its lifetime — which doubles as the liveness probe
// the watchdog hooks use to revive it — and exits when the event stream ends
// (server gone; the next server's startup respawns it).
//
// An event is only a nudge. Herdr hands the stream out at a bounded rate and
// a fresh subscription first replays buffered history, so an event's payload
// can be stale. The daemon therefore ignores payloads: every pass reads a
// fresh snapshot and applies the titles the panes carry right now.

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type watchTimings struct {
	Debounce     time.Duration // wait after the first event of a burst
	Retry        time.Duration // wait before retrying a failed pass
	ReadDeadline time.Duration // quiet time before the server is pinged
	BinaryPoll   time.Duration // how often the daemon checks its own binary
}

func defaultWatchTimings() watchTimings {
	return watchTimings{
		Debounce:     syncDebounce,
		Retry:        syncRetry,
		ReadDeadline: streamQuietLimit,
		BinaryPoll:   binaryPoll,
	}
}

// runScheduler turns wake-ups into debounced sync passes. The first event of
// a burst starts the timer and later ones do not push it back, so a storm
// still gets a pass every Debounce. A failed pass is retried after Retry,
// because the events that made it due will not be sent again.
func runScheduler(wake <-chan struct{}, sync func() error, timings watchTimings, stop <-chan struct{}) {
	var timer <-chan time.Time
	for {
		select {
		case <-stop:
			return
		case <-wake:
			if timer == nil {
				timer = time.After(timings.Debounce)
			}
		case <-timer:
			timer = nil
			if err := sync(); err != nil {
				timer = time.After(timings.Retry)
			}
		}
	}
}

// flockNB takes a nonblocking exclusive lock on an open file.
func flockNB(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// watchLockPath names the daemon's per-session liveness lock.
func watchLockPath(stateDir, session string) string {
	return filepath.Join(stateDir, "watch.lock."+session)
}

// restartSelf replaces the running daemon with the binary at path — the same
// PID execs the rebuilt image, and Go's CLOEXEC fds release the singleton
// flock atomically at exec so the new image re-acquires it without a race. If
// the exec fails (binary gone: unlink), the daemon simply dies. A var so
// tests can stub the exec.
var restartSelf = func(exePath string) {
	_ = syscall.Exec(exePath, []string{exePath, "watch", "--detached"}, os.Environ())
	os.Exit(0)
}

// binaryIdentity is the executable's change-detection fingerprint.
type binaryIdentity struct {
	modTime time.Time
	size    int64
}

// fingerprintBinary stats the executable; nil when it cannot be read now.
func fingerprintBinary(exePath string) *binaryIdentity {
	info, err := os.Stat(exePath)
	if err != nil {
		return nil
	}
	return &binaryIdentity{info.ModTime(), info.Size()}
}

// binaryWatcher polls the daemon's own executable and hands the process over
// to a replaced binary, so a rebuild takes effect without a manual restart.
// A single stat error is tolerated (a build replaces the file with a brief
// gap); two consecutive misses mean the plugin is gone and the handoff
// doubles as an orderly exit.
func binaryWatcher(exePath string, baseline *binaryIdentity, interval time.Duration, stop <-chan struct{}) {
	misses := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			id := fingerprintBinary(exePath)
			if id == nil {
				if misses++; misses >= 2 {
					restartSelf(exePath)
					return
				}
				continue
			}
			misses = 0
			if baseline == nil {
				baseline = id
				continue
			}
			if *id != *baseline {
				restartSelf(exePath)
				return
			}
		}
	}
}

// watchDaemon is the detached daemon body: singleton lock, subscribe, one
// initial pass, then a pass after every burst of events until the stream
// ends. binPath (when non-empty) is the daemon's own executable, watched so
// a rebuild restarts the daemon onto the new binary. A held lock and a dead
// server are orderly exits and return nil; a refused subscription is an
// error.
func watchDaemon(sockPath, stateDir, session, binPath string, sync func() error, timings watchTimings) error {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	lockFile, err := os.OpenFile(watchLockPath(stateDir, session), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lockFile.Close()
	if err := flockNB(lockFile); err != nil {
		return nil // another daemon is alive; that's the desired state
	}

	// Fingerprint the binary BEFORE subscribing: anything that replaces it
	// after this point — however quickly — is detected as a change.
	var baseline *binaryIdentity
	if binPath != "" {
		baseline = fingerprintBinary(binPath)
	}

	conn, reader, err := subscribeEvents(sockPath, watchSubscriptions, timings.ReadDeadline)
	if err != nil {
		return err
	}
	defer conn.Close()

	stop := make(chan struct{})
	done := make(chan struct{})
	wake := make(chan struct{}, 1)
	go func() { runScheduler(wake, sync, timings, stop); close(done) }()
	if binPath != "" {
		go binaryWatcher(binPath, baseline, timings.BinaryPoll, stop)
	}

	// The initial pass: bring every tab up to date before the first event.
	wake <- struct{}{}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(timings.ReadDeadline))
		if _, err := reader.ReadString('\n'); err != nil {
			if isTimeout(err) && pingSocket(sockPath) {
				continue // quiet session, server alive: keep streaming
			}
			break // EOF or dead server: exit; the watchdog hooks revive us
		}
		// A full buffer means a pass is already due, and that pass reads a
		// snapshot newer than this event: dropping the wake-up loses nothing.
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	close(stop)
	<-done
	return nil
}

// errSubscribeRejected is a definitive server rejection (an event type this
// herdr does not know), as opposed to a transport error.
var errSubscribeRejected = errors.New("events.subscribe rejected")

// subscribeEvents dials the session socket and subscribes to the given event
// types, verifying the server actually confirmed. Herdr rejects the whole
// call on any unknown type.
func subscribeEvents(sockPath string, subs []string, readDeadline time.Duration) (net.Conn, *bufio.Reader, error) {
	conn, err := net.DialTimeout("unix", sockPath, socketTimeout)
	if err != nil {
		return nil, nil, err
	}
	parts := make([]string, 0, len(subs))
	for _, s := range subs {
		parts = append(parts, fmt.Sprintf(`{"type":%q}`, s))
	}
	payload := fmt.Sprintf(`{"id":"watch","method":"events.subscribe","params":{"subscriptions":[%s]}}`,
		strings.Join(parts, ","))
	if _, err := fmt.Fprintf(conn, "%s\n", payload); err != nil {
		conn.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
	ack, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("events.subscribe: %w", err)
	}
	if !strings.Contains(ack, "subscription_started") {
		conn.Close()
		return nil, nil, fmt.Errorf("%w: %s", errSubscribeRejected, strings.TrimSpace(ack))
	}
	return conn, reader, nil
}

// pingSocket asks the server for a ping on a fresh connection.
func pingSocket(sockPath string) bool {
	_, err := apiRequest(sockPath, "ping", nil)
	return err == nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// runWatchParent is what herdr's [[startup]] spawns: hand off to a detached
// daemon so herdr's wait() returns.
func runWatchParent() error {
	if os.Getenv("HERDR_SOCKET_PATH") == "" {
		return nil // not spawned by a herdr server
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer devnull.Close()
	cmd := exec.Command(self, "watch", "--detached")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devnull, devnull, devnull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd.Start()
}

// errorLog records the daemon's errors in the state dir, because a detached
// daemon has no stderr. A repeat of the previous message is not written
// again: a persistent failure (the server refuses snapshots, say) is retried every few
// seconds and must not grow the file without bound.
type errorLog struct {
	path string
	last string
}

func (l *errorLog) record(err error) {
	if err == nil {
		l.last = ""
		return
	}
	msg := err.Error()
	if msg == l.last {
		return
	}
	l.last = msg
	f, openErr := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if openErr != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s %s\n", time.Now().Format(time.RFC3339), msg)
}

// runWatchDetached wires the daemon to the real sync pass.
func runWatchDetached() error {
	sockPath := os.Getenv("HERDR_SOCKET_PATH")
	if sockPath == "" {
		return nil
	}
	session := envOr("HERDR_SESSION", "default")
	stateDir := pluginStateDir()
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	exePath, _ := os.Executable()
	log := &errorLog{path: filepath.Join(stateDir, "watch."+session+".log")}
	sync := func() error {
		err := runSync()
		log.record(err)
		return err
	}
	err := watchDaemon(sockPath, stateDir, session, exePath, sync, defaultWatchTimings())
	log.record(err)
	return err
}

// daemonAlive probes the daemon's liveness lock: if we can take it, nobody
// holds it — the daemon is dead. Microseconds, no I/O beyond open+flock.
func daemonAlive(stateDir, session string) bool {
	f, err := os.OpenFile(watchLockPath(stateDir, session), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := flockNB(f); err != nil {
		return true // held: a daemon is alive
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

// runWatchdog handles the manifest's event hooks. With a live daemon it is a
// no-op: the daemon already saw the event. With a dead one it starts a new
// daemon, whose initial pass covers the gap. Races between concurrent
// revivers are settled by the daemon's own singleton lock.
func runWatchdog() error {
	if os.Getenv("HERDR_SOCKET_PATH") == "" {
		return nil // nothing to watch without a server socket
	}
	stateDir := pluginStateDir()
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	if daemonAlive(stateDir, envOr("HERDR_SESSION", "default")) {
		return nil
	}
	return runWatchParent()
}
