# herdr-terminal-titles

A [herdr](https://herdr.dev) plugin that sets each tab's label to the terminal
title of the tab's active pane. It does nothing else.

This is a reduced fork of
[davidolrik/herdr-titles](https://github.com/davidolrik/herdr-titles) (MIT).
The fork removes the window title template, program and agent naming, icons,
the shell hooks, the environment harvest, and manual-rename tracking.

## Behavior

- A tab with one pane takes that pane's terminal title.
- A tab with several panes takes the title of the pane that has focus in that
  tab. When focus moves to another pane, the label changes to that pane's
  title.
- A pane that has set no terminal title does not change the label. The tab
  keeps the label it had.
- The plugin owns every tab label. A label that you or another tool sets is
  replaced by the terminal title on the next pass.
- The plugin does not touch the OS window title. Set that with `window_title`
  in herdr's `config.toml`.

Your shell or program must set a terminal title. fish does this by default
(`fish_title`); zsh and bash do not.

## How it works

herdr starts `herdr-titles watch` at startup. That process starts a detached
daemon, one for each herdr session. The daemon subscribes to herdr's event
stream. After each burst of events it reads a snapshot of the session and
renames each tab whose label differs from its active pane's title. Event
payloads are not used, so a stale event cannot apply a stale title.

Two event hooks (`workspace.focused`, `tab.focused`) run `herdr-titles
watchdog`, which starts a new daemon if the old one has died. The daemon also
watches its own binary and restarts itself after a rebuild.

The daemon has no stderr. It writes errors to `watch.<session>.log` in the
plugin state directory (`~/.local/state/herdr/plugins/chriszarate.titles/`).

## Install

```sh
go build -o bin/herdr-titles .
herdr plugin link /path/to/herdr-titles
```

`herdr plugin link` does not run the build step, so build first, and build
again after each change.

## Configure

There is no config file. Every choice is a constant in `config.go`: the label
length limit (`maxLabelLen`, 40 characters), the daemon timings, and the list
of herdr events the daemon follows. Change a value and rebuild.

## Commands

| Command                 | What it does                                |
| ----------------------- | ------------------------------------------- |
| `herdr-titles sync`     | Run one pass now                            |
| `herdr-titles watch`    | Start the detached daemon (herdr runs this) |
| `herdr-titles watchdog` | Start the daemon if it is not running       |

`herdr plugin action invoke chriszarate.terminal-titles.sync` also runs one pass.

## Development

```sh
go test -race ./...
```
