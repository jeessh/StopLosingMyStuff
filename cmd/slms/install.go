package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jeessh/StopLosingMyStuff/internal/vfs"
)

const launchLabel = "com.stoplosing.slms"

var hookEvents = []string{"SessionStart", "SessionEnd", "PreCompact"}

func install(args []string) error {
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	if bin, err = filepath.EvalSymlinks(bin); err != nil {
		return err
	}
	if err := installHooks(filepath.Join(home(), ".claude", "settings.json"), bin); err != nil {
		return err
	}
	fmt.Println("added Claude Code hooks")
	if len(args) > 0 && args[0] == "--no-launchd" {
		return nil
	}
	return installLaunchd(bin)
}

// installHooks edits settings.json only by adding entries, keeps a backup, and
// stops if the file isn't valid JSON rather than replacing it.
func installHooks(path, bin string) error {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		path = real
	}
	settings := map[string]any{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, &settings); err != nil {
			return fmt.Errorf("%s is not valid JSON, leaving it alone: %w", path, err)
		}
		if err := os.WriteFile(path+".slms-backup", data, 0o600); err != nil {
			return err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return err
	}
	hooks, _ := settings["hooks"].(map[string]any)
	if hooks == nil {
		hooks = map[string]any{}
	}
	for _, ev := range hookEvents {
		list, _ := hooks[ev].([]any)
		cmd := fmt.Sprintf("%q hook %s", bin, ev)
		if !hasHook(list, " hook "+ev) {
			list = append(list, map[string]any{
				"hooks": []any{map[string]any{"type": "command", "command": cmd, "timeout": 5}},
			})
		}
		hooks[ev] = list
	}
	settings["hooks"] = hooks
	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return vfs.WriteAtomic(vfs.OS{}, path, append(out, '\n'))
}

func hasHook(list []any, suffix string) bool {
	for _, group := range list {
		g, _ := group.(map[string]any)
		inner, _ := g["hooks"].([]any)
		for _, h := range inner {
			m, _ := h.(map[string]any)
			if c, _ := m["command"].(string); strings.Contains(c, "slms") && strings.HasSuffix(c, suffix) {
				return true
			}
		}
	}
	return false
}

func installLaunchd(bin string) error {
	path := filepath.Join(home(), "Library", "LaunchAgents", launchLabel+".plist")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir(), 0o700); err != nil {
		return err
	}
	if err := vfs.WriteAtomic(vfs.OS{}, path, []byte(plist(bin, dataDir(), os.Getenv("PATH")))); err != nil {
		return err
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	_ = exec.Command("launchctl", "bootout", domain+"/"+launchLabel).Run()
	if out, err := exec.Command("launchctl", "bootstrap", domain, path).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl: %v: %s", err, out)
	}
	fmt.Println("daemon will start at login and restart if it stops")
	return nil
}

func plist(bin, dir, path string) string {
	e := html.EscapeString
	logFile := filepath.Join(dir, "daemon.log")
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>` + launchLabel + `</string>
  <key>ProgramArguments</key>
  <array><string>` + e(bin) + `</string><string>daemon</string></array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>` + e(path) + `</string>
    <key>SLMS_HOME</key><string>` + e(dir) + `</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>` + e(logFile) + `</string>
  <key>StandardErrorPath</key><string>` + e(logFile) + `</string>
</dict>
</plist>
`
}
