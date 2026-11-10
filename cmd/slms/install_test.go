package main

import (
	"encoding/json"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallHooksKeepsExistingSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	orig := `{"model":"x","hooks":{"Stop":[{"hooks":[{"type":"command","command":"ding"}]}],"SessionStart":[{"hooks":[{"type":"command","command":"mine"}]}]}}`
	if err := os.WriteFile(path, []byte(orig), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := installHooks(path, "/bin/slms"); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := os.ReadFile(path)
	var got struct {
		Model string                      `json:"model"`
		Hooks map[string][]map[string]any `json:"hooks"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Model != "x" || len(got.Hooks["Stop"]) != 1 || len(got.Hooks["SessionStart"]) != 2 || len(got.Hooks["PreCompact"]) != 1 {
		t.Fatalf("settings = %s", data)
	}
	backup, _ := os.ReadFile(path + ".slms-backup")
	if !strings.Contains(string(backup), "ding") {
		t.Fatal("no backup")
	}
}

func TestInstallHooksRefusesBrokenSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	_ = os.WriteFile(path, []byte("{broken"), 0o600)
	if err := installHooks(path, "/bin/slms"); err == nil {
		t.Fatal("overwrote invalid settings")
	}
	if data, _ := os.ReadFile(path); string(data) != "{broken" {
		t.Fatal("file changed")
	}
}

func TestPlistIsValidXMLWithOddPaths(t *testing.T) {
	out := plist(`/Users/a&b/slms`, `/Users/a&b/.stoplosing`, `/bin:/a<b`)
	d := xml.NewDecoder(strings.NewReader(out))
	d.Strict = false
	for {
		if _, err := d.Token(); err != nil {
			if err.Error() == "EOF" {
				return
			}
			t.Fatal(err)
		}
	}
}

func TestInstallHooksKeepsASymlink(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "dotfiles.json")
	link := filepath.Join(dir, "settings.json")
	_ = os.WriteFile(real, []byte(`{}`), 0o600)
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := installHooks(link, "/bin/slms"); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Lstat(link); fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink was replaced")
	}
	if data, _ := os.ReadFile(real); !strings.Contains(string(data), "SessionStart") {
		t.Fatalf("target not updated: %s", data)
	}
}
