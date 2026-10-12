package core

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDurableWriteFile_CreateAndReplace(t *testing.T) {
	for _, longPath := range []bool{false, true} {
		name := "short"
		if longPath {
			name = "long"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if longPath {
				dir = filepath.Join(dir, strings.Repeat("a", 90), strings.Repeat("b", 90), strings.Repeat("c", 90))
			}
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "会话.json")
			for _, data := range []string{`{"phase":"sending"}`, `{"phase":"model_turn_completed"}`} {
				if err := durableWriteFile(path, []byte(data), 0o600); err != nil {
					t.Fatalf("save snapshot: %v", err)
				}
				got, err := os.ReadFile(path)
				if err != nil || string(got) != data {
					t.Fatalf("read snapshot: %q, %v", got, err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("unexpected temporary files: %v, %v", entries, err)
			}
		})
	}
}

func TestDurableWriteFile_ReplaceFailurePreservesDestination(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(path, "existing")
	if err := os.WriteFile(marker, []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := durableWriteFile(path, []byte("replacement"), 0o600); err == nil {
		t.Fatal("replacement of a directory must fail")
	}
	got, err := os.ReadFile(marker)
	if err != nil || string(got) != "preserved" {
		t.Fatalf("destination changed: %q, %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "snapshot" {
		t.Fatalf("failed replacement left temporary files: %v, %v", entries, err)
	}
}
