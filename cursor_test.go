package cloudlogger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCursorStoresRoundTripAndErrors(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryCursorStore()
	if got, err := memory.Load(ctx); err != nil || got != "" {
		t.Fatalf("initial memory cursor = %q, %v", got, err)
	}
	if err := memory.Save(ctx, "memory-cursor"); err != nil {
		t.Fatal(err)
	}
	if got := memory.Cursor(); got != "memory-cursor" {
		t.Fatalf("Cursor() = %q", got)
	}

	path := filepath.Join(t.TempDir(), "nested", "cursor")
	file := FileCursorStore{Path: path}
	if got, err := file.Load(ctx); err != nil || got != "" {
		t.Fatalf("missing file cursor = %q, %v", got, err)
	}
	if err := file.Save(ctx, "file-cursor"); err != nil {
		t.Fatal(err)
	}
	if got, err := file.Load(ctx); err != nil || got != "file-cursor" {
		t.Fatalf("file cursor = %q, %v", got, err)
	}
	bad := FileCursorStore{Path: filepath.Join(path, "child")}
	if _, err := bad.Load(ctx); err == nil {
		t.Fatal("loading a path below a file unexpectedly passed")
	}
	if err := bad.Save(ctx, "cursor"); err == nil {
		t.Fatal("saving a path below a file unexpectedly passed")
	}
	if mode := mustFileMode(t, path); mode.Perm() != 0o600 {
		t.Fatalf("cursor mode = %v", mode.Perm())
	}
}

func mustFileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode()
}
