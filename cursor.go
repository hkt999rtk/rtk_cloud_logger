package cloudlogger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type CursorStore interface {
	Load(context.Context) (string, error)
	Save(context.Context, string) error
}

type MemoryCursorStore struct {
	mu     sync.Mutex
	cursor string
}

func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{}
}

func (s *MemoryCursorStore) Load(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor, nil
}

func (s *MemoryCursorStore) Save(_ context.Context, cursor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor = cursor
	return nil
}

func (s *MemoryCursorStore) Cursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

type FileCursorStore struct {
	Path string
}

func (s FileCursorStore) Load(context.Context) (string, error) {
	data, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func (s FileCursorStore) Save(_ context.Context, cursor string) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.Path, []byte(cursor+"\n"), 0o600)
}
