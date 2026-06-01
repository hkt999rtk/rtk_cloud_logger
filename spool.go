package cloudlogger

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type Spool interface {
	Enqueue(context.Context, []LogEvent) error
	Flush(context.Context, EventSink) error
}

type FileSpool struct {
	Dir      string
	MaxBytes int64
}

func (s FileSpool) Enqueue(_ context.Context, events []LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return err
	}
	name := time.Now().UTC().Format("20060102T150405.000000000") + ".json"
	path := filepath.Join(s.Dir, name)
	data, err := json.Marshal(IngestRequest{Events: events})
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}
	return s.enforceLimit()
}

func (s FileSpool) Flush(ctx context.Context, sink EventSink) error {
	entries, err := s.entries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := os.ReadFile(entry.path)
		if err != nil {
			return err
		}
		var request IngestRequest
		if err := json.Unmarshal(data, &request); err != nil {
			return err
		}
		if err := sink.Send(ctx, request.Events); err != nil {
			return err
		}
		if err := os.Remove(entry.path); err != nil {
			return err
		}
	}
	return nil
}

func (s FileSpool) enforceLimit() error {
	if s.MaxBytes <= 0 {
		return nil
	}
	entries, err := s.entries()
	if err != nil {
		return err
	}
	var total int64
	for _, entry := range entries {
		total += entry.size
	}
	for _, entry := range entries {
		if total <= s.MaxBytes {
			break
		}
		if err := os.Remove(entry.path); err != nil {
			return err
		}
		total -= entry.size
	}
	return nil
}

type spoolEntry struct {
	path string
	size int64
}

func (s FileSpool) entries() ([]spoolEntry, error) {
	entries, err := os.ReadDir(s.Dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]spoolEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		out = append(out, spoolEntry{path: filepath.Join(s.Dir, entry.Name()), size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out, nil
}
