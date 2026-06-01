package cloudlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type HTTPSink struct {
	Endpoint string
	Token    string
	Client   *http.Client
}

func (s HTTPSink) Send(ctx context.Context, events []LogEvent) error {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(IngestRequest{Events: events}); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("ingest failed: status=%d", resp.StatusCode)
	}
	return nil
}
