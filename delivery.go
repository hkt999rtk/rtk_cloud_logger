package cloudlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type HTTPDelivery struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewHTTPDelivery(endpoint, token string, client *http.Client) *HTTPDelivery {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPDelivery{endpoint: endpoint, token: token, client: client}
}

func (d *HTTPDelivery) Deliver(ctx context.Context, events []LogEvent) ([]IngestResult, error) {
	body, err := json.Marshal(IngestRequest{Events: events})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.token != "" {
		req.Header.Set("Authorization", "Bearer "+d.token)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ingest status %d", resp.StatusCode)
	}
	var response IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response.Results, nil
}
