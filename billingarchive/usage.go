package billingarchive

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// These wire types intentionally match Video Cloud usage.Event field order.
// Keep the shared contract vectors synchronized before extending the registry.
type canonicalUsageMeasurement struct {
	MetricCode    string `json:"metric_code"`
	Unit          string `json:"unit"`
	Quantity      uint64 `json:"quantity"`
	QuantityScale int    `json:"quantity_scale,omitempty"`
}

type canonicalUsageEvent struct {
	UsageID      string                      `json:"usage_id"`
	ServiceCode  string                      `json:"service_code"`
	BrandCloudID string                      `json:"brand_cloud_id"`
	EventTime    time.Time                   `json:"event_time"`
	WindowStart  time.Time                   `json:"window_start"`
	WindowEnd    time.Time                   `json:"window_end"`
	MeterEpoch   string                      `json:"meter_epoch"`
	Sequence     uint64                      `json:"sequence"`
	Source       string                      `json:"source"`
	Measurements []canonicalUsageMeasurement `json:"measurements"`
	Dimensions   map[string]string           `json:"dimensions,omitempty"`
}

// CanonicalUsageBinding derives the exact consumer reconciliation input from
// an independently verified archived usage_event object. Logger sequence and
// financial envelope digest remain separate from the canonical Event digest.
func CanonicalUsageBinding(sequence uint64, loggerDigest string, usageRaw json.RawMessage) (RecordBinding, error) {
	var out RecordBinding
	if sequence == 0 || sequence > math.MaxInt64 || !validSHA(loggerDigest) {
		return out, errors.New("invalid billing usage record binding")
	}
	var event canonicalUsageEvent
	if err := StrictDecode(strings.NewReader(string(usageRaw)), 1<<20, &event); err != nil {
		return out, err
	}
	event.UsageID = strings.TrimSpace(event.UsageID)
	event.ServiceCode = strings.TrimSpace(event.ServiceCode)
	event.BrandCloudID = strings.TrimSpace(event.BrandCloudID)
	event.MeterEpoch = strings.TrimSpace(event.MeterEpoch)
	event.Source = strings.TrimSpace(event.Source)
	event.EventTime = event.EventTime.UTC()
	event.WindowStart = event.WindowStart.UTC()
	event.WindowEnd = event.WindowEnd.UTC()
	if event.UsageID == "" {
		event.UsageID = deterministicArchivedUsageID(event)
	}
	if err := event.validate(); err != nil {
		return out, err
	}
	// Match PostgreSQL precision only after deterministic producer ID derivation.
	event.EventTime = event.EventTime.Truncate(time.Microsecond)
	event.WindowStart = event.WindowStart.Truncate(time.Microsecond)
	event.WindowEnd = event.WindowEnd.Truncate(time.Microsecond)
	sort.Slice(event.Measurements, func(i, j int) bool { return event.Measurements[i].MetricCode < event.Measurements[j].MetricCode })
	if err := event.validate(); err != nil {
		return out, err
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return out, err
	}
	return RecordBinding{Sequence: sequence, LoggerContentSHA256: loggerDigest, UsageID: event.UsageID, EventSHA256: Digest(raw)}, nil
}

func (e canonicalUsageEvent) validate() error {
	if e.UsageID == "" || len(e.UsageID) > 128 || e.ServiceCode == "" || e.BrandCloudID == "" || e.MeterEpoch == "" || e.Source == "" ||
		e.EventTime.IsZero() || e.WindowStart.IsZero() || e.WindowEnd.IsZero() || !e.WindowEnd.After(e.WindowStart) || e.Sequence > math.MaxInt64 || len(e.Measurements) == 0 {
		return errors.New("invalid canonical billing usage event")
	}
	seen := map[string]bool{}
	for _, measurement := range e.Measurements {
		if seen[measurement.MetricCode] || measurement.Quantity > math.MaxInt64 || !archivedMetricAllowed(e.ServiceCode, measurement) {
			return errors.New("invalid or unknown billing measurement")
		}
		seen[measurement.MetricCode] = true
	}
	return nil
}

func archivedMetricAllowed(service string, m canonicalUsageMeasurement) bool {
	if service == "mqtt" && m.QuantityScale == 0 {
		switch m.MetricCode {
		case "publish_bytes", "delivery_bytes":
			return m.Unit == "bytes"
		case "publish_count", "delivery_count":
			return m.Unit == "requests"
		}
	}
	if service == "ota" {
		switch m.MetricCode {
		case "device_task":
			return m.Unit == "tasks" && m.QuantityScale == 0
		case "successful_download_gib":
			return m.Unit == "GiB" && m.QuantityScale == 9
		case "artifact_storage_gib_month":
			return m.Unit == "GiB-month" && m.QuantityScale == 9
		case "artifact_write":
			return m.Unit == "requests" && m.QuantityScale == 0
		}
	}
	return false
}

func deterministicArchivedUsageID(e canonicalUsageEvent) string {
	keys := make([]string, 0, len(e.Dimensions))
	for key := range e.Dimensions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	input := strings.Builder{}
	for _, value := range []string{e.ServiceCode, e.BrandCloudID, e.MeterEpoch, fmt.Sprint(e.Sequence), e.WindowStart.UTC().Format(time.RFC3339Nano), e.WindowEnd.UTC().Format(time.RFC3339Nano)} {
		if input.Len() > 0 {
			input.WriteByte(0)
		}
		input.WriteString(value)
	}
	for _, key := range keys {
		input.WriteByte(0)
		input.WriteString(key)
		input.WriteByte('=')
		input.WriteString(e.Dimensions[key])
	}
	return "usage-" + Digest([]byte(input.String()))[:24]
}
