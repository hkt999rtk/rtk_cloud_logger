package billingarchive

import (
	"encoding/json"
	"strings"
	"testing"
)

const canonicalUsageVector = `{"usage_id":"usage-vector","service_code":"mqtt","brand_cloud_id":"43962cfa-a092-4368-b2c7-32925b0ef54f","event_time":"2026-10-03T01:02:03.123456789Z","window_start":"2026-10-03T01:01:03.123456789Z","window_end":"2026-10-03T01:02:03.123456789Z","meter_epoch":"epoch-vector","sequence":9007199254740993,"source":"meter","measurements":[{"metric_code":"publish_bytes","unit":"bytes","quantity":9007199254740993},{"metric_code":"delivery_bytes","unit":"bytes","quantity":2}]}`

func TestCanonicalUsageBindingMatchesVideoCloudContractVector(t *testing.T) {
	ref, err := CanonicalUsageBinding(9007199254740993, strings.Repeat("c", 64), json.RawMessage(canonicalUsageVector))
	if err != nil || ref.Sequence != 9007199254740993 || ref.UsageID != "usage-vector" || ref.EventSHA256 != "467ecdbbe93d2667385d8cd71340b0e7a5884eb143814d6f017fc117632e8cde" {
		t.Fatal("Video Cloud canonical event mismatch", ref, err)
	}
	// Host JSON formatting and dimensions map insertion order are not financial.
	other := strings.Replace(canonicalUsageVector, `"usage-vector"`, `" usage-vector "`, 1)
	ref2, err := CanonicalUsageBinding(ref.Sequence, ref.LoggerContentSHA256, json.RawMessage(other))
	if err != nil || ref2 != ref {
		t.Fatal(ref2, err)
	}
}

func TestCanonicalUsageBindingRejectsUnknownAndLossyEvidence(t *testing.T) {
	for _, raw := range []string{
		canonicalUsageVector + `{}`,
		strings.Replace(canonicalUsageVector, `"source":`, `"unknown":1,"source":`, 1),
		strings.Replace(canonicalUsageVector, `9007199254740993`, `9007199254740993.0`, 1),
		strings.Replace(canonicalUsageVector, `9007199254740993`, `9223372036854775808`, 1),
		strings.Replace(canonicalUsageVector, `"publish_bytes"`, `"unknown_metric"`, 1),
		strings.Replace(canonicalUsageVector, `"publish_bytes"`, `"delivery_bytes"`, 1),
		strings.Replace(canonicalUsageVector, `"window_start":"2026-10-03T01:01:03.123456789Z"`, `"window_start":"2026-10-03T01:02:03.123456001Z"`, 1),
	} {
		if _, err := CanonicalUsageBinding(1, strings.Repeat("a", 64), json.RawMessage(raw)); err == nil {
			t.Fatal("invalid usage binding accepted", raw)
		}
	}
	for _, sequence := range []uint64{0, ^uint64(0)} {
		if _, err := CanonicalUsageBinding(sequence, strings.Repeat("a", 64), json.RawMessage(canonicalUsageVector)); err == nil {
			t.Fatal("invalid sequence accepted")
		}
	}
}
