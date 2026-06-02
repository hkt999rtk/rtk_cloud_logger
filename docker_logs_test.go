package cloudlogger

import (
	"strings"
	"testing"
)

func TestParseDockerLogRecordsBuildsEMQXBrokerTraceEvent(t *testing.T) {
	input := strings.NewReader("2026-06-03T01:02:03.123456789Z [debug] clientid=load-device-0041 action=publish topic=$vc/devices/load-device-0041/shadow/update qos=0\n")
	records, err := ParseDockerLogRecords(input, 10, DockerLogParseConfig{
		Container:   "video-cloud-emqx",
		Service:     "emqx-broker",
		Env:         "staging",
		Version:     "emqx",
		Host:        "mqtt-host",
		Unit:        "emqx.service",
		Source:      "emqx",
		Component:   "mqtt-broker",
		OperationID: "mqtt-broker-trace",
	})
	if err != nil {
		t.Fatalf("ParseDockerLogRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	record := records[0]
	if record.Cursor != "2026-06-03T01:02:03.123456789Z" {
		t.Fatalf("cursor = %q", record.Cursor)
	}
	event := record.Event
	if event.Service != "emqx-broker" || event.Source != "emqx" || event.Component != "mqtt-broker" || event.OperationID != "mqtt-broker-trace" {
		t.Fatalf("unexpected event labels: %+v", event)
	}
	if event.Level != "debug" {
		t.Fatalf("level = %q, want debug", event.Level)
	}
	if event.Message != "emqx broker mqtt trace" {
		t.Fatalf("message = %q", event.Message)
	}
	if got := event.Fields["log_line"].(string); !strings.Contains(got, "action=publish") || !strings.Contains(got, "topic=$vc/devices/load-device-0041/shadow/update") {
		t.Fatalf("log_line missing broker detail: %q", got)
	}
}

func TestParseDockerLogRecordRedactsSensitiveLogLine(t *testing.T) {
	record, err := ParseDockerLogRecord("2026-06-03T01:02:03Z [debug] password=secret token=abc payload=ignored", DockerLogParseConfig{
		Container: "video-cloud-emqx",
		Service:   "emqx-broker",
		Env:       "staging",
		Version:   "emqx",
		Host:      "mqtt-host",
		Unit:      "emqx.service",
		Source:    "emqx",
	})
	if err != nil {
		t.Fatalf("ParseDockerLogRecord: %v", err)
	}
	if got := record.Event.Fields["log_line"]; got != RedactedValue {
		t.Fatalf("log_line = %q, want redacted", got)
	}
}
