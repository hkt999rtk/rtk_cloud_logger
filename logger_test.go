package cloudlogger

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestParseLevel(t *testing.T) {
	tests := map[string]zapcore.Level{
		"":        zapcore.InfoLevel,
		"INFO":    zapcore.InfoLevel,
		"debug":   zapcore.DebugLevel,
		"warning": zapcore.WarnLevel,
		"warn":    zapcore.WarnLevel,
		"error":   zapcore.ErrorLevel,
	}

	for input, want := range tests {
		if got := ParseLevel(input); got != want {
			t.Fatalf("ParseLevel(%q) = %s, want %s", input, got, want)
		}
	}
}

func TestNewEmitsJSONWithServiceFields(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{
		Service: "video-cloud-api",
		Env:     "staging",
		Version: "test-version",
		Level:   "debug",
	}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	logger.Info("started", zap.String("addr", "127.0.0.1:18080"))

	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatalf("log output is not JSON: %v\n%s", err, out.String())
	}
	if event["service"] != "video-cloud-api" || event["env"] != "staging" || event["version"] != "test-version" {
		t.Fatalf("missing service fields: %#v", event)
	}
	if event["level"] != "info" || event["msg"] != "started" || event["addr"] != "127.0.0.1:18080" {
		t.Fatalf("unexpected log event: %#v", event)
	}
}

func newForTest(cfg Config, sink zapcore.WriteSyncer) (*zap.Logger, error) {
	encoderCfg := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), sink, ParseLevel(cfg.Level))
	return zap.New(core).With(fields(cfg)...), nil
}

func fields(cfg Config) []zap.Field {
	initial := initialFields(cfg)
	out := make([]zap.Field, 0, len(initial))
	for key, value := range initial {
		if text, ok := value.(string); ok {
			out = append(out, zap.String(key, text))
		}
	}
	return out
}
