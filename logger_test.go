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
		"dpanic":  zapcore.DPanicLevel,
		"panic":   zapcore.PanicLevel,
		"fatal":   zapcore.FatalLevel,
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
		Unit:    "video-cloud-api.service",
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
	if event["service"] != "video-cloud-api" || event["env"] != "staging" || event["version"] != "test-version" || event["unit"] != "video-cloud-api.service" {
		t.Fatalf("missing service fields: %#v", event)
	}
	if event["level"] != "info" || event["msg"] != "started" || event["addr"] != "127.0.0.1:18080" {
		t.Fatalf("unexpected log event: %#v", event)
	}
}

func TestZapConfigUsesProductionJSONContract(t *testing.T) {
	cfg := newZapConfig(Config{
		Service:     " video-cloud-api ",
		Env:         " staging ",
		Version:     " v1.2.3 ",
		Unit:        " video-cloud-api.service ",
		Level:       "debug",
		Development: true,
	})

	if cfg.Encoding != "json" {
		t.Fatalf("Encoding = %q, want json", cfg.Encoding)
	}
	if got := cfg.OutputPaths; len(got) != 1 || got[0] != "stdout" {
		t.Fatalf("OutputPaths = %#v, want [stdout]", got)
	}
	if got := cfg.ErrorOutputPaths; len(got) != 1 || got[0] != "stderr" {
		t.Fatalf("ErrorOutputPaths = %#v, want [stderr]", got)
	}
	if cfg.EncoderConfig.TimeKey != "ts" || cfg.EncoderConfig.LevelKey != "level" || cfg.EncoderConfig.MessageKey != "msg" {
		t.Fatalf("unexpected encoder keys: %#v", cfg.EncoderConfig)
	}
	if cfg.EncoderConfig.CallerKey != "caller" || cfg.EncoderConfig.StacktraceKey != "stacktrace" {
		t.Fatalf("unexpected caller/stacktrace keys: %#v", cfg.EncoderConfig)
	}
	if cfg.InitialFields["service"] != "video-cloud-api" || cfg.InitialFields["env"] != "staging" || cfg.InitialFields["version"] != "v1.2.3" || cfg.InitialFields["unit"] != "video-cloud-api.service" {
		t.Fatalf("InitialFields not trimmed or incomplete: %#v", cfg.InitialFields)
	}
}

func TestNewForTestEmitsSingleLineJSONWithProductionFields(t *testing.T) {
	var out bytes.Buffer
	logger, err := newForTest(Config{Service: "api", Level: "info"}, zapcore.AddSync(&out))
	if err != nil {
		t.Fatalf("New test logger: %v", err)
	}

	logger.Info("started")

	if got := bytes.Count(out.Bytes(), []byte("\n")); got != 1 {
		t.Fatalf("log output should be one JSON line, got %d newlines in %q", got, out.String())
	}

	event := decodeLogEvent(t, out.Bytes())
	for _, key := range []string{"ts", "level", "msg", "caller"} {
		if _, ok := event[key]; !ok {
			t.Fatalf("missing production encoder field %q in %#v", key, event)
		}
	}
}

func TestPublicLoggerConstructors(t *testing.T) {
	logger, err := New(Config{Service: "coverage", Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	logger.Error("constructor coverage")
	_ = logger.Sync()
	if got := MustNew(Config{Service: "must"}); got == nil {
		t.Fatal("MustNew returned nil")
	}
	if got := Nop(); got == nil {
		t.Fatal("Nop returned nil")
	}
}

func newForTest(cfg Config, sink zapcore.WriteSyncer) (*zap.Logger, error) {
	encoderCfg := zap.NewProductionEncoderConfig()
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), sink, ParseLevel(cfg.Level))
	return zap.New(core, zap.AddCaller()).With(fields(cfg)...), nil
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

func decodeLogEvent(t *testing.T, line []byte) map[string]any {
	t.Helper()

	var event map[string]any
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatalf("log output is not JSON: %v\n%s", err, string(line))
	}
	return event
}
