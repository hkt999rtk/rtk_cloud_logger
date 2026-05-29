package cloudlogger

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Config struct {
	Service     string
	Env         string
	Version     string
	Level       string
	Development bool
}

func New(cfg Config) (*zap.Logger, error) {
	zapCfg := zap.NewProductionConfig()
	if cfg.Development {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.Encoding = "json"
	}
	zapCfg.Level = zap.NewAtomicLevelAt(ParseLevel(cfg.Level))
	zapCfg.OutputPaths = []string{"stdout"}
	zapCfg.ErrorOutputPaths = []string{"stderr"}
	zapCfg.InitialFields = initialFields(cfg)

	return zapCfg.Build()
}

func MustNew(cfg Config) *zap.Logger {
	logger, err := New(cfg)
	if err != nil {
		panic(err)
	}
	return logger
}

func Nop() *zap.Logger {
	return zap.NewNop()
}

func ParseLevel(level string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "dpanic":
		return zapcore.DPanicLevel
	case "panic":
		return zapcore.PanicLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

func initialFields(cfg Config) map[string]any {
	fields := make(map[string]any, 3)
	if value := strings.TrimSpace(cfg.Service); value != "" {
		fields["service"] = value
	}
	if value := strings.TrimSpace(cfg.Env); value != "" {
		fields["env"] = value
	}
	if value := strings.TrimSpace(cfg.Version); value != "" {
		fields["version"] = value
	}
	return fields
}
