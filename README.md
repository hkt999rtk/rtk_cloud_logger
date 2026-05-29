# RTK Cloud Logger

Shared logging package for RTK cloud Go services.

This module owns the common zap logger configuration used by service and worker
entrypoints across the RTK cloud repositories. It emits single-line JSON logs to
stdout so deployment agents can collect service logs from journald, Docker, or
file-based sinks.

## Defaults

- backend: `go.uber.org/zap`
- style: typed `*zap.Logger` fields, not `SugaredLogger`
- encoding: zap production JSON
- static fields: `service`, `env`, `version`
- level names: `debug`, `info`, `warn`, `error`

## Usage

```go
logger, err := cloudlogger.New(cloudlogger.Config{
	Service: "video-cloud-api",
	Env:     "staging",
	Version: "v0.1.0",
	Level:   "info",
})
if err != nil {
	panic(err)
}
defer logger.Sync()

logger.Info("starting service", zap.String("addr", "127.0.0.1:18080"))
```
