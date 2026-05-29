package cloudlogger_test

import (
	"net/http"

	cloudlogger "github.com/hkt999rtk/rtk_cloud_logger"
	"go.uber.org/zap"
)

func ExampleNew() {
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
}

func ExampleHTTPMiddleware() {
	logger := cloudlogger.Nop()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler := cloudlogger.HTTPMiddleware(logger)(mux)
	_ = handler
}
