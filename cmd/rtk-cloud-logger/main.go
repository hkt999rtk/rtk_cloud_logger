package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	cloudlogger "github.com/hkt999rtk/rtk_cloud_logger"
)

func main() {
	addr := flag.String("addr", ":18090", "listen address")
	token := flag.String("token", os.Getenv("RTK_CLOUD_LOGGER_TOKEN"), "bearer token required by forwarders")
	storeKind := flag.String("store", firstNonEmpty(os.Getenv("RTK_CLOUD_LOGGER_STORE"), "memory"), "event store: memory or loki")
	lokiURL := flag.String("loki-url", os.Getenv("RTK_CLOUD_LOGGER_LOKI_URL"), "Loki base URL")
	flag.Parse()

	store := cloudlogger.EventStore(cloudlogger.NewMemoryEventStore())
	if *storeKind == "loki" {
		loki, err := cloudlogger.NewLokiEventStore(cloudlogger.LokiStoreConfig{BaseURL: *lokiURL})
		if err != nil {
			log.Fatal(err)
		}
		store = loki
	}
	handler := cloudlogger.IngestHandler(store, cloudlogger.IngestConfig{Token: *token})
	log.Printf("starting rtk-cloud-logger on %s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
