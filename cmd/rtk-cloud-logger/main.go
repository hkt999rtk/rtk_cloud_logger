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
	lokiURL := flag.String("loki-url", os.Getenv("RTK_CLOUD_LOGGER_LOKI_URL"), "Loki base URL")
	flag.Parse()

	store, err := cloudlogger.NewLokiEventStore(cloudlogger.LokiStoreConfig{BaseURL: *lokiURL})
	if err != nil {
		log.Fatal(err)
	}
	handler := cloudlogger.IngestHandler(store, cloudlogger.IngestConfig{Token: *token})
	log.Printf("starting rtk-cloud-logger on %s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
