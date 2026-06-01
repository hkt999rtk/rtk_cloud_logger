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
	flag.Parse()

	handler := cloudlogger.IngestHandler(cloudlogger.NewMemoryEventStore(), cloudlogger.IngestConfig{Token: *token})
	log.Printf("starting rtk-cloud-logger on %s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatal(err)
	}
}
