package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	cloudlogger "github.com/hkt999rtk/rtk_cloud_logger"
)

func main() {
	endpoint := flag.String("endpoint", os.Getenv("RTK_CLOUD_LOGGER_INGEST_URL"), "logger ingest URL")
	token := flag.String("token", os.Getenv("RTK_CLOUD_LOGGER_TOKEN"), "logger bearer token")
	cursorPath := flag.String("cursor", "/var/lib/rtk-cloud-logger/journal.cursor", "journal cursor path")
	spoolDir := flag.String("spool-dir", "/var/lib/rtk-cloud-logger/spool", "bounded spool directory")
	spoolBytes := flag.Int64("spool-max-bytes", 64*1024*1024, "maximum local spool bytes")
	units := flag.String("units", os.Getenv("RTK_CLOUD_LOGGER_UNITS"), "comma-separated systemd units")
	emqxDockerContainer := flag.String("emqx-docker-container", os.Getenv("RTK_CLOUD_LOGGER_EMQX_DOCKER_CONTAINER"), "EMQX Docker container to forward when broker verbose trace is enabled")
	initialSince := flag.String("initial-since", firstNonEmpty(os.Getenv("RTK_CLOUD_LOGGER_INITIAL_SINCE"), "5m"), "initial source window when no cursor exists")
	service := flag.String("service", os.Getenv("SERVICE"), "default service name")
	env := flag.String("env", os.Getenv("ENV"), "default environment")
	version := flag.String("version", os.Getenv("VERSION"), "default version")
	once := flag.Bool("once", false, "run one forwarding cycle and exit")
	interval := flag.Duration("interval", 5*time.Second, "poll interval")
	batchSize := flag.Int("batch-size", 100, "max records per batch")
	flag.Parse()

	if *endpoint == "" {
		log.Fatal("-endpoint or RTK_CLOUD_LOGGER_INGEST_URL is required")
	}
	var source cloudlogger.EventSource
	if *emqxDockerContainer != "" {
		source = cloudlogger.DockerLogsSource{
			Container:    *emqxDockerContainer,
			InitialSince: *initialSince,
			Config: cloudlogger.DockerLogParseConfig{
				Container:   *emqxDockerContainer,
				Service:     firstNonEmpty(*service, "emqx-broker"),
				Env:         *env,
				Version:     firstNonEmpty(*version, "emqx"),
				Host:        firstNonEmpty(os.Getenv("HOSTNAME"), "mqtt-host"),
				Unit:        "emqx.service",
				Source:      "emqx",
				Component:   "mqtt-broker",
				OperationID: "mqtt-broker-trace",
			},
		}
	} else {
		source = cloudlogger.JournalctlSource{
			Units: splitCSV(*units),
			Config: cloudlogger.JournalParseConfig{
				DefaultService: *service,
				DefaultEnv:     *env,
				DefaultVersion: *version,
			},
		}
	}
	forwarder := cloudlogger.NewForwarder(
		source,
		cloudlogger.HTTPSink{Endpoint: *endpoint, Token: *token},
		cloudlogger.FileCursorStore{Path: *cursorPath},
		cloudlogger.ForwarderConfig{BatchSize: *batchSize},
	).WithSpool(cloudlogger.FileSpool{Dir: *spoolDir, MaxBytes: *spoolBytes})

	ctx := context.Background()
	for {
		if err := forwarder.RunOnce(ctx); err != nil {
			log.Printf("forwarder degraded: %v", err)
		}
		if *once {
			break
		}
		time.Sleep(*interval)
	}
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
