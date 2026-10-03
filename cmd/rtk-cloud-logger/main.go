package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	cloudlogger "github.com/hkt999rtk/rtk_cloud_logger"
	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
)

func main() {
	addr := flag.String("addr", ":18090", "listen address")
	token := flag.String("token", os.Getenv("RTK_CLOUD_LOGGER_TOKEN"), "bearer token required by forwarders")
	billingToken := flag.String("billing-token", os.Getenv("RTK_CLOUD_LOGGER_BILLING_USAGE_TOKEN"), "dedicated bearer token for billing_usage stream")
	billingPath := flag.String("billing-inbox", os.Getenv("RTK_CLOUD_LOGGER_BILLING_INBOX"), "absolute retained billing inbox file (private parent directory required)")
	initializeBilling := flag.Bool("initialize-billing-inbox", false, "explicitly initialize a new empty billing inbox; never use to replace lost storage")
	lifecycleConfig := flag.String("billing-lifecycle-config", os.Getenv("RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_CONFIG"), "private public-only backup lifecycle JSON config; disabled when omitted")
	lifecycleToken := flag.String("billing-lifecycle-token", os.Getenv("RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_TOKEN"), "distinct internal lifecycle controller bearer token")
	lifecycleReadToken := flag.String("billing-lifecycle-read-token", os.Getenv("RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_READ_TOKEN"), "distinct read-only retirement status credential")
	lifecycleAddr := flag.String("billing-lifecycle-listen-addr", os.Getenv("RTK_CLOUD_LOGGER_BILLING_LIFECYCLE_LISTEN_ADDR"), "private internal lifecycle listener (default :8081), never expose via public ingress")
	recoveryMode := flag.Bool("billing-inbox-recovery-mode", os.Getenv("RTK_CLOUD_LOGGER_BILLING_INBOX_RECOVERY_MODE") == "true", "durably fence a restored inbox until independent signed recovery admission")
	lokiURL := flag.String("loki-url", os.Getenv("RTK_CLOUD_LOGGER_LOKI_URL"), "Loki base URL")
	flag.Parse()

	store, err := cloudlogger.NewLokiEventStore(cloudlogger.LokiStoreConfig{BaseURL: *lokiURL})
	if err != nil {
		log.Fatal(err)
	}
	var inbox *cloudlogger.BillingInbox
	if *billingToken != "" {
		if *billingToken == *token {
			log.Fatal("billing and operational credentials must differ")
		}
		inbox, err = cloudlogger.OpenBillingInbox(*billingPath, *initializeBilling)
		if err != nil {
			log.Fatal("billing inbox unavailable; inspect retained storage and initialization policy")
		}
		defer inbox.Close()
		if *recoveryMode {
			if err := inbox.SetRecoveryMode(context.Background()); err != nil {
				log.Fatal("cannot persist restored-inbox admission fence")
			}
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *lifecycleConfig != "" {
		if inbox == nil || *lifecycleToken == "" || *lifecycleToken == *billingToken || *lifecycleToken == *token {
			log.Fatal("lifecycle needs retained inbox and distinct control credential")
		}
		info, err := os.Lstat(*lifecycleConfig)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
			log.Fatal("private bounded lifecycle config required")
		}
		body, err := os.ReadFile(*lifecycleConfig)
		if err != nil {
			log.Fatal("lifecycle config unreadable")
		}
		var cfg struct {
			Lifecycle   cloudlogger.LifecycleConfig `json:"lifecycle"`
			ObjectStore cloudlogger.S3ArchiveConfig `json:"object_store"`
		}
		if err := billingarchive.StrictDecode(bytes.NewReader(body), 64<<10, &cfg); err != nil {
			log.Fatal("invalid lifecycle config")
		}
		cfg.Lifecycle.AuthorityToken = os.Getenv("RTK_CLOUD_LOGGER_BILLING_RETENTION_AUTHORITY_TOKEN")
		if cfg.Lifecycle.AuthorityToken == *lifecycleToken || cfg.Lifecycle.AuthorityToken == *billingToken || cfg.Lifecycle.AuthorityToken == *token {
			log.Fatal("authority read credential must be isolated")
		}
		if cfg.ObjectStore.Environment != cfg.Lifecycle.Environment {
			log.Fatal("backup environment mismatch")
		}
		if cfg.Lifecycle.BackupEnabled {
			writer, err := cloudlogger.NewS3ArchiveStore(cfg.ObjectStore, os.Getenv("RTK_CLOUD_LOGGER_BILLING_BACKUP_ACCESS_KEY_ID"), os.Getenv("RTK_CLOUD_LOGGER_BILLING_BACKUP_SECRET_ACCESS_KEY"))
			if err != nil {
				log.Fatal(err)
			}
			cfg.Lifecycle.ObjectStore = writer
		}
		interval := os.Getenv("RTK_CLOUD_LOGGER_BILLING_BACKUP_INTERVAL")
		if interval == "" {
			interval = "12h"
		}
		cfg.Lifecycle.CaptureInterval, err = time.ParseDuration(interval)
		if err != nil {
			log.Fatal("invalid billing backup interval")
		}
		if err := inbox.ConfigureLifecycle(cfg.Lifecycle); err != nil {
			log.Fatal(err)
		}
		if *lifecycleReadToken == "" || *lifecycleReadToken == *lifecycleToken || *lifecycleReadToken == *billingToken || *lifecycleReadToken == *token || *lifecycleReadToken == cfg.Lifecycle.AuthorityToken {
			log.Fatal("isolated terminal status read credential required")
		}
		if cfg.Lifecycle.BackupEnabled {
			go inbox.RunBackupWorker(ctx)
		}
	}
	config := cloudlogger.IngestConfig{Token: *token, BillingToken: *billingToken, BillingInbox: inbox, LifecycleToken: *lifecycleToken, LifecycleReadToken: *lifecycleReadToken}
	server := &http.Server{Addr: *addr, Handler: cloudlogger.IngestHandler(store, config), ReadHeaderTimeout: 10 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	var lifecycleServer *http.Server
	if *lifecycleConfig != "" {
		if *lifecycleAddr == "" {
			*lifecycleAddr = ":8081"
		}
		if *lifecycleAddr == *addr {
			log.Fatal("lifecycle listener must be separate from public ingest")
		}
		lifecycleServer = &http.Server{Addr: *lifecycleAddr, Handler: cloudlogger.LifecycleHandler(config), ReadHeaderTimeout: 10 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
		go func() {
			if err := lifecycleServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal("private lifecycle listener unavailable")
			}
		}()
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if lifecycleServer != nil {
			_ = lifecycleServer.Shutdown(shutdown)
		}
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("starting rtk-cloud-logger on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
