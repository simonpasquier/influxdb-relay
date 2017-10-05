package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/influxdata/influxdb-relay/relay"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	configFile = flag.String("config", "", "Configuration file to use")
)

const (
	DefaultMetricsPath = "/metrics"
)

func main() {
	flag.Parse()

	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "Missing configuration file")
		flag.PrintDefaults()
		os.Exit(1)
	}

	cfg, err := relay.LoadConfigFile(*configFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Problem loading config file:", err)
	}

	r, err := relay.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.TelemetryEnabled() {
		path := cfg.Telemetry.Path
		if path == "" {
			path = DefaultMetricsPath
		}
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		log.Printf("starting telemetry on %s%s", cfg.Telemetry.Addr, path)
		go func() {
			http.Handle(path, promhttp.Handler())
			log.Fatal(http.ListenAndServe(cfg.Telemetry.Addr, nil))
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		r.Stop()
	}()

	log.Println("starting relays...")
	r.Run()
}
