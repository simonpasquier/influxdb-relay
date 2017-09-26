package relay

import (
	"fmt"
	"log"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type Service struct {
	relays map[string]Relay
	totalRequestsDesc *prometheus.Desc
	failedRequestsDesc *prometheus.Desc
	receivedPointsDesc *prometheus.Desc
	sentBytesDesc *prometheus.Desc
	failedBytesDesc *prometheus.Desc
	bufferBytesDesc *prometheus.Desc
}

func New(config Config) (*Service, error) {
	s := new(Service)
	s.relays = make(map[string]Relay)

	for _, cfg := range config.HTTPRelays {
		h, err := NewHTTP(cfg)
		if err != nil {
			return nil, err
		}
		if s.relays[h.Name()] != nil {
			return nil, fmt.Errorf("duplicate relay: %q", h.Name())
		}
		s.relays[h.Name()] = h
	}

	for _, cfg := range config.UDPRelays {
		u, err := NewUDP(cfg)
		if err != nil {
			return nil, err
		}
		if s.relays[u.Name()] != nil {
			return nil, fmt.Errorf("duplicate relay: %q", u.Name())
		}
		s.relays[u.Name()] = u
	}
	s.totalRequestsDesc = prometheus.NewDesc(
		"influxdb_relay_requests_total",
		"Number of total requests processed by the relay service",
		[]string{"name"},
		nil,
	)
	s.failedRequestsDesc = prometheus.NewDesc(
		"influxdb_relay_failed_requests_total",
		"Number of unsuccessful requests processed by the relay service",
		[]string{"name"},
		nil,
	)
	s.receivedPointsDesc = prometheus.NewDesc(
		"influxdb_relay_received_points_total",
		"Number of points received by the HTTP relay service",
		[]string{"name"},
		nil,
	)
	s.sentBytesDesc = prometheus.NewDesc(
		"influxdb_relay_backend_sent_bytes_total",
		"Number of bytes sent successfully to the InfluxDB backend",
		[]string{"name", "backend"},
		nil,
	)
	s.failedBytesDesc = prometheus.NewDesc(
		"influxdb_relay_backend_failed_bytes_total",
		"Number of bytes rejected by the InfluxDB backend",
		[]string{"name", "backend"},
		nil,
	)
	s.bufferBytesDesc = prometheus.NewDesc(
		"influxdb_relay_backend_buffer_bytes",
		"Number of buffered bytes waiting to be sent to the InfluxDB backend",
		[]string{"name", "backend"},
		nil,
	)

	return s, nil
}

func (s *Service) Run() {
	var wg sync.WaitGroup
	wg.Add(len(s.relays))

	for k := range s.relays {
		relay := s.relays[k]
		go func() {
			defer wg.Done()

			if err := relay.Run(); err != nil {
				log.Printf("Error running relay %q: %v", relay.Name(), err)
			}
		}()
	}

	prometheus.MustRegister(s)

	wg.Wait()
}

func (s *Service) Stop() {
	for _, v := range s.relays {
		v.Stop()
	}
}

// Describe implements prometheus.Collector
func (s *Service) Describe(ch chan<- *prometheus.Desc) {
	ch <- s.totalRequestsDesc
	ch <- s.failedRequestsDesc
	ch <- s.receivedPointsDesc
	ch <- s.sentBytesDesc
	ch <- s.failedBytesDesc
	ch <- s.bufferBytesDesc
}

// Collect implements prometheus.Collector
func (s *Service) Collect(ch chan<- prometheus.Metric) {
	for k := range s.relays {
		if t, ok := interface{}(s.relays[k]).(TelemetryInterface); ok {
			ch <- prometheus.MustNewConstMetric(
				s.totalRequestsDesc,
				prometheus.CounterValue,
				t.TotalRequests(),
				k,
			)
			ch <- prometheus.MustNewConstMetric(
				s.failedRequestsDesc,
				prometheus.CounterValue,
				t.FailedRequests(),
				k,
			)
			ch <- prometheus.MustNewConstMetric(
				s.receivedPointsDesc,
				prometheus.CounterValue,
				t.ReceivedPoints(),
				k,
			)
			for backend, v := range t.SentBytes() {
				ch <- prometheus.MustNewConstMetric(
					s.sentBytesDesc,
					prometheus.CounterValue,
					v,
					k,
					backend,
				)
			}
			for backend, v := range t.FailedBytes() {
				ch <- prometheus.MustNewConstMetric(
					s.failedBytesDesc,
					prometheus.CounterValue,
					v,
					k,
					backend,
				)
			}
			for backend, v := range t.BufferedBytes() {
				ch <- prometheus.MustNewConstMetric(
					s.bufferBytesDesc,
					prometheus.GaugeValue,
					v,
					k,
					backend,
				)
			}
		}
	}
}

type TelemetryInterface interface {
	TotalRequests() float64
	FailedRequests() float64
	ReceivedPoints() float64
	SentBytes() map[string]float64
	FailedBytes() map[string]float64
	BufferedBytes() map[string]float64
}

type Relay interface {
	Name() string
	Run() error
	Stop() error
}
