package metrics

import (
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	LoggerWriteBytesTotal = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "logger",
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written to TiCDC log file.",
		},
		func() float64 { return float64(logger.LogWriteBytesTotal()) },
	)

	LoggerFileSizeBytes = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "logger",
			Name:      "file_size_bytes",
			Help:      "Size of the current TiCDC log file.",
		},
		func() float64 { return float64(logger.LogFileSizeBytes()) },
	)

	LoggerTotalSizeBytes = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "logger",
			Name:      "total_size_bytes",
			Help:      "Total size of the TiCDC log file and its rotated backups in the same directory.",
		},
		func() float64 { return float64(logger.LogTotalSizeBytes()) },
	)

	LoggerDiskTotalBytes = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "logger",
			Name:      "disk_total_bytes",
			Help:      "Total size of the filesystem containing TiCDC log file directory.",
		},
		func() float64 { return float64(logger.LogDiskTotalBytes()) },
	)

	LoggerDiskUsedBytes = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "logger",
			Name:      "disk_used_bytes",
			Help:      "Used bytes of the filesystem containing TiCDC log file directory.",
		},
		func() float64 { return float64(logger.LogDiskUsedBytes()) },
	)
)

func initLoggerMetrics(registry *prometheus.Registry) {
	registry.MustRegister(LoggerWriteBytesTotal)
	registry.MustRegister(LoggerFileSizeBytes)
	registry.MustRegister(LoggerTotalSizeBytes)
	registry.MustRegister(LoggerDiskTotalBytes)
	registry.MustRegister(LoggerDiskUsedBytes)
}
