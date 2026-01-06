package logger

import (
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

var (
	logWriteBytesTotal atomic.Uint64

	logFileSizeBytes  atomic.Uint64
	logTotalSizeBytes atomic.Uint64

	logDiskTotalBytes atomic.Uint64
	logDiskUsedBytes  atomic.Uint64
)

type countingWriteSyncer struct {
	next zapcore.WriteSyncer
}

func newCountingWriteSyncer(next zapcore.WriteSyncer) zapcore.WriteSyncer {
	return &countingWriteSyncer{next: next}
}

func (s *countingWriteSyncer) Write(p []byte) (int, error) {
	n, err := s.next.Write(p)
	if n > 0 {
		logWriteBytesTotal.Add(uint64(n))
	}
	return n, err
}

func (s *countingWriteSyncer) Sync() error {
	return s.next.Sync()
}

func LogWriteBytesTotal() uint64 {
	return logWriteBytesTotal.Load()
}

func LogFileSizeBytes() uint64 {
	return logFileSizeBytes.Load()
}

func LogTotalSizeBytes() uint64 {
	return logTotalSizeBytes.Load()
}

func LogDiskTotalBytes() uint64 {
	return logDiskTotalBytes.Load()
}

func LogDiskUsedBytes() uint64 {
	return logDiskUsedBytes.Load()
}

func setLogFileSizeBytes(v uint64) {
	logFileSizeBytes.Store(v)
}

func setLogTotalSizeBytes(v uint64) {
	logTotalSizeBytes.Store(v)
}

func setLogDiskTotalBytes(v uint64) {
	logDiskTotalBytes.Store(v)
}

func setLogDiskUsedBytes(v uint64) {
	logDiskUsedBytes.Store(v)
}
