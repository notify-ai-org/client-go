package notify

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config mirrors the Java NotifyProperties (prefix notify.ai.properties).
type Config struct {
	// ACPServerURL is the acp-server (control plane) base URL.
	// Default https://app.notify-ai.dev.
	ACPServerURL string
	// ApplicationName is sent at registration. Default "notify-client".
	ApplicationName string
	// ClientToken identifies this client to acp-server (sent as clientId at
	// registration, as the Java SDK does).
	ClientToken string
	// BasePackage is reported at registration; for Go use the module or
	// package path of the instrumented service.
	BasePackage string

	// BufferBatchSize is the maximum number of records sent per dispatch. Default 100.
	BufferBatchSize int
	// FlushInterval is how often the dispatcher drains the buffer. Default 100ms.
	FlushInterval time.Duration
	// MaxBufferSize caps buffered records; the oldest event capture is
	// dropped when full. Default 10000. Negative means unbounded.
	MaxBufferSize int

	// KafkaEnabled switches event delivery to Kafka (and enables the
	// scheduled-event consumer). Requires WithKafka.
	KafkaEnabled bool

	// HTTPClient is used for acp-server calls. Default: 10s dial timeout,
	// per-request timeouts as in the Java SDK.
	HTTPClient *http.Client
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.ACPServerURL == "" {
		c.ACPServerURL = "https://app.notify-ai.dev"
	}
	c.ACPServerURL = strings.TrimRight(c.ACPServerURL, "/")
	if c.ApplicationName == "" {
		c.ApplicationName = "notify-client"
	}
	if c.BufferBatchSize <= 0 {
		c.BufferBatchSize = 100
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
	if c.MaxBufferSize == 0 {
		c.MaxBufferSize = 10_000
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// ConfigFromEnv reads configuration from environment variables, the Go
// counterpart of notify.ai.properties.* in application.properties:
//
//	NOTIFY_AI_ACP_SERVER_URL, NOTIFY_AI_APPLICATION_NAME, NOTIFY_AI_CLIENT_TOKEN,
//	NOTIFY_AI_BASE_PACKAGE, NOTIFY_AI_BUFFER_BATCH_SIZE,
//	NOTIFY_AI_FLUSH_INTERVAL_MS, NOTIFY_AI_MAX_BUFFER_SIZE, NOTIFY_AI_KAFKA_ENABLED
func ConfigFromEnv() Config {
	atoi := func(k string) int { n, _ := strconv.Atoi(os.Getenv(k)); return n }
	enabled, _ := strconv.ParseBool(os.Getenv("NOTIFY_AI_KAFKA_ENABLED"))
	return Config{
		ACPServerURL:    os.Getenv("NOTIFY_AI_ACP_SERVER_URL"),
		ApplicationName: os.Getenv("NOTIFY_AI_APPLICATION_NAME"),
		ClientToken:     os.Getenv("NOTIFY_AI_CLIENT_TOKEN"),
		BasePackage:     os.Getenv("NOTIFY_AI_BASE_PACKAGE"),
		BufferBatchSize: atoi("NOTIFY_AI_BUFFER_BATCH_SIZE"),
		FlushInterval:   time.Duration(atoi("NOTIFY_AI_FLUSH_INTERVAL_MS")) * time.Millisecond,
		MaxBufferSize:   atoi("NOTIFY_AI_MAX_BUFFER_SIZE"),
		KafkaEnabled:    enabled,
	}
}
