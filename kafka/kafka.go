// Package kafka is the Kafka transport for the Notify.ai Go SDK, built on
// franz-go. It is the counterpart of the Java SDK's KafkaProducer /
// KafkaConsumer setup (KafkaConfig + Bootstrapper#initializeKafkaClients).
//
//	client := notify.New(notify.Config{..., KafkaEnabled: true},
//	    notify.WithKafka(kafka.Factory(kafka.Config{Brokers: []string{"broker:9092"}})))
//
// The SDK decides topic, partition, key and headers; this package only
// writes records and consumes scheduled events.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/notify-ai-org/client-go/notify"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// DefaultBroker matches the Java SDK's kafka.bootstrap-servers default.
const DefaultBroker = "pkc-41p56.asia-south1.gcp.confluent.cloud:9092"

// Config mirrors the Java SDK's kafka.* properties.
type Config struct {
	// Brokers defaults to DefaultBroker.
	Brokers []string
	// GroupID for the scheduled-event consumer. Default "vocab-agent-group".
	GroupID string
	// ClientID defaults to "<clientToken>-producer" / "<clientToken>-consumer",
	// or "notify-ai-client" when no client token is configured.
	ClientID string

	// SecurityProtocol: PLAINTEXT (default), SSL, SASL_PLAINTEXT or SASL_SSL.
	// When acp-server returns an API key and secret at registration they
	// take precedence: SASL_SSL with PLAIN, as in the Java SDK.
	SecurityProtocol string
	// SASLMechanism: PLAIN (default), SCRAM-SHA-256 or SCRAM-SHA-512.
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	// TLSConfig is used for SSL / SASL_SSL. Default: system roots.
	TLSConfig *tls.Config

	// Compression: snappy (default), lz4, gzip, zstd or none.
	Compression string
	// Linger is the producer batching delay. Default 5ms.
	Linger time.Duration
	// ResetToLatest starts a new consumer group at the latest offset instead
	// of the earliest (Java default: earliest).
	ResetToLatest bool

	Logger *slog.Logger
	// ExtraOptions are appended to both producer and consumer client options.
	ExtraOptions []kgo.Opt
}

// Factory returns a notify.KafkaFactory for notify.WithKafka.
func Factory(cfg Config) notify.KafkaFactory {
	return func(ctx context.Context, creds notify.KafkaCredentials) (notify.KafkaTransport, error) {
		return New(cfg, creds)
	}
}

// Transport implements notify.KafkaTransport.
type Transport struct {
	cfg      Config
	base     []kgo.Opt
	producer *kgo.Client

	mu        sync.Mutex
	consumers []*kgo.Client
}

// New builds a transport. creds come from the acp-server registration.
func New(cfg Config, creds notify.KafkaCredentials) (*Transport, error) {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{DefaultBroker}
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "vocab-agent-group"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Linger <= 0 {
		cfg.Linger = 5 * time.Millisecond
	}
	if creds.APIKey != "" && creds.APISecret != "" {
		cfg.SecurityProtocol, cfg.SASLMechanism = "SASL_SSL", "PLAIN"
		cfg.SASLUsername, cfg.SASLPassword = creds.APIKey, creds.APISecret
	}

	base := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	security, err := securityOpts(cfg)
	if err != nil {
		return nil, err
	}
	base = append(base, security...)

	compression, err := compressionCodec(cfg.Compression)
	if err != nil {
		return nil, err
	}
	producerOpts := append(append([]kgo.Opt{}, base...),
		kgo.ClientID(clientID(cfg, creds, "producer")),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compression),
		kgo.ProducerLinger(cfg.Linger),
		kgo.RecordDeliveryTimeout(2*time.Minute),
	)
	producerOpts = append(producerOpts, cfg.ExtraOptions...)
	producer, err := kgo.NewClient(producerOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	t := &Transport{cfg: cfg, base: base, producer: producer}
	t.base = append(t.base, kgo.ClientID(clientID(cfg, creds, "consumer")))
	return t, nil
}

func clientID(cfg Config, creds notify.KafkaCredentials, role string) string {
	switch {
	case cfg.ClientID != "":
		return cfg.ClientID
	case creds.ClientID != "":
		return creds.ClientID + "-" + role
	default:
		return "notify-ai-client"
	}
}

func securityOpts(cfg Config) ([]kgo.Opt, error) {
	protocol := strings.ToUpper(strings.TrimSpace(cfg.SecurityProtocol))
	var opts []kgo.Opt
	switch protocol {
	case "", "PLAINTEXT":
		return nil, nil
	case "SSL", "SASL_SSL":
		tlsCfg := cfg.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	case "SASL_PLAINTEXT":
	default:
		return nil, fmt.Errorf("kafka: unsupported security protocol %q", cfg.SecurityProtocol)
	}
	if strings.HasPrefix(protocol, "SASL") {
		mech, err := saslMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

func saslMechanism(cfg Config) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.SASLMechanism) {
	case "", "PLAIN":
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", cfg.SASLMechanism)
}

func compressionCodec(name string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(name) {
	case "", "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	}
	return kgo.CompressionCodec{}, fmt.Errorf("kafka: unsupported compression %q", name)
}

// Partitions returns the partition count of topic from cluster metadata.
func (t *Transport) Partitions(ctx context.Context, topic string) (int, error) {
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, t.producer)
	if err != nil {
		return 0, err
	}
	for _, mt := range resp.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			if err := kerrFor(mt.ErrorCode); err != nil {
				return 0, err
			}
			return len(mt.Partitions), nil
		}
	}
	return 0, fmt.Errorf("kafka: topic %q not found", topic)
}

// Produce writes records synchronously to their pre-chosen partitions.
func (t *Transport) Produce(ctx context.Context, records []notify.KafkaRecord) error {
	recs := make([]*kgo.Record, 0, len(records))
	for _, r := range records {
		rec := &kgo.Record{Topic: r.Topic, Partition: r.Partition, Key: []byte(r.Key), Value: r.Value}
		for k, v := range r.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs = append(recs, rec)
	}
	return t.producer.ProduceSync(ctx, recs...).FirstErr()
}

// Consume joins the consumer group on topic and hands each record value to
// handle, committing offsets after every poll, until ctx is done.
func (t *Transport) Consume(ctx context.Context, topic string, handle func(context.Context, []byte)) error {
	reset := kgo.NewOffset().AtStart()
	if t.cfg.ResetToLatest {
		reset = kgo.NewOffset().AtEnd()
	}
	opts := append(append([]kgo.Opt{}, t.base...),
		kgo.ConsumerGroup(t.cfg.GroupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
	)
	opts = append(opts, t.cfg.ExtraOptions...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka: consumer: %w", err)
	}
	t.mu.Lock()
	t.consumers = append(t.consumers, cl)
	t.mu.Unlock()
	defer cl.Close()

	t.cfg.Logger.Info("kafka: consuming scheduled events", "topic", topic, "group", t.cfg.GroupID)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				t.cfg.Logger.Warn("kafka: fetch error", "topic", topic, "partition", partition, "error", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) { handle(ctx, r.Value) })
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			t.cfg.Logger.Warn("kafka: offset commit failed", "error", err)
		}
	}
}

// Close flushes and closes the producer and any consumers.
func (t *Transport) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := t.producer.Flush(ctx)
	t.producer.Close()
	t.mu.Lock()
	for _, c := range t.consumers {
		c.Close()
	}
	t.consumers = nil
	t.mu.Unlock()
	return err
}
