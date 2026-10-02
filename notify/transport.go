package notify

import "context"

// Kafka topics shared with acp-server.
const (
	EventsTopic          = "notify-v1-events"
	ScheduledEventsTopic = "notify-v1-scheduled-events"
)

// KafkaRecord is a fully addressed record: the SDK chooses the partition and
// key exactly as the Java SDK does, the transport only writes it.
type KafkaRecord struct {
	Topic     string
	Partition int32
	Key       string
	Value     []byte
	Headers   map[string]string
}

// KafkaCredentials come from the acp-server registration response.
type KafkaCredentials struct {
	ClientID  string
	APIKey    string
	APISecret string
}

// KafkaTransport is implemented by github.com/notify-ai-org/client-go/kafka.
type KafkaTransport interface {
	// Partitions returns the partition count of topic.
	Partitions(ctx context.Context, topic string) (int, error)
	// Produce writes records synchronously.
	Produce(ctx context.Context, records []KafkaRecord) error
	// Consume delivers records from topic to handle until ctx is done.
	Consume(ctx context.Context, topic string, handle func(ctx context.Context, value []byte)) error
	Close() error
}

// KafkaFactory builds a transport once registration has returned credentials.
type KafkaFactory func(ctx context.Context, creds KafkaCredentials) (KafkaTransport, error)

// Option configures a Client.
type Option func(*Client)

// WithKafka supplies the Kafka transport used when Config.KafkaEnabled is set.
func WithKafka(factory KafkaFactory) Option {
	return func(c *Client) { c.kafkaFactory = factory }
}
