package notify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type kafkaOrder struct {
	OrderID    string `json:"orderId" notify:"orderId"`
	CustomerID string `json:"customerId" notify:"customerId"`
}

// TestClientOverRealKafkaProtocol runs the SDK against an in-process Kafka
// cluster (kfake) speaking the real wire protocol.
func TestClientOverRealKafkaProtocol(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(12, EventsTopic),
		kfake.SeedTopics(1, ScheduledEventsTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	token := enc(map[string]string{"alg": "none"}) + "." + enc(map[string]string{"tenantId": "tenant-42"}) + ".sig"
	acp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/client/register" {
			_ = json.NewEncoder(w).Encode(RegistrationResponse{Token: token, KafkaHeaderToken: token})
			return
		}
		w.WriteHeader(http.StatusOK) // vocabulary / rules
	}))
	defer acp.Close()

	client := New(Config{ACPServerURL: acp.URL, ClientToken: "client-123", KafkaEnabled: true,
		FlushInterval: 10 * time.Millisecond},
		WithKafka(KafkaTransportFactory(KafkaConfig{Brokers: brokers, GroupID: "test-group"})))
	place := Event(client, EventSpec{Key: "ORDER_PLACED", Priority: 2},
		func(_ context.Context, o kafkaOrder) (kafkaOrder, error) { return o, nil })
	SubjectSupplier(client, "ORDER_PLACED", "", func(_ context.Context, o kafkaOrder) ([]Subject, error) {
		return []Subject{NewSmsSubject(o.CustomerID, "", nil)}, nil
	})
	VocabularySupplier(client, "REMINDER", "", func(ctx context.Context, _ kafkaOrder) (any, error) {
		s, _ := ScheduleFromContext(ctx)
		return kafkaOrder{OrderID: "scheduled-" + s.ID}, nil
	})

	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := place(ctx, kafkaOrder{OrderID: "O-1", CustomerID: "CUST-1"}); err != nil {
		t.Fatal(err)
	}

	// acp-server publishes a scheduled trigger.
	pub, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.ProduceSync(ctx, &kgo.Record{Topic: ScheduledEventsTopic,
		Value: []byte(`{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}`)}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Read everything the SDK produced.
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(EventsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := map[string]*kgo.Record{}
	for len(got) < 3 && ctx.Err() == nil {
		reader.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got[string(r.Key)] = r })
	}

	want := map[string]int32{
		"tenant-42:ORDER_PLACED":        7,  // REGISTER: Java hash("tenant-42") % 12
		"tenant-42:ORDER_PLACED:CUST-1": 11, // per-subject: Java hash("CUST-1") % 12
		"tenant-42:REMINDER":            7,  // SCHEDULED
	}
	for key, partition := range want {
		r, ok := got[key]
		if !ok {
			t.Fatalf("missing record %q; got %v", key, recordKeys(got))
		}
		if r.Partition != partition {
			t.Errorf("%q on partition %d, want %d", key, r.Partition, partition)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != "Authorization" || string(r.Headers[0].Value) != "Bearer "+token {
			t.Errorf("%q headers = %v", key, r.Headers)
		}
	}
	var scheduled EventCapture
	if err := json.Unmarshal(got["tenant-42:REMINDER"].Value, &scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled.Event.EventType != "SCHEDULED" || scheduled.Payload["orderId"] != "scheduled-s-1" {
		t.Errorf("scheduled capture = %+v", scheduled)
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer closeCancel()
	if err := client.Close(closeCtx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func recordKeys(m map[string]*kgo.Record) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
