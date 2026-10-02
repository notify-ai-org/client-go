package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/notify-ai-org/client-go/notify"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type order struct {
	OrderID    string `json:"orderId" notify:"orderId"`
	CustomerID string `json:"customerId" notify:"customerId"`
}

// TestClientOverRealKafkaProtocol runs the SDK against an in-process Kafka
// cluster (kfake) speaking the real wire protocol.
func TestClientOverRealKafkaProtocol(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(12, notify.EventsTopic),
		kfake.SeedTopics(1, notify.ScheduledEventsTopic),
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
			_ = json.NewEncoder(w).Encode(notify.RegistrationResponse{Token: token, KafkaHeaderToken: token})
			return
		}
		w.WriteHeader(http.StatusOK) // vocabulary / rules
	}))
	defer acp.Close()

	client := notify.New(notify.Config{ACPServerURL: acp.URL, ClientToken: "client-123", KafkaEnabled: true,
		FlushInterval: 10 * time.Millisecond},
		notify.WithKafka(Factory(Config{Brokers: brokers, GroupID: "test-group"})))
	place := notify.Event(client, notify.EventSpec{Key: "ORDER_PLACED", Priority: 2},
		func(_ context.Context, o order) (order, error) { return o, nil })
	notify.SubjectSupplier(client, "ORDER_PLACED", "", func(_ context.Context, o order) ([]notify.Subject, error) {
		return []notify.Subject{notify.NewSmsSubject(o.CustomerID, "", nil)}, nil
	})
	notify.VocabularySupplier(client, "REMINDER", "", func(ctx context.Context, _ order) (any, error) {
		s, _ := notify.ScheduleFromContext(ctx)
		return order{OrderID: "scheduled-" + s.ID}, nil
	})

	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := place(ctx, order{OrderID: "O-1", CustomerID: "CUST-1"}); err != nil {
		t.Fatal(err)
	}

	// acp-server publishes a scheduled trigger.
	pub, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.ProduceSync(ctx, &kgo.Record{Topic: notify.ScheduledEventsTopic,
		Value: []byte(`{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}`)}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Read everything the SDK produced.
	reader, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(notify.EventsTopic),
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
			t.Fatalf("missing record %q; got %v", key, keys(got))
		}
		if r.Partition != partition {
			t.Errorf("%q on partition %d, want %d", key, r.Partition, partition)
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != "Authorization" || string(r.Headers[0].Value) != "Bearer "+token {
			t.Errorf("%q headers = %v", key, r.Headers)
		}
	}
	var scheduled notify.EventCapture
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

func keys(m map[string]*kgo.Record) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
