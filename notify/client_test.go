package notify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func fakeJWT(claims map[string]any) string {
	enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
	return enc(map[string]string{"alg": "HS256"}) + "." + enc(claims) + ".sig"
}

// fakeACP is an in-memory acp-server.
type fakeACP struct {
	mu            sync.Mutex
	srv           *httptest.Server
	token         string
	failRegisters int
	reject401Once bool
	refreshed     int
	registrations []RegistrationRequest
	vocabulary    [][]ClassModel
	rules         []map[string]any
	captures      []map[string]any // raw JSON, as the server sees it
	authHeaders   []string
}

func newFakeACP(t *testing.T) *fakeACP {
	f := &fakeACP{token: fakeJWT(map[string]any{"tenantId": "tenant-42"})}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeACP) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	switch r.URL.Path {
	case "/client/register":
		if f.failRegisters > 0 {
			f.failRegisters--
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		var req RegistrationRequest
		_ = json.Unmarshal(body, &req)
		f.registrations = append(f.registrations, req)
		_ = json.NewEncoder(w).Encode(RegistrationResponse{Token: f.token, RefreshToken: "refresh-1", ExpiresInMs: 60_000})
		return
	case "/auth/token/refresh":
		f.refreshed++
		f.token = fakeJWT(map[string]any{"tenantId": "tenant-42", "n": f.refreshed})
		_ = json.NewEncoder(w).Encode(TokenRefreshResponse{Token: f.token, ExpiresInMs: 60_000})
		return
	}
	if f.reject401Once && r.URL.Path == "/api/event" {
		f.reject401Once = false
		http.Error(w, "expired", http.StatusUnauthorized)
		return
	}
	if auth != f.token {
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	f.authHeaders = append(f.authHeaders, auth)
	switch r.URL.Path {
	case "/api/vocabulary":
		var m []ClassModel
		_ = json.Unmarshal(body, &m)
		f.vocabulary = append(f.vocabulary, m)
	case "/api/vocabulary/rules/process":
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		f.rules = append(f.rules, m)
	case "/api/event":
		var list []map[string]any
		_ = json.Unmarshal(body, &list)
		f.captures = append(f.captures, list...)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeACP) capturesOfType(eventType string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, c := range f.captures {
		if c["event"].(map[string]any)["eventType"] == eventType {
			out = append(out, c)
		}
	}
	return out
}

type OrderPayload struct {
	OrderID    string  `json:"orderId" notify:"orderId" notifyDesc:"Unique order identifier"`
	CustomerID string  `json:"customerId" notify:"customerId" notifyDesc:"Customer who placed the order"`
	Amount     float64 `json:"amount" notify:"amount" notifyDesc:"Total order amount in USD"`
}

func (OrderPayload) NotifyModelDescription() string { return "Payload for order placement events" }

type orderService struct{ placed []string }

func (s *orderService) PlaceOrder(_ context.Context, p OrderPayload) (OrderPayload, error) {
	if p.Amount < 0 {
		return p, errors.New("negative amount")
	}
	if p.Amount == 13 {
		panic("unlucky")
	}
	s.placed = append(s.placed, p.OrderID)
	return p, nil
}

func newTestClient(f *fakeACP, opts ...Option) *Client {
	return New(Config{
		ACPServerURL:    f.srv.URL,
		ApplicationName: "test-app",
		ClientToken:     "client-123",
		BasePackage:     "example.com/shop",
		FlushInterval:   10 * time.Millisecond,
	}, opts...)
}

func ready(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

func flush(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEndToEndHTTP(t *testing.T) {
	f := newFakeACP(t)
	c := newTestClient(f)
	svc := &orderService{}

	var calls []string
	placeOrder := Event(c, EventSpec{Key: "ORDER_PLACED", Description: "Customer placed an order", EventType: "static",
		ScheduleIntent: "immediate", PreferredTimeWindow: "09:00-18:00", Priority: 2}, svc.PlaceOrder)
	SubjectSupplier(c, "ORDER_PLACED", "order customer", func(_ context.Context, p OrderPayload) ([]Subject, error) {
		email := ""
		if p.CustomerID != "" {
			email = p.CustomerID + "@example.com"
		}
		return []Subject{NewEmailSubject(email, "", "", "", map[string]string{"firstName": "Alice"})}, nil
	})
	Rule(c, RuleSpec{Name: "fraud-check", Event: "ORDER_PLACED", Description: "Blocks large orders"},
		func(_ context.Context, p OrderPayload) (bool, error) { return p.Amount < 1000, nil })
	Rule(c, RuleSpec{Name: "always"}, func(context.Context, OrderPayload) (bool, error) { return true, nil })
	Callback(c, "ORDER_PLACED", Before, func(_ context.Context, p OrderPayload) error {
		calls = append(calls, "before:"+p.OrderID)
		return nil
	})
	Callback(c, "ORDER_PLACED", After, func(_ context.Context, p OrderPayload) error {
		calls = append(calls, "after:"+p.OrderID)
		return errors.New("ignored")
	})

	// Captured before Start: must be sent after the registration records.
	if _, err := placeOrder(context.Background(), OrderPayload{OrderID: "O-0", CustomerID: "alice", Amount: 5}); err != nil {
		t.Fatal(err)
	}

	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	ready(t, c)

	out, err := placeOrder(context.Background(), OrderPayload{OrderID: "O-1", CustomerID: "alice", Amount: 120})
	if err != nil || out.OrderID != "O-1" {
		t.Fatalf("wrapped call = %+v, %v", out, err)
	}
	if _, err := placeOrder(context.Background(), OrderPayload{OrderID: "O-2", Amount: -1}); err == nil {
		t.Fatal("error not propagated")
	}
	func() {
		defer func() {
			if recover() != "unlucky" {
				t.Error("panic not re-raised")
			}
		}()
		_, _ = placeOrder(context.Background(), OrderPayload{OrderID: "O-3", Amount: 13})
	}()
	flush(t, c)

	if strings.Join(calls, ",") != "before:O-0,after:O-0,before:O-1,after:O-1,before:O-2,after:O-2,before:O-3,after:O-3" {
		t.Errorf("callbacks: %v", calls)
	}

	f.mu.Lock()
	reg := f.registrations[0]
	vocab := f.vocabulary
	rules := f.rules
	first := f.captures[0]
	f.mu.Unlock()

	if reg.ClientID != "client-123" || reg.ApplicationName != "test-app" || reg.BasePackage != "example.com/shop" {
		t.Errorf("registration = %+v", reg)
	}
	if first["event"].(map[string]any)["eventType"] != "REGISTER" {
		t.Errorf("first capture should be REGISTER, got %v", first["event"])
	}
	if len(vocab) != 1 || vocab[0][0].ClassName != "OrderPayload" || vocab[0][0].ClassDescription != "Payload for order placement events" {
		t.Errorf("vocabulary = %+v", vocab)
	}
	if len(rules) != 2 || rules[0]["ruleName"] != "fraud-check" || rules[1]["eventName"] != "*" {
		t.Errorf("rules = %v", rules)
	}

	register := f.capturesOfType("REGISTER")[0]
	ev := register["event"].(map[string]any)
	if ev["name"] != "ORDER_PLACED" || ev["priority"] != float64(2) || ev["scheduleIntent"] != "immediate" ||
		ev["preferredTimeWindow"] != "09:00-18:00" || register["serviceName"] != "orderService" {
		t.Errorf("REGISTER capture = %v", register)
	}
	md := register["payload"].(map[string]any)["EventMetadata"].(map[string]any)
	if md["methodName"] != "PlaceOrder" || md["version"] != "v1" ||
		!strings.HasSuffix(md["declaringClassName"].(string), ".orderService") ||
		!strings.HasSuffix(md["returnTypeName"].(string), ".OrderPayload") {
		t.Errorf("EventMetadata = %v", md)
	}

	users := f.capturesOfType("USER")
	if len(users) != 4 {
		t.Fatalf("got %d USER captures", len(users))
	}
	ok := users[1]
	if p := ok["payload"].(map[string]any); p["orderId"] != "O-1" || p["amount"] != float64(120) {
		t.Errorf("payload = %v", p)
	}
	if ok["timestamp"] == nil || ok["occuredAt"] != ok["timestamp"] || ok["serviceName"] != "orderService" {
		t.Errorf("capture metadata = %v", ok)
	}
	if res := ok["result"].(map[string]any); res["success"] != true || !strings.Contains(res["returnValue"].(string), `"orderId":"O-1"`) {
		t.Errorf("result = %v", res)
	}
	sr := ok["subjectResult"].(map[string]any)
	subj := sr["subjects"].([]any)[0].(map[string]any)
	if sr["success"] != true || subj["channel"] != "EMAIL" || subj["email"] != "alice@example.com" {
		t.Errorf("subjectResult = %v", sr)
	}
	rr := ok["ruleResults"].([]any)
	if len(rr) != 2 || rr[0].(map[string]any)["result"] != true || rr[1].(map[string]any)["ruleName"] != "always" {
		t.Errorf("ruleResults = %v", rr)
	}
	frames := ok["callStack"].(map[string]any)["frames"].([]any)
	if len(frames) == 0 || !strings.Contains(frames[0].(map[string]any)["methodName"].(string), "TestEndToEndHTTP") {
		t.Errorf("call stack should start in the caller: %v", frames)
	}

	failed := users[2]
	if failed["result"].(map[string]any)["success"] != false || failed["exception"].(map[string]any)["message"] != "negative amount" {
		t.Errorf("error capture = %v / %v", failed["result"], failed["exception"])
	}
	// Subject supplier returned a blank address for O-2 (no customer).
	if sr := failed["subjectResult"].(map[string]any); sr["success"] != false || !strings.Contains(sr["errorMessage"].(string), "without a destination address") {
		t.Errorf("subject validation = %v", sr)
	}
	if exc := users[3]["exception"].(map[string]any); exc["message"] != "unlucky" || !strings.HasPrefix(exc["exceptionType"].(string), "panic") {
		t.Errorf("panic capture = %v", exc)
	}

	if m := c.Metrics(); m.EventCaptureCount != 4 || m.RuleInvokeCount != 8 || m.SubjectSupplierInvokeCount != 4 {
		t.Errorf("metrics = %+v", m)
	}
}

func TestLateRegistrationIsSentImmediately(t *testing.T) {
	f := newFakeACP(t)
	c := newTestClient(f)
	_ = c.Start()
	defer c.Close(context.Background())
	ready(t, c)

	emit := Emitter[OrderPayload](c, EventSpec{Key: "LATE", Priority: 1})
	Rule(c, RuleSpec{Name: "late-rule", Event: "LATE"}, func(context.Context, OrderPayload) (bool, error) { return true, nil })
	_ = emit(context.Background(), OrderPayload{OrderID: "L"})
	flush(t, c)

	if len(f.capturesOfType("REGISTER")) != 1 || len(f.capturesOfType("USER")) != 1 {
		t.Errorf("captures = %v", f.captures)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.vocabulary) != 1 || len(f.rules) != 1 {
		t.Errorf("vocab=%d rules=%d", len(f.vocabulary), len(f.rules))
	}
}

func TestTokenRefreshOn401(t *testing.T) {
	f := newFakeACP(t)
	c := newTestClient(f)
	emit := Emitter[OrderPayload](c, EventSpec{Key: "E", Priority: 1})
	_ = c.Start()
	defer c.Close(context.Background())
	ready(t, c)
	flush(t, c)

	f.mu.Lock()
	f.reject401Once = true
	f.mu.Unlock()
	_ = emit(context.Background(), OrderPayload{OrderID: "X"})
	flush(t, c)

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refreshed != 1 {
		t.Errorf("refreshed %d times", f.refreshed)
	}
	if last := f.captures[len(f.captures)-1]; last["payload"].(map[string]any)["orderId"] != "X" {
		t.Errorf("capture not delivered after refresh")
	}
}

func TestRegistrationRetriesAndFailedSendIsRequeued(t *testing.T) {
	f := newFakeACP(t)
	f.failRegisters = 1
	c := newTestClient(f)
	emit := Emitter[OrderPayload](c, EventSpec{Key: "E", Priority: 1})
	_ = c.Start()
	defer c.Close(context.Background())
	_ = emit(context.Background(), OrderPayload{OrderID: "queued"})
	ready(t, c) // after one retry (~1s)

	deadline := time.Now().Add(5 * time.Second)
	for len(f.capturesOfType("USER")) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(f.capturesOfType("USER")) != 1 {
		t.Fatal("buffered capture was not delivered after registration")
	}
}

func TestInvalidSpecsPanic(t *testing.T) {
	c := New(Config{})
	mustPanic := func(name string, fn func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s: expected panic", name)
			}
		}()
		fn()
	}
	mustPanic("blank key", func() { Emitter[OrderPayload](c, EventSpec{Priority: 1}) })
	mustPanic("priority", func() { Emitter[OrderPayload](c, EventSpec{Key: "K", Priority: 6}) })
	mustPanic("rule name", func() {
		Rule(c, RuleSpec{}, func(context.Context, OrderPayload) (bool, error) { return true, nil })
	})
}

// fakeKafka records produced records and can deliver scheduled events.
type fakeKafka struct {
	mu       sync.Mutex
	records  []KafkaRecord
	creds    KafkaCredentials
	schedule chan []byte
}

func (k *fakeKafka) Partitions(context.Context, string) (int, error) { return 12, nil }
func (k *fakeKafka) Produce(_ context.Context, rs []KafkaRecord) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.records = append(k.records, rs...)
	return nil
}
func (k *fakeKafka) Consume(ctx context.Context, _ string, handle func(context.Context, []byte)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case v := <-k.schedule:
			handle(ctx, v)
		}
	}
}
func (k *fakeKafka) Close() error { return nil }

func TestKafkaTransportPartitioningAndScheduledEvents(t *testing.T) {
	f := newFakeACP(t)
	kafka := &fakeKafka{schedule: make(chan []byte, 1)}
	c := New(Config{ACPServerURL: f.srv.URL, ClientToken: "client-123", FlushInterval: 10 * time.Millisecond, KafkaEnabled: true},
		WithKafka(func(_ context.Context, creds KafkaCredentials) (KafkaTransport, error) {
			kafka.creds = creds
			return kafka, nil
		}))

	emit := Emitter[OrderPayload](c, EventSpec{Key: "ORDER_PLACED", Priority: 2})
	SubjectSupplier(c, "ORDER_PLACED", "", func(_ context.Context, p OrderPayload) ([]Subject, error) {
		return []Subject{NewSmsSubject("CUST-1", "", nil), NewEmailSubject("alice@example.com", "", "", "", nil)}, nil
	})
	type leaky struct {
		Password string `json:"password"`
	}
	leak := Emitter[leaky](c, EventSpec{Key: "LEAK", Priority: 1})
	VocabularySupplier(c, "REMINDER", "", func(ctx context.Context, _ OrderPayload) (any, error) {
		s, _ := ScheduleFromContext(ctx)
		return OrderPayload{OrderID: "from-" + s.ID}, nil
	})

	_ = c.Start()
	defer c.Close(context.Background())
	ready(t, c)
	if kafka.creds.ClientID != "client-123" {
		t.Errorf("creds = %+v", kafka.creds)
	}
	_ = emit(context.Background(), OrderPayload{OrderID: "K-1"})
	_ = leak(context.Background(), leaky{Password: "hunter2"})
	kafka.schedule <- []byte(`{"id":"s-1","eventName":"REMINDER","triggerType":"DELAY"}`)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		kafka.mu.Lock()
		n := len(kafka.records)
		kafka.mu.Unlock()
		if n >= 5 { // 2 REGISTER + 2 subject records + 1 SCHEDULED
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	flush(t, c)

	kafka.mu.Lock()
	defer kafka.mu.Unlock()
	byKey := map[string]KafkaRecord{}
	for _, r := range kafka.records {
		byKey[r.Key] = r
		if r.Topic != EventsTopic || r.Headers["Authorization"] != "Bearer "+f.token {
			t.Errorf("record %q topic/header = %s / %q", r.Key, r.Topic, r.Headers["Authorization"])
		}
		if strings.Contains(string(r.Value), "hunter2") {
			t.Error("credential payload was published")
		}
	}
	// Partitions must equal Java's Math.abs(hashCode) % 12 (see TestJavaStringHashCodeMatchesJVM).
	want := map[string]int32{
		"tenant-42:ORDER_PLACED:CUST-1":            11,
		"tenant-42:ORDER_PLACED:alice@example.com": 1,
		"tenant-42:ORDER_PLACED":                   7, // REGISTER, tenant-keyed
		"tenant-42:REMINDER":                       7, // SCHEDULED, no subjects
	}
	for key, partition := range want {
		r, ok := byKey[key]
		if !ok {
			t.Errorf("missing record %q (have %d records)", key, len(kafka.records))
			continue
		}
		if r.Partition != partition {
			t.Errorf("%q partition = %d, want %d", key, r.Partition, partition)
		}
	}
	var scheduled EventCapture
	_ = json.Unmarshal(byKey["tenant-42:REMINDER"].Value, &scheduled)
	if scheduled.Event.EventType != "SCHEDULED" || scheduled.Payload["orderId"] != "from-s-1" {
		t.Errorf("scheduled capture = %+v", scheduled)
	}
	if len(f.capturesOfType("USER")) != 0 {
		t.Error("captures should not use HTTP when Kafka is active")
	}
}

func TestBasicProfileTokenForcesHTTP(t *testing.T) {
	f := newFakeACP(t)
	f.token = fakeJWT(map[string]any{"tenantId": "t", "profile": "basic"})
	kafka := &fakeKafka{schedule: make(chan []byte)}
	c := New(Config{ACPServerURL: f.srv.URL, FlushInterval: 10 * time.Millisecond, KafkaEnabled: true},
		WithKafka(func(context.Context, KafkaCredentials) (KafkaTransport, error) { return kafka, nil }))
	emit := Emitter[OrderPayload](c, EventSpec{Key: "E", Priority: 1})
	_ = c.Start()
	defer c.Close(context.Background())
	ready(t, c)
	_ = emit(context.Background(), OrderPayload{OrderID: "B"})
	flush(t, c)
	if len(kafka.records) != 0 || len(f.capturesOfType("USER")) != 1 {
		t.Errorf("kafka=%d http=%d", len(kafka.records), len(f.capturesOfType("USER")))
	}
}

func TestBufferDropsOldestCaptureWhenFull(t *testing.T) {
	b := newBuffer(2, nil)
	b.addRule("E", "r", "")
	b.addEventCapture(&EventCapture{ID: "1"})
	b.addEventCapture(&EventCapture{ID: "2"})
	recs := b.drain(0)
	if len(recs) != 2 || recs[0].typ != RecordRule || recs[1].payload.(*EventCapture).ID != "2" {
		t.Errorf("buffer = %+v", recs)
	}
}
