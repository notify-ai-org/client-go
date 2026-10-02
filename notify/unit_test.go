package notify

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestJavaStringHashCodeMatchesJVM(t *testing.T) {
	// Reference values produced by java.lang.String#hashCode on a real JVM.
	cases := []struct {
		s         string
		hash      int32
		partition int32 // Math.abs(hash) % 12
	}{
		{"", 0, 0},
		{"hello", 99162322, 10},
		{"CUST-1", 1999207223, 11},
		{"alice@example.com", 2145772861, 1},
		{"tenant-42", -1852830079, 7},
		{"héllo wörld", 1628148953, 5},
		{"📦 order", 1563790519, 7},
		// Integer.MIN_VALUE: Java yields -8 (an invalid partition); Go uses 8.
		{"polygenelubricants", -2147483648, 8},
	}
	for _, tc := range cases {
		if got := javaStringHashCode(tc.s); got != tc.hash {
			t.Errorf("hash(%q) = %d, want %d", tc.s, got, tc.hash)
		}
		if got := javaPartition(tc.s, 12); got != tc.partition {
			t.Errorf("partition(%q) = %d, want %d", tc.s, got, tc.partition)
		}
	}
}

type address struct {
	City string `notify:"city" notifyDesc:"Delivery city"`
	Zip  string `notify:"zip"`
}

type order struct {
	OrderID  string   `json:"orderId" notify:"orderId" notifyDesc:"Unique order identifier"`
	Amount   float64  `notify:"amount"`
	Items    []string `notify:"items"`
	Ship     *address `notify:"shipping"`
	Internal string   // untagged: excluded because other fields are tagged
}

type untagged struct {
	CustomerID string `json:"customerId"`
	Count      int
	hidden     string
}

func TestFlattenMirrorsJavaVocabularyManager(t *testing.T) {
	r := newModelRegistry()
	for _, m := range []any{order{}, address{}, untagged{}} {
		if _, _, err := r.register(reflect.TypeOf(m), ""); err != nil {
			t.Fatal(err)
		}
	}

	got := r.flatten(order{OrderID: "O-1", Amount: 12.5, Items: []string{"a"}, Ship: &address{City: "Pune", Zip: "411001"}, Internal: "x"})
	want := map[string]any{"orderId": "O-1", "amount": 12.5, "items": []string{"a"}, "city": "Pune", "zip": "411001"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("model flatten = %#v, want %#v", got, want)
	}

	got = r.flatten(&order{OrderID: "O-2"})
	if got["shipping"] != nil || got["orderId"] != "O-2" {
		t.Errorf("nil nested model should be a nil leaf: %#v", got)
	}

	got = r.flatten(untagged{CustomerID: "C", Count: 2, hidden: "h"})
	if !reflect.DeepEqual(got, map[string]any{"customerId": "C", "Count": 2}) {
		t.Errorf("untagged flatten = %#v", got)
	}

	type notAModel struct{ X int }
	got = r.flatten(notAModel{X: 1})
	if _, ok := got["notAModel"]; !ok || len(got) != 1 {
		t.Errorf("non-model should flatten to {TypeName: value}: %#v", got)
	}
}

func TestUnexportedTaggedFieldIsRejected(t *testing.T) {
	type bad struct {
		secret string `notify:"secret"`
	}
	if _, _, err := newModelRegistry().register(reflect.TypeOf(bad{}), ""); err == nil {
		t.Fatal("expected error for unexported tagged field")
	}
}

func TestClassModels(t *testing.T) {
	r := newModelRegistry()
	_, _, _ = r.register(reflect.TypeOf(order{}), "Order payload")
	models := r.classModels()
	if len(models) != 1 {
		t.Fatalf("got %d models", len(models))
	}
	m := models[0]
	if m.ClassName != "order" || m.ClassDescription != "Order payload" || m.PackageName == "" {
		t.Errorf("unexpected class model %+v", m)
	}
	types := map[string]string{}
	for _, a := range m.Attributes {
		types[a.Name] = a.Type
	}
	want := map[string]string{"orderId": "String", "amount": "double", "items": "List", "shipping": "address"}
	if !reflect.DeepEqual(types, want) {
		t.Errorf("attribute types = %v, want %v", types, want)
	}
}

func TestCredentialPolicy(t *testing.T) {
	blocked := []any{
		map[string]any{"password": "x"},
		map[string]any{"user": map[string]any{"api_key": "x"}},
		map[string]any{"stripeSecret": "x"},
		map[string]any{"list": []any{map[string]any{"Authorization": "Bearer y"}}},
		map[string]any{"note": "-----BEGIN PRIVATE KEY-----abc"},
		map[string]any{"json": `{"refresh_token":"x"}`},
	}
	for _, b := range blocked {
		if ValidateCredentialFree(b) == nil {
			t.Errorf("expected %v to be rejected", b)
		}
	}
	allowed := map[string]any{"orderId": "1", "message": "your password was changed", "tokens": 3, "json": "{not json"}
	if err := ValidateCredentialFree(allowed); err != nil {
		t.Errorf("unexpected rejection: %v", err)
	}
}

func TestSubjectJSONMatchesJavaShape(t *testing.T) {
	attrs := map[string]string{"firstName": "Alice"}
	cases := []struct {
		subject Subject
		want    map[string]any
	}{
		{NewEmailSubject("a@x.com", "", "b@x.com", "c1", attrs),
			map[string]any{"channel": "EMAIL", "correlationId": "c1", "attributes": map[string]any{"firstName": "Alice"}, "email": "a@x.com", "cc": nil, "bcc": "b@x.com"}},
		{NewSmsSubject("+1555", "c2", nil),
			map[string]any{"channel": "SMS", "correlationId": "c2", "attributes": nil, "phoneNumber": "+1555"}},
		{NewInAppSubject(" /inbox ", "u1", "c3", nil),
			map[string]any{"channel": "IN_APP", "correlationId": "c3", "attributes": nil, "url": "/inbox", "userId": "u1"}},
	}
	for _, tc := range cases {
		raw, err := json.Marshal(tc.subject)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		_ = json.Unmarshal(raw, &got)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s JSON = %v, want %v", tc.subject.Channel(), got, tc.want)
		}
	}
	if NewSmsSubject("1", "", nil).CorrelationID() == "" {
		t.Error("empty correlation id should default to a UUID")
	}
	if got := NewEmailSubject("A@X.com", "", "", "", nil).AddressFingerprint(); got != "email:a@x.com" {
		t.Errorf("fingerprint = %q", got)
	}
}

func TestEventCaptureEmitsTimestampAndOccuredAt(t *testing.T) {
	ts := time.Date(2026, 10, 1, 12, 0, 0, 123000000, time.UTC)
	raw, _ := json.Marshal(EventCapture{Timestamp: ts, Event: EventModel{Name: "E"}})
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m["timestamp"] != "2026-10-01T12:00:00.123Z" || m["occuredAt"] != m["timestamp"] {
		t.Errorf("timestamps: %v / %v", m["timestamp"], m["occuredAt"])
	}
	var back EventCapture
	if err := json.Unmarshal(raw, &back); err != nil || !back.Timestamp.Equal(ts) {
		t.Errorf("round trip: %v %v", back.Timestamp, err)
	}
}

func TestInstantDecodesJacksonForms(t *testing.T) {
	var s EventSchedule
	if err := json.Unmarshal([]byte(`{"eventName":"E","scheduledAt":1767225600.5}`), &s); err != nil {
		t.Fatal(err)
	}
	if s.ScheduledAt.Unix() != 1767225600 || s.ScheduledAt.Nanosecond() != 500000000 {
		t.Errorf("numeric instant = %v", s.ScheduledAt.Time)
	}
	if err := json.Unmarshal([]byte(`{"scheduledAt":"2026-01-01T00:00:00Z"}`), &s); err != nil || s.ScheduledAt.Year() != 2026 {
		t.Errorf("string instant = %v %v", s.ScheduledAt, err)
	}
}

func TestParseFuncName(t *testing.T) {
	cases := map[string]funcInfo{
		"example.com/shop/orders.(*Service).Place-fm": {"example.com/shop/orders", "Service", "Place"},
		"example.com/shop/orders.Service.Place-fm":    {"example.com/shop/orders", "Service", "Place"},
		"example.com/shop/orders.place":               {"example.com/shop/orders", "", "place"},
		"main.main.func1":                             {"main", "", "main.func1"},
	}
	for in, want := range cases {
		if got := parseFuncName(in); got != want {
			t.Errorf("parseFuncName(%q) = %+v, want %+v", in, got, want)
		}
	}
}

func TestJWTClaimHelpers(t *testing.T) {
	tok := fakeJWT(map[string]any{"tenantId": "t-9", "profile": "basic"})
	if tenantIDFromToken(tok) != "t-9" || !hasBasicProfile("Bearer "+tok) {
		t.Error("claims not read")
	}
	if tenantIDFromToken("garbage") != "unknown-tenant" || hasBasicProfile("") {
		t.Error("bad token handling")
	}
}
