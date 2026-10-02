package kafka

import (
	"context"
	"testing"

	"github.com/notify-ai-org/client-go/notify"
)

var _ notify.KafkaTransport = (*Transport)(nil)

func TestRegistrationCredentialsEnableSASLSSL(t *testing.T) {
	tr, err := New(Config{Brokers: []string{"127.0.0.1:1"}}, notify.KafkaCredentials{ClientID: "c1", APIKey: "k", APISecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if tr.cfg.SecurityProtocol != "SASL_SSL" || tr.cfg.SASLUsername != "k" || tr.cfg.SASLPassword != "s" {
		t.Errorf("cfg = %+v", tr.cfg)
	}
	if got := clientID(tr.cfg, notify.KafkaCredentials{ClientID: "c1"}, "producer"); got != "c1-producer" {
		t.Errorf("client id = %q", got)
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := New(Config{SecurityProtocol: "BOGUS"}, notify.KafkaCredentials{}); err == nil {
		t.Error("expected protocol error")
	}
	if _, err := New(Config{SecurityProtocol: "SASL_PLAINTEXT", SASLMechanism: "GSSAPI"}, notify.KafkaCredentials{}); err == nil {
		t.Error("expected mechanism error")
	}
	if _, err := New(Config{Compression: "brotli"}, notify.KafkaCredentials{}); err == nil {
		t.Error("expected compression error")
	}
}

func TestFactoryImplementsInterface(t *testing.T) {
	tr, err := Factory(Config{Brokers: []string{"127.0.0.1:1"}})(context.Background(), notify.KafkaCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	_ = tr.Close()
}
