package notify

import (
	"context"
	"testing"
)

var _ KafkaTransport = (*FranzKafkaTransport)(nil)

func TestRegistrationCredentialsEnableSASLSSL(t *testing.T) {
	tr, err := NewKafkaTransport(KafkaConfig{Brokers: []string{"127.0.0.1:1"}}, KafkaCredentials{ClientID: "c1", APIKey: "k", APISecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if tr.cfg.SecurityProtocol != "SASL_SSL" || tr.cfg.SASLUsername != "k" || tr.cfg.SASLPassword != "s" {
		t.Errorf("cfg = %+v", tr.cfg)
	}
	if got := kafkaClientID(tr.cfg, KafkaCredentials{ClientID: "c1"}, "producer"); got != "c1-producer" {
		t.Errorf("client id = %q", got)
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := NewKafkaTransport(KafkaConfig{SecurityProtocol: "BOGUS"}, KafkaCredentials{}); err == nil {
		t.Error("expected protocol error")
	}
	if _, err := NewKafkaTransport(KafkaConfig{SecurityProtocol: "SASL_PLAINTEXT", SASLMechanism: "GSSAPI"}, KafkaCredentials{}); err == nil {
		t.Error("expected mechanism error")
	}
	if _, err := NewKafkaTransport(KafkaConfig{Compression: "brotli"}, KafkaCredentials{}); err == nil {
		t.Error("expected compression error")
	}
}

func TestFactoryImplementsInterface(t *testing.T) {
	tr, err := KafkaTransportFactory(KafkaConfig{Brokers: []string{"127.0.0.1:1"}})(context.Background(), KafkaCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	_ = tr.Close()
}
