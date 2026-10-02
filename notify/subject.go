package notify

import (
	"encoding/json"
	"strings"
)

// Channel mirrors com.notify.agent.client.enums.Channel.
type Channel string

const (
	ChannelSMS      Channel = "SMS"
	ChannelEmail    Channel = "EMAIL"
	ChannelWhatsApp Channel = "WHATSAPP"
	ChannelPush     Channel = "PUSH"
	ChannelNone     Channel = "NONE"
	ChannelWebhook  Channel = "WEBHOOK"
	ChannelInApp    Channel = "IN_APP"
)

// Subject is a notification recipient returned by a subject supplier. The
// concrete types serialize to the same JSON as the Java SDK's Subject
// hierarchy (discriminated by "channel").
type Subject interface {
	Channel() Channel
	// SubjectID identifies the recipient; it is the destination address.
	SubjectID() string
	CorrelationID() string
	Attributes() map[string]string
	// Address is the channel-specific destination (email, phone, URL, ...).
	Address() string
	AddressFingerprint() string
}

type subjectBase struct {
	channel       Channel
	correlationID string
	attributes    map[string]string
}

func newBase(ch Channel, correlationID string, attrs map[string]string) subjectBase {
	if correlationID == "" {
		correlationID = newUUID()
	}
	return subjectBase{channel: ch, correlationID: correlationID, attributes: attrs}
}

func (b subjectBase) Channel() Channel              { return b.channel }
func (b subjectBase) CorrelationID() string         { return b.correlationID }
func (b subjectBase) Attributes() map[string]string { return b.attributes }

func (b subjectBase) fields() map[string]any {
	return map[string]any{
		"channel":       b.channel,
		"correlationId": b.correlationID,
		"attributes":    b.attributes,
	}
}

func optional(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// EmailSubject delivers to an email address.
type EmailSubject struct {
	subjectBase
	email, cc, bcc string
}

// NewEmailSubject mirrors the Java EmailSubject constructor. An empty
// correlationID is replaced by a random UUID.
func NewEmailSubject(email, cc, bcc, correlationID string, attributes map[string]string) *EmailSubject {
	return &EmailSubject{subjectBase: newBase(ChannelEmail, correlationID, attributes), email: email, cc: cc, bcc: bcc}
}

func (s *EmailSubject) Email() string              { return s.email }
func (s *EmailSubject) CC() string                 { return s.cc }
func (s *EmailSubject) BCC() string                { return s.bcc }
func (s *EmailSubject) Address() string            { return s.email }
func (s *EmailSubject) SubjectID() string          { return s.email }
func (s *EmailSubject) AddressFingerprint() string { return "email:" + strings.ToLower(s.email) }
func (s *EmailSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["email"], m["cc"], m["bcc"] = s.email, optional(s.cc), optional(s.bcc)
	return json.Marshal(m)
}

// SmsSubject delivers to a phone number over SMS.
type SmsSubject struct {
	subjectBase
	phoneNumber string
}

func NewSmsSubject(phoneNumber, correlationID string, attributes map[string]string) *SmsSubject {
	return &SmsSubject{subjectBase: newBase(ChannelSMS, correlationID, attributes), phoneNumber: phoneNumber}
}

func (s *SmsSubject) PhoneNumber() string        { return s.phoneNumber }
func (s *SmsSubject) Address() string            { return s.phoneNumber }
func (s *SmsSubject) SubjectID() string          { return s.phoneNumber }
func (s *SmsSubject) AddressFingerprint() string { return "sms:" + s.phoneNumber }
func (s *SmsSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["phoneNumber"] = s.phoneNumber
	return json.Marshal(m)
}

// WhatsAppSubject delivers to a phone number over WhatsApp.
type WhatsAppSubject struct {
	subjectBase
	phoneNumber string
}

func NewWhatsAppSubject(phoneNumber, correlationID string, attributes map[string]string) *WhatsAppSubject {
	phoneNumber = strings.TrimSpace(phoneNumber)
	return &WhatsAppSubject{subjectBase: newBase(ChannelWhatsApp, correlationID, attributes), phoneNumber: phoneNumber}
}

func (s *WhatsAppSubject) PhoneNumber() string        { return s.phoneNumber }
func (s *WhatsAppSubject) Address() string            { return s.phoneNumber }
func (s *WhatsAppSubject) SubjectID() string          { return s.phoneNumber }
func (s *WhatsAppSubject) AddressFingerprint() string { return "whatsapp:" + s.phoneNumber }
func (s *WhatsAppSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["phoneNumber"] = s.phoneNumber
	return json.Marshal(m)
}

// PushSubject delivers to a device token.
type PushSubject struct {
	subjectBase
	deviceToken string
}

func NewPushSubject(deviceToken, correlationID string, attributes map[string]string) *PushSubject {
	return &PushSubject{subjectBase: newBase(ChannelPush, correlationID, attributes), deviceToken: deviceToken}
}

func (s *PushSubject) DeviceToken() string        { return s.deviceToken }
func (s *PushSubject) Address() string            { return s.deviceToken }
func (s *PushSubject) SubjectID() string          { return s.deviceToken }
func (s *PushSubject) AddressFingerprint() string { return "push:" + s.deviceToken }
func (s *PushSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["deviceToken"] = s.deviceToken
	return json.Marshal(m)
}

// WebhookSubject delivers to an HTTP endpoint.
type WebhookSubject struct {
	subjectBase
	url string
}

func NewWebhookSubject(url, correlationID string, attributes map[string]string) *WebhookSubject {
	return &WebhookSubject{subjectBase: newBase(ChannelWebhook, correlationID, attributes), url: url}
}

func (s *WebhookSubject) URL() string                { return s.url }
func (s *WebhookSubject) Address() string            { return s.url }
func (s *WebhookSubject) SubjectID() string          { return s.url }
func (s *WebhookSubject) AddressFingerprint() string { return "webhook:" + s.url }
func (s *WebhookSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["url"] = s.url
	return json.Marshal(m)
}

// InAppSubject delivers an in-app notification.
type InAppSubject struct {
	subjectBase
	url, userID string
}

func NewInAppSubject(url, userID, correlationID string, attributes map[string]string) *InAppSubject {
	url = strings.TrimSpace(url)
	return &InAppSubject{subjectBase: newBase(ChannelInApp, correlationID, attributes), url: url, userID: userID}
}

func (s *InAppSubject) URL() string       { return s.url }
func (s *InAppSubject) UserID() string    { return s.userID }
func (s *InAppSubject) Address() string   { return s.url }
func (s *InAppSubject) SubjectID() string { return s.url }
func (s *InAppSubject) AddressFingerprint() string {
	return "in_app:" + s.url + ":" + s.userID
}
func (s *InAppSubject) MarshalJSON() ([]byte, error) {
	m := s.fields()
	m["url"], m["userId"] = s.url, optional(s.userID)
	return json.Marshal(m)
}
