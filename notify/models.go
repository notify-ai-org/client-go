package notify

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
)

// Wire models. JSON field names match the Java SDK / acp-server DTOs exactly.

// EventModel mirrors com.notify.agent.client.models.Event.
type EventModel struct {
	ID                  string `json:"id,omitempty"`
	Name                string `json:"name"`
	Description         string `json:"description,omitempty"`
	Priority            int    `json:"priority"`
	Status              string `json:"status,omitempty"`
	ScheduleIntent      string `json:"scheduleIntent,omitempty"`
	PreferredTimeWindow string `json:"preferredTimeWindow,omitempty"`
	EventType           string `json:"eventType,omitempty"`
}

// EventCapture mirrors com.notify.agent.client.models.EventCapture.
type EventCapture struct {
	ID                  string           `json:"id,omitempty"`
	TenantID            string           `json:"tenantId,omitempty"`
	Timestamp           time.Time        `json:"-"`
	CorrelationID       string           `json:"correlationId,omitempty"`
	Payload             map[string]any   `json:"payload"`
	Event               EventModel       `json:"event"`
	CallStack           *CallStack       `json:"callStack,omitempty"`
	Result              *ExecutionResult `json:"result,omitempty"`
	Exception           *ExceptionInfo   `json:"exception,omitempty"`
	DurationMillis      int64            `json:"durationMillis"`
	ServiceName         string           `json:"serviceName,omitempty"`
	AgentThoughtProcess string           `json:"agentThoughtProcess,omitempty"`
	BulletReasons       string           `json:"bulletReasons,omitempty"`
	Status              string           `json:"status,omitempty"`
	SubjectResult       *SubjectResult   `json:"subjectResult,omitempty"`
	RuleResults         []RuleResult     `json:"ruleResults,omitempty"`
}

// MarshalJSON emits the timestamp under both "timestamp" and "occuredAt", as
// Jackson does for the Java DTO (field plus getOccuredAt()).
func (e EventCapture) MarshalJSON() ([]byte, error) {
	type plain EventCapture
	var ts *time.Time
	if !e.Timestamp.IsZero() {
		t := e.Timestamp.UTC()
		ts = &t
	}
	return json.Marshal(struct {
		plain
		Timestamp *time.Time `json:"timestamp"`
		OccuredAt *time.Time `json:"occuredAt"`
	}{plain(e), ts, ts})
}

// UnmarshalJSON accepts either "timestamp" or "occuredAt".
func (e *EventCapture) UnmarshalJSON(data []byte) error {
	type plain EventCapture
	aux := struct {
		*plain
		Timestamp *Instant `json:"timestamp"`
		OccuredAt *Instant `json:"occuredAt"`
	}{plain: (*plain)(e)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	switch {
	case aux.Timestamp != nil:
		e.Timestamp = aux.Timestamp.Time
	case aux.OccuredAt != nil:
		e.Timestamp = aux.OccuredAt.Time
	}
	return nil
}

// CallStack mirrors com.notify.agent.client.models.CallStack.
type CallStack struct {
	Frames []StackFrame `json:"frames"`
}

// StackFrame mirrors com.notify.agent.client.models.StackFrame.
type StackFrame struct {
	ClassName  string `json:"className"`
	MethodName string `json:"methodName"`
	LineNumber int    `json:"lineNumber"`
	FileName   string `json:"fileName"`
}

// ExecutionResult mirrors com.notify.agent.client.models.ExecutionResult.
type ExecutionResult struct {
	Success     bool    `json:"success"`
	ReturnValue *string `json:"returnValue"`
}

// ExceptionInfo mirrors com.notify.agent.client.models.ExceptionInfo.
type ExceptionInfo struct {
	ExceptionType string `json:"exceptionType"`
	Message       string `json:"message"`
	StackTrace    string `json:"stackTrace"`
}

// SubjectResult mirrors com.notify.agent.client.models.dto.SubjectResultDto.
type SubjectResult struct {
	EventKey            string    `json:"eventKey"`
	Subjects            []Subject `json:"subjects"`
	ExecutionTimeMillis int64     `json:"executionTimeMillis"`
	Success             bool      `json:"success"`
	ErrorMessage        *string   `json:"errorMessage"`
}

// RuleResult mirrors com.notify.agent.client.models.dto.RuleResultDto.
type RuleResult struct {
	RuleName            string  `json:"ruleName"`
	EventKey            string  `json:"eventKey"`
	Result              any     `json:"result"`
	ExecutionTimeMillis int64   `json:"executionTimeMillis"`
	Success             bool    `json:"success"`
	ErrorMessage        *string `json:"errorMessage"`
}

// ClassModel mirrors com.notify.agent.client.models.ClassModel (vocabulary ingestion).
type ClassModel struct {
	PackageName      string           `json:"packageName"`
	ClassName        string           `json:"className"`
	ClassDescription string           `json:"classDescription"`
	ClassType        *string          `json:"classType"`
	SuperClass       *string          `json:"superClass"`
	Interfaces       []string         `json:"interfaces"`
	Attributes       []AttributeModel `json:"attributes"`
	Methods          []MethodModel    `json:"methods"`
}

// AttributeModel mirrors com.notify.agent.client.models.AttributeModel.
type AttributeModel struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Description  string  `json:"description"`
	DefaultValue *string `json:"defaultValue"`
}

// MethodModel mirrors com.notify.agent.client.models.MethodModel.
type MethodModel struct {
	Name        string           `json:"name"`
	ReturnType  string           `json:"returnType"`
	Description string           `json:"description"`
	Parameters  []ParameterModel `json:"parameters"`
}

// ParameterModel mirrors com.notify.agent.client.models.ParameterModel.
type ParameterModel struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// EventSchedule mirrors com.notify.agent.client.models.EventSchedule — a
// scheduled trigger delivered by acp-server over Kafka.
type EventSchedule struct {
	ID             string   `json:"id"`
	EventName      string   `json:"eventName"`
	Description    string   `json:"description"`
	TriggerType    string   `json:"triggerType"`
	ScheduledAt    *Instant `json:"scheduledAt"`
	CronExpression string   `json:"cronExpression"`
}

// RegistrationRequest mirrors ClientRegistrationDto.Request.
type RegistrationRequest struct {
	ClientID        string `json:"clientId"`
	ApplicationName string `json:"applicationName"`
	BasePackage     string `json:"basePackage"`
	RawToken        string `json:"rawToken,omitempty"`
}

// RegistrationResponse mirrors ClientRegistrationDto.Response.
type RegistrationResponse struct {
	ClientID         string `json:"clientId"`
	Token            string `json:"token"`
	APIKey           string `json:"apiKey"`
	APISecret        string `json:"apiSecret"`
	RefreshToken     string `json:"refreshToken"`
	KafkaHeaderToken string `json:"kafkaHeaderToken"`
	ExpiresInMs      int64  `json:"expiresInMs"`
}

// TokenRefreshRequest mirrors TokenRefreshDto.Request.
type TokenRefreshRequest struct {
	ClientID     string `json:"clientId"`
	RefreshToken string `json:"refreshToken"`
}

// TokenRefreshResponse mirrors TokenRefreshDto.Response.
type TokenRefreshResponse struct {
	Token       string `json:"token"`
	ExpiresInMs int64  `json:"expiresInMs"`
}

// Instant is a time.Time that decodes the forms Jackson emits for
// java.time.Instant: ISO-8601 strings and epoch seconds (with optional
// fractional nanoseconds). It encodes as RFC 3339.
type Instant struct{ time.Time }

func (i Instant) MarshalJSON() ([]byte, error) { return json.Marshal(i.Time.UTC()) }

func (i *Instant) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		return json.Unmarshal(data, &i.Time)
	}
	f, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return err
	}
	sec := int64(f)
	i.Time = time.Unix(sec, int64((f-float64(sec))*1e9)).UTC()
	return nil
}
