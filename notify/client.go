package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Client is the SDK runtime: the Go counterpart of the Java Bootstrapper,
// Dispatcher, AcpServerClient and TokenHolder wired together.
type Client struct {
	cfg     Config
	log     *slog.Logger
	acp     *acpClient
	tokens  tokenHolder
	buf     *buffer
	models  *modelRegistry
	hooks   *hooks
	metrics metrics

	kafkaFactory KafkaFactory
	kafka        KafkaTransport
	partitions   int

	mu         sync.Mutex
	dispatchMu sync.Mutex // one dispatch at a time, so Flush waits for in-flight sends
	started    bool
	closed     bool
	registered bool // guarded by hooks.mu: registration records enqueued; later registrations go straight to the buffer
	clientID   string

	lastRegistration *RegistrationResponse

	ready  chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a client. Register models, events, rules and suppliers, then
// call Start.
func New(cfg Config, opts ...Option) *Client {
	cfg = cfg.withDefaults()
	c := &Client{
		cfg:    cfg,
		log:    cfg.Logger,
		acp:    &acpClient{baseURL: cfg.ACPServerURL, http: cfg.HTTPClient},
		models: newModelRegistry(),
		hooks:  newHooks(),
		ready:  make(chan struct{}),
	}
	c.buf = newBuffer(cfg.MaxBufferSize, func() { c.metrics.dropped.Add(1) })
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start registers with acp-server and starts the background dispatcher. It
// does not block: registration is retried with backoff until it succeeds or
// the client is closed, and captures made meanwhile are buffered.
func (c *Client) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("notify: client already started")
	}
	if c.closed {
		return errors.New("notify: client is closed")
	}
	c.started = true
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(ctx)
	}()
	return nil
}

// WaitReady blocks until registration has completed or ctx is done.
func (c *Client) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns a snapshot of SDK counters.
func (c *Client) Metrics() Metrics { return c.metrics.snapshot() }

// Flush sends everything currently buffered. It requires a completed
// registration (see WaitReady).
func (c *Client) Flush(ctx context.Context) error {
	select {
	case <-c.ready:
	default:
		return errors.New("notify: not registered with acp-server yet")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Always dispatch at least once: it waits for any in-flight batch.
		if err := c.dispatchOnce(ctx); err != nil {
			return err
		}
		if c.buf.size() == 0 {
			return nil
		}
	}
}

// Close stops the dispatcher and consumer, flushes buffered records within
// ctx's deadline, and closes the Kafka transport.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()

	var err error
	select {
	case <-c.ready:
		if ferr := c.Flush(ctx); ferr != nil {
			err = fmt.Errorf("notify: %d record(s) not flushed: %w", c.buf.size(), ferr)
		}
	default:
		if n := c.buf.size(); n > 0 {
			err = fmt.Errorf("notify: closed before registration; %d record(s) discarded", n)
		}
	}
	if c.kafka != nil {
		if cerr := c.kafka.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (c *Client) run(ctx context.Context) {
	if !c.registerWithRetry(ctx) {
		return
	}
	if c.cfg.KafkaEnabled {
		c.initKafka(ctx)
	} else {
		c.log.Info("notify: Kafka disabled; event captures use HTTP transport")
	}
	c.enqueueRegistrations()
	close(c.ready)
	c.dispatchLoop(ctx)
}

// registerWithRetry mirrors Bootstrapper's registration step, retrying
// instead of disabling the SDK when acp-server is unreachable.
func (c *Client) registerWithRetry(ctx context.Context) bool {
	backoff := time.Second
	for {
		resp, err := c.acp.register(ctx, RegistrationRequest{
			ClientID:        c.cfg.ClientToken,
			ApplicationName: c.cfg.ApplicationName,
			BasePackage:     c.cfg.BasePackage,
		})
		if err == nil {
			c.clientID = c.cfg.ClientToken
			if resp.ClientID != "" {
				c.clientID = resp.ClientID
			}
			if resp.Token != "" {
				c.tokens.setTokens(resp.Token, resp.RefreshToken, resp.KafkaHeaderToken, resp.ExpiresInMs)
				c.log.Info("notify: registered with acp-server", "url", c.cfg.ACPServerURL)
			} else {
				c.log.Warn("notify: registration response did not contain tokens")
			}
			c.lastRegistration = resp
			return true
		}
		c.log.Warn("notify: registration failed; retrying", "error", err, "retryIn", backoff)
		if !sleep(ctx, backoff) {
			return false
		}
		backoff = min(backoff*2, time.Minute)
	}
}

func (c *Client) initKafka(ctx context.Context) {
	if c.kafkaFactory == nil {
		c.log.Warn("notify: KafkaEnabled is set but no transport was supplied (notify.WithKafka); using HTTP")
		return
	}
	resp := c.lastRegistration
	transport, err := c.kafkaFactory(ctx, KafkaCredentials{ClientID: c.cfg.ClientToken, APIKey: resp.APIKey, APISecret: resp.APISecret})
	if err != nil {
		c.log.Error("notify: Kafka transport failed to start; using HTTP", "error", err)
		return
	}
	n, err := transport.Partitions(ctx, EventsTopic)
	if err != nil || n <= 0 {
		c.log.Error("notify: could not read Kafka partitions; using HTTP", "topic", EventsTopic, "error", err)
		_ = transport.Close()
		return
	}
	c.kafka, c.partitions = transport, n
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := transport.Consume(ctx, ScheduledEventsTopic, func(ctx context.Context, value []byte) {
			var schedule EventSchedule
			if err := json.Unmarshal(value, &schedule); err != nil {
				c.log.Warn("notify: invalid scheduled event", "error", err)
				return
			}
			c.onScheduledEvent(ctx, &schedule)
		})
		if err != nil && ctx.Err() == nil {
			c.log.Error("notify: scheduled-event consumer stopped", "error", err)
		}
	}()
	c.log.Info("notify: Kafka transport ready", "partitions", n)
}

// enqueueRegistrations mirrors the Bootstrapper enqueue step: REGISTER
// captures for events, vocabulary models, then rules — ahead of any captures
// buffered while registration was in progress.
func (c *Client) enqueueRegistrations() {
	c.hooks.mu.Lock()
	defer c.hooks.mu.Unlock()
	var recs []record
	for _, e := range c.hooks.events {
		recs = append(recs, record{RecordEventCapture, registerCapture(e)})
	}
	if models := c.models.classModels(); len(models) > 0 {
		recs = append(recs, record{RecordVocabulary, models})
	}
	for _, r := range c.hooks.rules {
		recs = append(recs, ruleRecord(r))
	}
	c.buf.requeue(recs)
	c.registered = true
	c.log.Info("notify: registration records queued", "events", len(c.hooks.events), "models", len(c.models.all()), "rules", len(c.hooks.rules))
}

// Registrations take hooks.mu so they cannot interleave with
// enqueueRegistrations: each one is either included in the initial batch or
// enqueued on its own afterwards, never both.

func (c *Client) registerModel(t reflect.Type, description string) {
	c.hooks.mu.Lock()
	defer c.hooks.mu.Unlock()
	meta, added, err := c.models.register(t, description)
	if err != nil {
		panic(err)
	}
	if added && c.registered {
		c.buf.addVocabulary(classModelsOf(c.models, meta))
	}
}

func classModelsOf(r *modelRegistry, meta *modelMeta) []ClassModel {
	for _, m := range r.classModels() {
		if m.PackageName == meta.typ.PkgPath() && m.ClassName == meta.typ.Name() {
			return []ClassModel{m}
		}
	}
	return nil
}

func (c *Client) registerEvent(meta *eventMeta) {
	c.hooks.mu.Lock()
	defer c.hooks.mu.Unlock()
	c.hooks.events = append(c.hooks.events, meta)
	if c.registered {
		c.buf.addEventCapture(registerCapture(meta))
	}
}

func (c *Client) registerRule(r *ruleMeta) {
	c.hooks.mu.Lock()
	defer c.hooks.mu.Unlock()
	c.hooks.rules = append(c.hooks.rules, r)
	c.hooks.rulesByEvent[r.event] = append(c.hooks.rulesByEvent[r.event], r)
	if c.registered {
		c.buf.add(ruleRecord(r))
	}
}

func ruleRecord(r *ruleMeta) record {
	return record{RecordRule, map[string]any{"eventName": r.event, "ruleName": r.name, "ruleDescription": r.description}}
}

// registerCapture builds the REGISTER capture that tells acp-server about an
// event's configuration. The payload matches the Java SDK's flattened
// EventMetadata.
func registerCapture(e *eventMeta) *EventCapture {
	ev := EventModel{
		Name:                e.spec.Key,
		Description:         e.spec.Description,
		Priority:            e.spec.Priority,
		ScheduleIntent:      e.spec.ScheduleIntent,
		PreferredTimeWindow: e.spec.PreferredTimeWindow,
		EventType:           "REGISTER",
	}
	return &EventCapture{
		Timestamp: time.Now(),
		Event:     ev,
		Payload: map[string]any{"EventMetadata": map[string]any{
			"event":              ev,
			"version":            e.spec.Version,
			"methodName":         e.methodName,
			"declaringClassName": e.declaringClassName,
			"returnTypeName":     e.returnTypeName,
			"parameterTypeNames": e.paramTypeNames,
		}},
		ServiceName: e.serviceName,
	}
}

func (c *Client) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.buf.notify:
		}
		for c.buf.size() > 0 && ctx.Err() == nil {
			if err := c.dispatchOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.Error("notify: dispatch failed; will retry", "error", err, "retryIn", backoff)
				if !sleep(ctx, backoff) {
					return
				}
				backoff = min(backoff*2, 30*time.Second)
				break
			}
			backoff = time.Second
		}
	}
}

// dispatchOnce mirrors one Dispatcher#run iteration: vocabulary and rules go
// to their HTTP endpoints; event captures go over Kafka when available
// (unless the token has the "basic" profile) or HTTP otherwise. Anything
// unsent is requeued at the front of the buffer.
func (c *Client) dispatchOnce(ctx context.Context) error {
	c.dispatchMu.Lock()
	defer c.dispatchMu.Unlock()
	recs := c.buf.drain(c.cfg.BufferBatchSize)
	var captures []*EventCapture
	var captureRecs []record
	for i, r := range recs {
		var err error
		switch r.typ {
		case RecordVocabulary:
			models := r.payload.([]ClassModel)
			err = c.withAuth(ctx, func(token string) error { return c.acp.postVocabulary(ctx, models, token) })
		case RecordRule:
			rule := r.payload.(map[string]any)
			err = c.withAuth(ctx, func(token string) error { return c.acp.postRule(ctx, rule, token) })
		case RecordEventCapture:
			captures = append(captures, r.payload.(*EventCapture))
			captureRecs = append(captureRecs, r)
		}
		if err != nil {
			c.buf.requeue(append(captureRecs, recs[i:]...))
			return err
		}
	}
	if len(captures) == 0 {
		return nil
	}
	var err error
	if c.kafka == nil || hasBasicProfile(c.tokens.access()) {
		err = c.withAuth(ctx, func(token string) error { return c.acp.postEventCaptures(ctx, captures, token) })
	} else {
		err = c.produce(ctx, captures)
	}
	if err != nil {
		c.buf.requeue(captureRecs)
	}
	return err
}

// produce mirrors the Dispatcher's Kafka path: one record per subject keyed
// tenant:event:subjectId and partitioned by the subject id's Java hash, or a
// single tenant:event record (tenant-hash partition) when there are no
// subjects. The Authorization header carries the Kafka header token.
func (c *Client) produce(ctx context.Context, captures []*EventCapture) error {
	token := c.tokens.kafkaHeader()
	tenant := tenantIDFromToken(token)
	headers := map[string]string{}
	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			token = "Bearer " + token
		}
		headers["Authorization"] = token
	}
	var records []KafkaRecord
	for _, capture := range captures {
		if err := ValidateCredentialFree(capture.Payload); err != nil {
			c.log.Error("notify: dropping capture with credential-like payload fields", "event", capture.Event.Name)
			continue
		}
		value, err := json.Marshal(capture)
		if err != nil {
			c.log.Error("notify: dropping unserializable capture", "event", capture.Event.Name, "error", err)
			continue
		}
		name := capture.Event.Name
		if capture.SubjectResult == nil || len(capture.SubjectResult.Subjects) == 0 {
			records = append(records, KafkaRecord{Topic: EventsTopic, Partition: javaPartition(tenant, c.partitions),
				Key: tenant + ":" + name, Value: value, Headers: headers})
			continue
		}
		for _, s := range capture.SubjectResult.Subjects {
			records = append(records, KafkaRecord{Topic: EventsTopic, Partition: javaPartition(s.SubjectID(), c.partitions),
				Key: tenant + ":" + name + ":" + s.SubjectID(), Value: value, Headers: headers})
		}
	}
	if len(records) == 0 {
		return nil
	}
	return c.kafka.Produce(ctx, records)
}

// withAuth runs op with the access token, refreshing first if it has expired
// and once more on HTTP 401 (Dispatcher#postWithAuthRetry).
func (c *Client) withAuth(ctx context.Context, op func(token string) error) error {
	if c.tokens.expired() {
		c.refresh(ctx)
	}
	err := op(c.tokens.access())
	if isUnauthorized(err) {
		c.refresh(ctx)
		err = op(c.tokens.access())
	}
	return err
}

func (c *Client) refresh(ctx context.Context) {
	ref := c.tokens.refreshToken()
	if ref == "" {
		return
	}
	resp, err := c.acp.refreshToken(ctx, TokenRefreshRequest{ClientID: c.clientID, RefreshToken: ref})
	if err != nil {
		c.log.Error("notify: token refresh failed", "error", err)
		return
	}
	c.tokens.setToken(resp.Token, resp.ExpiresInMs)
	c.log.Info("notify: access token refreshed")
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
