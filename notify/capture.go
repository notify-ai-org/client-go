package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// sdkDir is this package's source directory; frames from it (other than
// tests) are SDK internals and are left out of captured call stacks.
var sdkDir = func() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(file) + string(filepath.Separator)
}()

// runEvent mirrors EventListener#aroundEvent: BEFORE callbacks, the event
// function, AFTER callbacks, subject supplier and rules, then buffers the
// capture. Panics in the event function are captured and re-raised.
func (c *Client) runEvent(ctx context.Context, meta *eventMeta, payload any, invoke func() (any, error)) (any, error) {
	key := meta.spec.Key
	start := time.Now()
	frames := callerFrames(5)

	c.runCallbacks(ctx, Before, key, payload)

	var (
		result     any
		err        error
		panicked   bool
		panicValue any
		panicStack []byte
	)
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				panicked, panicValue, panicStack = true, rv, debug.Stack()
			}
		}()
		result, err = invoke()
	}()

	c.runCallbacks(ctx, After, key, payload)

	capture := &EventCapture{
		Timestamp:      time.Now(),
		Event:          EventModel{Name: key, EventType: "USER", Description: meta.spec.Description},
		Payload:        c.models.flatten(payload),
		DurationMillis: time.Since(start).Milliseconds(),
		CallStack:      &CallStack{Frames: frames},
		ServiceName:    meta.serviceName,
		SubjectResult:  c.executeSubjectSupplier(ctx, key, payload),
		RuleResults:    c.executeRules(ctx, key, payload),
	}
	switch {
	case panicked:
		capture.Exception = &ExceptionInfo{
			ExceptionType: fmt.Sprintf("panic(%T)", panicValue),
			Message:       fmt.Sprint(panicValue),
			StackTrace:    string(panicStack),
		}
		capture.Result = &ExecutionResult{Success: false}
	case err != nil:
		capture.Exception = &ExceptionInfo{
			ExceptionType: fmt.Sprintf("%T", err),
			Message:       err.Error(),
			StackTrace:    err.Error() + "\n\n" + string(debug.Stack()),
		}
		capture.Result = &ExecutionResult{Success: false}
	default:
		capture.Result = &ExecutionResult{Success: true, ReturnValue: jsonString(result)}
	}

	c.buf.addEventCapture(capture)
	c.metrics.captureCount.Add(1)

	if panicked {
		panic(panicValue)
	}
	return result, err
}

func jsonString(v any) *string {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		if rv.IsNil() {
			return nil
		}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		s := fmt.Sprint(v)
		return &s
	}
	s := string(raw)
	return &s
}

func (c *Client) runCallbacks(ctx context.Context, when When, key string, payload any) {
	for _, cb := range c.hooks.callbacksFor(when, key) {
		func() {
			defer func() {
				if rv := recover(); rv != nil {
					c.log.Warn("notify: callback panicked", "event", key, "panic", rv)
				}
			}()
			if _, err := cb.fn(ctx, payload); err != nil {
				c.log.Warn("notify: callback failed", "event", key, "error", err)
			}
		}()
	}
}

// executeSubjectSupplier mirrors EventListener#executeSubjectSupplier.
func (c *Client) executeSubjectSupplier(ctx context.Context, key string, payload any) *SubjectResult {
	res := &SubjectResult{EventKey: key, Subjects: []Subject{}, Success: true}
	s := c.hooks.subjectSupplier(key)
	if s == nil {
		return res
	}
	start := time.Now()
	out, err := safeCall(ctx, s.fn, payload)
	res.ExecutionTimeMillis = time.Since(start).Milliseconds()
	c.metrics.subject(time.Since(start))
	var subjects []Subject
	if err == nil {
		subjects, _ = out.([]Subject)
		err = validateSubjects(key, subjects)
	}
	if err != nil {
		res.Success, res.ErrorMessage = false, strPtr(err.Error())
		return res
	}
	res.Subjects = subjects
	return res
}

func validateSubjects(key string, subjects []Subject) error {
	for i, s := range subjects {
		if s == nil || reflect.ValueOf(s).IsNil() {
			return fmt.Errorf("SubjectSupplier for event '%s' returned nil subject at index %d", key, i)
		}
		if strings.TrimSpace(s.Address()) == "" {
			return fmt.Errorf("SubjectSupplier for event '%s' returned %s subject at index %d without a destination address",
				key, s.Channel(), i)
		}
	}
	return nil
}

// executeRules mirrors EventListener#executeRules (event-specific then "*").
func (c *Client) executeRules(ctx context.Context, key string, payload any) []RuleResult {
	rules := c.hooks.rulesFor(key)
	results := make([]RuleResult, 0, len(rules))
	for _, r := range rules {
		start := time.Now()
		out, err := safeCall(ctx, r.fn, payload)
		c.metrics.rule(time.Since(start))
		rr := RuleResult{RuleName: r.name, EventKey: key, ExecutionTimeMillis: time.Since(start).Milliseconds(), Success: err == nil}
		if err != nil {
			rr.ErrorMessage = strPtr(err.Error())
		} else {
			rr.Result = out
		}
		results = append(results, rr)
	}
	return results
}

func safeCall(ctx context.Context, fn erasedFn, payload any) (out any, err error) {
	defer func() {
		if rv := recover(); rv != nil {
			err = fmt.Errorf("panic: %v", rv)
		}
	}()
	return fn(ctx, payload)
}

// onScheduledEvent mirrors EventListener#onScheduledEvent: a trigger from
// acp-server becomes a SCHEDULED capture whose payload comes from the
// event's vocabulary supplier, if any.
func (c *Client) onScheduledEvent(ctx context.Context, schedule *EventSchedule) {
	capture := &EventCapture{
		Timestamp: time.Now(),
		Event:     EventModel{Name: schedule.EventName, EventType: "SCHEDULED"},
	}
	if v := c.hooks.vocabularySupplier(schedule.EventName); v != nil {
		start := time.Now()
		out, err := safeCall(context.WithValue(ctx, scheduleKey{}, schedule), v.fn, nil)
		c.metrics.vocab(time.Since(start))
		if err != nil {
			c.log.Warn("notify: vocabulary supplier failed", "event", schedule.EventName, "error", err)
		} else if out != nil {
			capture.Payload = c.models.flatten(out)
		}
	}
	c.buf.addEventCapture(capture)
	c.metrics.captureCount.Add(1)
}

// callerFrames returns up to n frames from the application, skipping the SDK.
func callerFrames(n int) []StackFrame {
	pcs := make([]uintptr, 32)
	pcs = pcs[:runtime.Callers(2, pcs)]
	iter := runtime.CallersFrames(pcs)
	var out []StackFrame
	for len(out) < n {
		f, more := iter.Next()
		internal := strings.HasPrefix(f.File, sdkDir) && !strings.HasSuffix(f.File, "_test.go")
		if !internal && !strings.HasPrefix(f.Function, "runtime.") {
			fi := parseFuncName(f.Function)
			out = append(out, StackFrame{ClassName: fi.className(), MethodName: fi.method, LineNumber: f.Line, FileName: f.File})
		}
		if !more {
			break
		}
	}
	return out
}
