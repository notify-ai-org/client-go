package notify

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

// Registration functions are the Go equivalents of the Java annotations.
// Invalid registrations are programmer errors and panic (the Java SDK fails
// application startup in the same situations). Register before or after
// Start; registrations made after Start are sent to acp-server immediately.

// EventSpec mirrors the @Event annotation attributes.
type EventSpec struct {
	Key                 string // required, e.g. "ORDER_PLACED"
	Description         string
	PreferredTimeWindow string // e.g. "09:00-18:00"
	EventType           string // e.g. "static", "deferred"
	ScheduleIntent      string // e.g. "immediate", "scheduled", "deferred"
	Priority            int    // 1..5
	Version             string // default "v1"
	// ServiceName overrides the service name reported with captures. By
	// default it is the receiver type of fn (or its package name).
	ServiceName string
}

// RuleSpec mirrors the @Rule annotation. An empty Event applies the rule to
// every event ("*").
type RuleSpec struct {
	Name        string
	Event       string
	Description string
}

// When mirrors Callback.When.
type When int

const (
	Before When = iota
	After
)

type erasedFn func(ctx context.Context, payload any) (any, error)

// erase adapts a typed hook to the registry. A nil payload (scheduled events)
// becomes the zero value of P.
func erase[P, R any](fn func(context.Context, P) (R, error)) erasedFn {
	return func(ctx context.Context, payload any) (any, error) {
		var p P
		if payload != nil {
			typed, ok := payload.(P)
			if !ok {
				return nil, fmt.Errorf("notify: hook expects %T payload, got %T", p, payload)
			}
			p = typed
		}
		return fn(ctx, p)
	}
}

type eventMeta struct {
	spec               EventSpec
	methodName         string
	declaringClassName string
	returnTypeName     string
	paramTypeNames     []string
	serviceName        string
}

type ruleMeta struct {
	name, description, event string
	fn                       erasedFn
}

type supplierMeta struct {
	event, description string
	fn                 erasedFn
}

type callbackMeta struct {
	event string
	when  When
	fn    erasedFn
}

type hooks struct {
	mu             sync.RWMutex
	events         []*eventMeta
	rules          []*ruleMeta
	rulesByEvent   map[string][]*ruleMeta
	subjectByEvent map[string]*supplierMeta
	vocabByEvent   map[string]*supplierMeta
	callbacks      map[When]map[string][]*callbackMeta
}

func newHooks() *hooks {
	return &hooks{
		rulesByEvent:   map[string][]*ruleMeta{},
		subjectByEvent: map[string]*supplierMeta{},
		vocabByEvent:   map[string]*supplierMeta{},
		callbacks:      map[When]map[string][]*callbackMeta{Before: {}, After: {}},
	}
}

func (h *hooks) rulesFor(event string) []*ruleMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append(append([]*ruleMeta(nil), h.rulesByEvent[event]...), h.rulesByEvent["*"]...)
}

func (h *hooks) subjectSupplier(event string) *supplierMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.subjectByEvent[event]
}

func (h *hooks) vocabularySupplier(event string) *supplierMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.vocabByEvent[event]
}

func (h *hooks) callbacksFor(when When, event string) []*callbackMeta {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append([]*callbackMeta(nil), h.callbacks[when][event]...)
}

// RegisterModel mirrors @Model: registers T's vocabulary fields (see TagName)
// so payloads of type T are flattened into named attributes and the model is
// sent to acp-server's vocabulary.
func RegisterModel[T any](c *Client, description string) {
	c.registerModel(reflect.TypeOf((*T)(nil)).Elem(), description)
}

// Event mirrors @Event: it registers the event and returns a function with the
// same signature as fn that captures every call. The first argument after ctx
// is the event payload; if it is a struct it is registered as a model.
func Event[P, R any](c *Client, spec EventSpec, fn func(context.Context, P) (R, error)) func(context.Context, P) (R, error) {
	if strings.TrimSpace(spec.Key) == "" {
		panic("notify: EventSpec.Key must not be blank")
	}
	if spec.Priority < 1 || spec.Priority > 5 {
		panic(fmt.Sprintf("notify: event %q has priority=%d; must be between 1 and 5", spec.Key, spec.Priority))
	}
	if spec.Version == "" {
		spec.Version = "v1"
	}
	pType := reflect.TypeOf((*P)(nil)).Elem()
	rType := reflect.TypeOf((*R)(nil)).Elem()
	if structType(pType) != nil {
		var description string
		if d, ok := any(reflect.New(structType(pType)).Interface()).(ModelDescriber); ok {
			description = d.NotifyModelDescription()
		}
		c.registerModel(pType, description)
	}

	fi := parseFuncName(runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
	meta := &eventMeta{
		spec:               spec,
		methodName:         fi.method,
		declaringClassName: fi.className(),
		returnTypeName:     qualifiedTypeName(rType),
		paramTypeNames:     []string{"context.Context", qualifiedTypeName(pType)},
		serviceName:        spec.ServiceName,
	}
	if meta.serviceName == "" {
		meta.serviceName = fi.serviceName()
	}
	c.registerEvent(meta)

	return func(ctx context.Context, payload P) (R, error) {
		var out R
		_, err := c.runEvent(ctx, meta, payload, func() (any, error) {
			r, err := fn(ctx, payload)
			out = r
			return r, err
		})
		return out, err
	}
}

// Emitter registers an event with no business logic of its own and returns a
// function that captures it — for "fire this event now" call sites.
func Emitter[P any](c *Client, spec EventSpec) func(context.Context, P) error {
	wrapped := Event(c, spec, func(_ context.Context, p P) (P, error) { return p, nil })
	return func(ctx context.Context, p P) error {
		_, err := wrapped(ctx, p)
		return err
	}
}

// Rule mirrors @Rule. The result (typically bool) is reported to acp-server
// with each capture of the event.
func Rule[P, R any](c *Client, spec RuleSpec, fn func(context.Context, P) (R, error)) {
	if strings.TrimSpace(spec.Name) == "" {
		panic("notify: RuleSpec.Name must not be blank")
	}
	event := spec.Event
	if event == "" {
		event = "*"
	}
	c.registerRule(&ruleMeta{name: spec.Name, description: spec.Description, event: event, fn: erase(fn)})
}

// SubjectSupplier mirrors @SubjectSupplier: resolves the recipients for an
// event. One supplier per event; a later registration replaces an earlier one.
func SubjectSupplier[P any](c *Client, event, description string, fn func(context.Context, P) ([]Subject, error)) {
	if strings.TrimSpace(event) == "" {
		panic("notify: SubjectSupplier event must not be blank")
	}
	c.hooks.mu.Lock()
	c.hooks.subjectByEvent[event] = &supplierMeta{event: event, description: description, fn: erase(fn)}
	c.hooks.mu.Unlock()
}

// VocabularySupplier mirrors @VocabularySupplier: supplies the payload for
// scheduled triggers of an event delivered by acp-server. It receives the
// zero value of P; use ScheduleFromContext to read the trigger.
func VocabularySupplier[P any](c *Client, event, description string, fn func(context.Context, P) (any, error)) {
	if strings.TrimSpace(event) == "" {
		panic("notify: VocabularySupplier event must not be blank")
	}
	c.hooks.mu.Lock()
	c.hooks.vocabByEvent[event] = &supplierMeta{event: event, description: description, fn: erase(fn)}
	c.hooks.mu.Unlock()
}

// Callback mirrors @Callback: runs before or after the event function.
// Errors and panics are logged and do not affect the event.
func Callback[P any](c *Client, event string, when When, fn func(context.Context, P) error) {
	if strings.TrimSpace(event) == "" {
		panic("notify: Callback event must not be blank")
	}
	wrapped := erase(func(ctx context.Context, p P) (struct{}, error) { return struct{}{}, fn(ctx, p) })
	c.hooks.mu.Lock()
	c.hooks.callbacks[when][event] = append(c.hooks.callbacks[when][event], &callbackMeta{event: event, when: when, fn: wrapped})
	c.hooks.mu.Unlock()
}

type scheduleKey struct{}

// ScheduleFromContext returns the scheduled trigger inside a
// VocabularySupplier invoked for a scheduled event.
func ScheduleFromContext(ctx context.Context) (*EventSchedule, bool) {
	s, ok := ctx.Value(scheduleKey{}).(*EventSchedule)
	return s, ok
}

// funcInfo is a Go function name split into Java-like class/method parts.
type funcInfo struct {
	pkgPath, receiver, method string
}

// parseFuncName splits runtime names such as
// "example.com/app/orders.(*Service).Place-fm" or "example.com/app/orders.place".
func parseFuncName(full string) funcInfo {
	slash := strings.LastIndex(full, "/")
	dot := strings.Index(full[slash+1:], ".")
	if dot < 0 {
		return funcInfo{method: full}
	}
	pkg, rest := full[:slash+1+dot], full[slash+1+dot+1:]
	isMethodValue := strings.HasSuffix(rest, "-fm")
	rest = strings.TrimSuffix(rest, "-fm")
	if i := strings.Index(rest, "."); i > 0 && (strings.HasPrefix(rest, "(") || isMethodValue) {
		recv := strings.Trim(rest[:i], "(*)")
		return funcInfo{pkgPath: pkg, receiver: recv, method: rest[i+1:]}
	}
	return funcInfo{pkgPath: pkg, method: rest}
}

func (f funcInfo) className() string {
	if f.receiver != "" {
		return f.pkgPath + "." + f.receiver
	}
	return f.pkgPath
}

func (f funcInfo) serviceName() string {
	if f.receiver != "" {
		return f.receiver
	}
	return f.pkgPath[strings.LastIndex(f.pkgPath, "/")+1:]
}
