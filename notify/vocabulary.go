package notify

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// Struct tags that replace the Java @Vocabulary annotation:
//
//	type OrderPayload struct {
//	    OrderID string  `json:"orderId" notify:"orderId" notifyDesc:"Unique order identifier"`
//	    Amount  float64 `json:"amount"  notify:"amount"  notifyDesc:"Total order amount in USD"`
//	}
//
// As with @Model, when at least one field carries a `notify` tag only tagged
// fields are vocabulary; otherwise every exported field is, named after its
// json tag (or Go field name). `notify:"-"` excludes a field.
const (
	TagName        = "notify"
	TagDescription = "notifyDesc"
)

// ModelDescriber lets a payload type supply its @Model description when it
// is registered implicitly by notify.Event.
type ModelDescriber interface {
	NotifyModelDescription() string
}

type vocabField struct {
	name, description string
	index             []int
	typ               reflect.Type
}

type modelMeta struct {
	typ         reflect.Type
	description string
	fields      []vocabField
}

type modelRegistry struct {
	mu     sync.RWMutex
	byType map[reflect.Type]*modelMeta
	order  []*modelMeta
}

func newModelRegistry() *modelRegistry {
	return &modelRegistry{byType: map[reflect.Type]*modelMeta{}}
}

func structType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	return t
}

// register adds t as a model. It returns (meta, true) when newly added.
func (r *modelRegistry) register(t reflect.Type, description string) (*modelMeta, bool, error) {
	st := structType(t)
	if st == nil {
		return nil, false, fmt.Errorf("notify: model %v must be a struct type", t)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.byType[st]; ok {
		if description != "" && m.description == "" {
			m.description = description
		}
		return m, false, nil
	}
	fields, err := vocabFields(st)
	if err != nil {
		return nil, false, err
	}
	m := &modelMeta{typ: st, description: description, fields: fields}
	r.byType[st] = m
	r.order = append(r.order, m)
	return m, true, nil
}

func (r *modelRegistry) get(t reflect.Type) *modelMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byType[t]
}

func (r *modelRegistry) all() []*modelMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*modelMeta(nil), r.order...)
}

func vocabFields(st reflect.Type) ([]vocabField, error) {
	tagged := false
	for i := 0; i < st.NumField(); i++ {
		if tag, ok := st.Field(i).Tag.Lookup(TagName); ok && tag != "-" {
			tagged = true
			break
		}
	}
	var out []vocabField
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		tag, hasTag := f.Tag.Lookup(TagName)
		if tag == "-" || (tagged && !hasTag) {
			continue
		}
		if !f.IsExported() {
			if tagged {
				return nil, fmt.Errorf("notify: field %s.%s has a %q tag but is unexported", st.Name(), f.Name, TagName)
			}
			continue
		}
		name := tag
		if name == "" {
			name = jsonName(f)
		}
		out = append(out, vocabField{name: name, description: f.Tag.Get(TagDescription), index: f.Index, typ: f.Type})
	}
	return out, nil
}

func jsonName(f reflect.StructField) string {
	if tag := f.Tag.Get("json"); tag != "" {
		if name := strings.Split(tag, ",")[0]; name != "" && name != "-" {
			return name
		}
	}
	return f.Name
}

// vocabNode mirrors the Java Vocabulary instance graph.
type vocabNode struct {
	term     string
	value    any
	children []*vocabNode
}

// flatten mirrors VocabularyManager#toFlattenedMap: a model instance becomes
// {vocabName: value}; nested models are expanded into their own leaves; a
// non-model value becomes {TypeName: value}.
func (r *modelRegistry) flatten(instance any) map[string]any {
	out := map[string]any{}
	if instance == nil {
		return out
	}
	v := reflect.ValueOf(instance)
	root := &vocabNode{term: simpleTypeName(v.Type()), value: instance}
	r.build(v, root, map[uintptr]bool{})
	flattenNode(root, out)
	return out
}

func (r *modelRegistry) build(v reflect.Value, parent *vocabNode, visited map[uintptr]bool) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() || visited[v.Pointer()] {
			return
		}
		visited[v.Pointer()] = true
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return
	}
	meta := r.get(v.Type())
	if meta == nil {
		return
	}
	for _, f := range meta.fields {
		fv := v.FieldByIndex(f.index)
		child := &vocabNode{term: f.name, value: nilIfNilPointer(fv)}
		parent.children = append(parent.children, child)
		r.build(fv, child, visited)
	}
}

func nilIfNilPointer(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return nil
		}
	}
	return v.Interface()
}

func flattenNode(n *vocabNode, out map[string]any) {
	if len(n.children) == 0 {
		out[n.term] = n.value
		return
	}
	for _, c := range n.children {
		flattenNode(c, out)
	}
}

// classModels mirrors VocabularyManager#toClassModelDtoList.
func (r *modelRegistry) classModels() []ClassModel {
	var out []ClassModel
	for _, m := range r.all() {
		attrs := make([]AttributeModel, 0, len(m.fields))
		for _, f := range m.fields {
			attrs = append(attrs, AttributeModel{Name: f.name, Type: javaTypeName(f.typ), Description: f.description})
		}
		out = append(out, ClassModel{
			PackageName:      m.typ.PkgPath(),
			ClassName:        m.typ.Name(),
			ClassDescription: m.description,
			Interfaces:       []string{},
			Attributes:       attrs,
			Methods:          []MethodModel{},
		})
	}
	return out
}

var timeType = reflect.TypeOf(time.Time{})

// javaTypeName maps Go types to the Java simple names acp-server sees from the
// Java SDK, so vocabulary looks the same whichever SDK registered it.
func javaTypeName(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == timeType {
		return "Instant"
	}
	switch t.Kind() {
	case reflect.String:
		return "String"
	case reflect.Bool:
		return "boolean"
	case reflect.Int8:
		return "byte"
	case reflect.Int16:
		return "short"
	case reflect.Int, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return "int"
	case reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
		return "long"
	case reflect.Float32:
		return "float"
	case reflect.Float64:
		return "double"
	case reflect.Slice, reflect.Array:
		return "List"
	case reflect.Map:
		return "Map"
	case reflect.Interface:
		return "Object"
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

func simpleTypeName(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

// qualifiedTypeName mirrors Class#getName for event metadata.
func qualifiedTypeName(t reflect.Type) string {
	if t == nil {
		return "void"
	}
	base := t
	for base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if base.PkgPath() != "" && base.Name() != "" {
		return base.PkgPath() + "." + base.Name()
	}
	return t.String()
}
