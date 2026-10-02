package notify

import "sync"

// RecordType mirrors Buffer.RecordType.
type RecordType int

const (
	RecordVocabulary   RecordType = iota // payload: []ClassModel
	RecordRule                           // payload: map[string]any {eventName, ruleName, ruleDescription}
	RecordEventCapture                   // payload: *EventCapture
)

type record struct {
	typ     RecordType
	payload any
}

// buffer is the in-memory queue between event capture and the dispatcher.
// Unlike the Java SDK's unbounded queue it is capped; when full, the oldest
// event capture is dropped (registration records are never dropped).
type buffer struct {
	mu      sync.Mutex
	records []record
	max     int
	notify  chan struct{}
	dropped func()
}

func newBuffer(max int, onDrop func()) *buffer {
	return &buffer{max: max, notify: make(chan struct{}, 1), dropped: onDrop}
}

func (b *buffer) add(r record) {
	b.mu.Lock()
	if b.max > 0 && len(b.records) >= b.max {
		for i, old := range b.records {
			if old.typ == RecordEventCapture {
				b.records = append(b.records[:i], b.records[i+1:]...)
				if b.dropped != nil {
					b.dropped()
				}
				break
			}
		}
	}
	b.records = append(b.records, r)
	b.mu.Unlock()
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *buffer) addVocabulary(models []ClassModel) {
	if len(models) > 0 {
		b.add(record{RecordVocabulary, models})
	}
}

func (b *buffer) addRule(eventName, ruleName, description string) {
	if ruleName == "" && eventName == "" {
		return
	}
	b.add(record{RecordRule, map[string]any{
		"eventName": eventName, "ruleName": ruleName, "ruleDescription": description,
	}})
}

func (b *buffer) addEventCapture(c *EventCapture) {
	if c != nil {
		b.add(record{RecordEventCapture, c})
	}
}

// drain removes up to max records from the front.
func (b *buffer) drain(max int) []record {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(b.records)
	if max > 0 && n > max {
		n = max
	}
	out := append([]record(nil), b.records[:n]...)
	b.records = b.records[n:]
	return out
}

// requeue puts unsent records back at the front, preserving order.
func (b *buffer) requeue(rs []record) {
	if len(rs) == 0 {
		return
	}
	b.mu.Lock()
	b.records = append(append([]record(nil), rs...), b.records...)
	b.mu.Unlock()
}

func (b *buffer) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}
