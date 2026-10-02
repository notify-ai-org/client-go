package notify

import (
	"sync/atomic"
	"time"
)

// Metrics mirrors MetricsManager counters.
type Metrics struct {
	RuleInvokeCount               int64
	VocabularySupplierInvokeCount int64
	SubjectSupplierInvokeCount    int64
	EventCaptureCount             int64
	TotalRuleInvokeMs             int64
	TotalVocabularySupplierMs     int64
	TotalSubjectSupplierMs        int64
	// Go-only: captures dropped because the buffer was full.
	DroppedCaptureCount int64
}

type metrics struct {
	ruleCount, vocabCount, subjectCount, captureCount, dropped atomic.Int64
	ruleMs, vocabMs, subjectMs                                 atomic.Int64
}

func (m *metrics) rule(d time.Duration)    { m.ruleCount.Add(1); m.ruleMs.Add(d.Milliseconds()) }
func (m *metrics) vocab(d time.Duration)   { m.vocabCount.Add(1); m.vocabMs.Add(d.Milliseconds()) }
func (m *metrics) subject(d time.Duration) { m.subjectCount.Add(1); m.subjectMs.Add(d.Milliseconds()) }

func (m *metrics) snapshot() Metrics {
	return Metrics{
		RuleInvokeCount:               m.ruleCount.Load(),
		VocabularySupplierInvokeCount: m.vocabCount.Load(),
		SubjectSupplierInvokeCount:    m.subjectCount.Load(),
		EventCaptureCount:             m.captureCount.Load(),
		TotalRuleInvokeMs:             m.ruleMs.Load(),
		TotalVocabularySupplierMs:     m.vocabMs.Load(),
		TotalSubjectSupplierMs:        m.subjectMs.Load(),
		DroppedCaptureCount:           m.dropped.Load(),
	}
}
