package notify

import (
	"math"
	"sync"
	"time"
)

// tokenHolder mirrors TokenHolder: in-memory access / refresh / Kafka header
// tokens with expiry.
type tokenHolder struct {
	mu                   sync.RWMutex
	token, refresh, head string
	expiresAt            time.Time
}

func expiry(expiresInMs int64) time.Time {
	if expiresInMs <= 0 {
		return time.Unix(math.MaxInt32, 0)
	}
	return time.Now().Add(time.Duration(expiresInMs) * time.Millisecond)
}

func (t *tokenHolder) setTokens(access, refresh, header string, expiresInMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = access
	if refresh != "" {
		t.refresh = refresh
	}
	t.head = header
	t.expiresAt = expiry(expiresInMs)
}

func (t *tokenHolder) setToken(access string, expiresInMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = access
	t.expiresAt = expiry(expiresInMs)
}

func (t *tokenHolder) access() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token
}

func (t *tokenHolder) refreshToken() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.refresh
}

// kafkaHeader returns the Kafka header token, falling back to the access token.
func (t *tokenHolder) kafkaHeader() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.head != "" {
		return t.head
	}
	return t.token
}

func (t *tokenHolder) expired() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token != "" && !time.Now().Before(t.expiresAt)
}
