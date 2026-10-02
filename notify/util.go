package notify

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf16"
)

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// javaStringHashCode reproduces java.lang.String#hashCode so Kafka partition
// selection matches the Java SDK for the same tenant / subject id.
func javaStringHashCode(s string) int32 {
	var h int32
	for _, c := range utf16.Encode([]rune(s)) {
		h = 31*h + int32(c)
	}
	return h
}

// javaPartition mirrors Math.abs(s.hashCode()) % n, avoiding Java's negative
// result for Integer.MIN_VALUE.
func javaPartition(s string, n int) int32 {
	if n <= 0 {
		return 0
	}
	h := int64(javaStringHashCode(s))
	if h < 0 {
		h = -h
	}
	return int32(h % int64(n))
}

// jwtClaims decodes a JWT payload without verifying it. The SDK does not hold
// the server's signing key; it reads claims only to pick a transport and a
// partition key, and the server validates the signature on receipt.
func jwtClaims(token string) map[string]any {
	token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil
	}
	var claims map[string]any
	if json.Unmarshal(raw, &claims) != nil {
		return nil
	}
	return claims
}

func tenantIDFromToken(token string) string {
	if v, ok := jwtClaims(token)["tenantId"]; ok && v != nil {
		return fmt.Sprint(v)
	}
	return "unknown-tenant"
}

func hasBasicProfile(token string) bool {
	return jwtClaims(token)["profile"] == "basic"
}

func strPtr(s string) *string { return &s }
