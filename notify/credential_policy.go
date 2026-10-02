package notify

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

// ErrCredentialsInPayload is returned when a payload carries credential-like
// fields. Port of the Java CredentialPayloadPolicy, applied to captures before
// they are published to Kafka.
var ErrCredentialsInPayload = errors.New("notify: credentials are not allowed in notification payloads")

var (
	credentialKeys = []string{"credentials", "secret", "signingsecret", "callbacksecret", "password", "apikey",
		"apisecret", "authtoken", "accesstoken", "refreshtoken", "authorization", "privatekey",
		"serviceaccountjson", "secretstring", "secretbinary", "awssecretarn"}
	nonAlnum = regexp.MustCompile(`[^a-z0-9]`)
)

// ValidateCredentialFree rejects data whose keys look like credentials
// (password, apiKey, *_secret, ...) or whose strings contain private keys.
// Message prose is not inspected beyond that.
func ValidateCredentialFree(data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return ErrCredentialsInPayload
	}
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		return ErrCredentialsInPayload
	}
	if !credentialFree(tree, 0) {
		return ErrCredentialsInPayload
	}
	return nil
}

func credentialFree(node any, depth int) bool {
	if depth > 50 {
		return false
	}
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			compact := nonAlnum.ReplaceAllString(strings.ToLower(k), "")
			for _, key := range credentialKeys {
				if compact == key || strings.HasSuffix(compact, key) {
					return false
				}
			}
			if !credentialFree(v, depth+1) {
				return false
			}
		}
	case []any:
		for _, v := range n {
			if !credentialFree(v, depth+1) {
				return false
			}
		}
	case string:
		text := strings.TrimSpace(n)
		if strings.Contains(text, "-----BEGIN PRIVATE KEY-----") || strings.Contains(text, "-----BEGIN RSA PRIVATE KEY-----") {
			return false
		}
		if strings.HasPrefix(text, "{") || strings.HasPrefix(text, "[") {
			var nested any
			if json.Unmarshal([]byte(text), &nested) == nil && !credentialFree(nested, depth+1) {
				return false
			}
		}
	}
	return true
}
