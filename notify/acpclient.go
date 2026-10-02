package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPError is a non-2xx response from acp-server.
type HTTPError struct {
	Method, Path string
	StatusCode   int
	Body         string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("notify: %s %s failed: %d %s", e.Method, e.Path, e.StatusCode, e.Body)
}

func isUnauthorized(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.StatusCode == http.StatusUnauthorized
}

// acpClient mirrors AcpServerClient's HTTP surface.
type acpClient struct {
	baseURL string
	http    *http.Client
}

func (c *acpClient) do(ctx context.Context, path string, timeout time.Duration, body any, bearer string, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	// 409 Conflict means the server's idempotency filter saw a duplicate.
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusConflict {
		return &HTTPError{Method: "POST", Path: path, StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	if out != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

func (c *acpClient) register(ctx context.Context, req RegistrationRequest) (*RegistrationResponse, error) {
	var resp RegistrationResponse
	if err := c.do(ctx, "/client/register", 15*time.Second, req, "", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *acpClient) refreshToken(ctx context.Context, req TokenRefreshRequest) (*TokenRefreshResponse, error) {
	var resp TokenRefreshResponse
	if err := c.do(ctx, "/auth/token/refresh", 10*time.Second, req, "", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *acpClient) postVocabulary(ctx context.Context, models []ClassModel, bearer string) error {
	if len(models) == 0 {
		return nil
	}
	return c.do(ctx, "/api/vocabulary", 30*time.Second, models, bearer, nil)
}

func (c *acpClient) postRule(ctx context.Context, rule map[string]any, bearer string) error {
	return c.do(ctx, "/api/vocabulary/rules/process", 30*time.Second, rule, bearer, nil)
}

func (c *acpClient) postEventCaptures(ctx context.Context, captures []*EventCapture, bearer string) error {
	if len(captures) == 0 {
		return nil
	}
	return c.do(ctx, "/api/event", 30*time.Second, captures, bearer, nil)
}
