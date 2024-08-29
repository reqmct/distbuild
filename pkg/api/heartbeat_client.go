//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/url"
)

type HeartbeatClient struct {
	l        *zap.Logger
	endpoint string
}

func NewHeartbeatClient(l *zap.Logger, endpoint string) *HeartbeatClient {
	return &HeartbeatClient{
		l:        l.Named("heartbeat client"),
		endpoint: endpoint,
	}
}

func (c *HeartbeatClient) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	c.l.Info("Starting heartbeat request", zap.String("endpoint", c.endpoint))

	link, err := url.JoinPath(c.endpoint, "/heartbeat")
	if err != nil {
		c.l.Error("Failed to join URL path", zap.Error(err))
		return nil, err
	}

	c.l.Debug("Formed request URL", zap.String("link", link))

	var requestBody bytes.Buffer
	if err := json.NewEncoder(&requestBody).Encode(req); err != nil {
		c.l.Error("Failed to encode request body", zap.Error(err))
		return nil, err
	}

	reqWithCtx, err := http.NewRequestWithContext(ctx, "POST", link, &requestBody)
	if err != nil {
		c.l.Error("Failed to create new request with context", zap.Error(err))
		return nil, err
	}

	reqWithCtx.Header.Set("Content-Type", "application/json")

	client := http.Client{}
	resp, err := client.Do(reqWithCtx)
	if err != nil {
		c.l.Error("Request failed", zap.Error(err))
		return nil, err
	}
	defer resp.Body.Close()

	c.l.Debug("Received response", zap.Int("status_code", resp.StatusCode))

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.l.Error("Failed to read error response body", zap.Error(err))
			return nil, err
		}
		c.l.Error("Request returned non-OK status", zap.String("response_body", string(body)))
		return nil, fmt.Errorf(string(body))
	}

	heartbeatResponse := HeartbeatResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&heartbeatResponse); err != nil {
		c.l.Error("Failed to decode response body", zap.Error(err))
		return nil, err
	}

	c.l.Info("Heartbeat request successful", zap.Any("response", heartbeatResponse))

	return &heartbeatResponse, nil
}
