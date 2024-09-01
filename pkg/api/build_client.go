//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/reqmct/distbuild/pkg/build"
	"go.uber.org/zap"
	"io"
	"net/http"
)

type BuildClient struct {
	l        *zap.Logger
	endpoint string
}

func NewBuildClient(l *zap.Logger, endpoint string) *BuildClient {
	return &BuildClient{
		l:        l.Named("build client"),
		endpoint: endpoint,
	}
}

func (c *BuildClient) StartBuild(ctx context.Context, request *BuildRequest) (*BuildStarted, StatusReader, error) {
	url := fmt.Sprintf("%s/build", c.endpoint)
	reqBody, err := json.Marshal(request)
	if err != nil {
		c.l.Error("failed to marshal BuildRequest", zap.Error(err))
		return nil, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(bytes.NewReader(reqBody)))
	if err != nil {
		c.l.Error("failed to create HTTP request", zap.Error(err))
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.l.Error("failed to send HTTP request", zap.Error(err))
		return nil, nil, err
	}

	if resp.StatusCode != http.StatusOK {
		errText, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, nil, err
		}
		defer resp.Body.Close()

		err = errors.New(string(errText))
		c.l.Error(fmt.Sprintf("unexpected response status %d:", resp.StatusCode), zap.Error(err))
		return nil, nil, err
	}

	var buildStarted BuildStarted
	if err := json.NewDecoder(resp.Body).Decode(&buildStarted); err != nil {
		c.l.Error("failed to decode BuildStarted", zap.Error(err))
		_ = resp.Body.Close()
		return nil, nil, err
	}

	c.l.Info("build started", zap.String("buildID", buildStarted.ID.String()))

	return &buildStarted, newJSONStatusReader(resp), nil
}

func (c *BuildClient) SignalBuild(ctx context.Context, buildID build.ID, signal *SignalRequest) (*SignalResponse, error) {
	url := fmt.Sprintf("%s/signal?build_id=%s", c.endpoint, buildID.String())
	reqBody, err := json.Marshal(signal)
	if err != nil {
		c.l.Error("failed to marshal SignalRequest", zap.Error(err))
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, io.NopCloser(bytes.NewReader(reqBody)))
	if err != nil {
		c.l.Error("failed to create HTTP request", zap.Error(err))
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.l.Error("failed to send HTTP request", zap.Error(err))
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errText, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		err = errors.New(string(errText))
		c.l.Error(fmt.Sprintf("unexpected response status %d:", resp.StatusCode), zap.Error(err))
		return nil, err
	}

	var signalResp SignalResponse
	if err := json.NewDecoder(resp.Body).Decode(&signalResp); err != nil {
		c.l.Error("failed to decode SignalResponse", zap.Error(err))
		return nil, err
	}

	c.l.Info("signal sent", zap.String("buildID", buildID.String()))

	return &signalResp, nil
}

type JSONStatusReader struct {
	resp     *http.Response
	isClosed bool
}

func newJSONStatusReader(resp *http.Response) StatusReader {
	return &JSONStatusReader{
		resp: resp,
	}
}

func (r *JSONStatusReader) Next() (*StatusUpdate, error) {
	if r.isClosed {
		return nil, errors.New("is closed")
	}
	if r.resp.StatusCode != http.StatusOK {
		msg, err := io.ReadAll(r.resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(string(msg))
	}
	var update StatusUpdate
	err := json.NewDecoder(r.resp.Body).Decode(&update)
	return &update, err
}

func (r *JSONStatusReader) Close() error {
	if !r.isClosed {
		r.isClosed = true
		return r.resp.Body.Close()
	}
	return errors.New("already closed")
}
