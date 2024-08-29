//go:build !solution

package filecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"go.uber.org/zap"

	"distbuild/pkg/build"
)

type Client struct {
	l        *zap.Logger
	endpoint string
}

func NewClient(l *zap.Logger, endpoint string) *Client {
	return &Client{
		l:        l,
		endpoint: endpoint,
	}
}

func (c *Client) Upload(ctx context.Context, id build.ID, localPath string) error {
	url := fmt.Sprintf("%s/file?id=%s", c.endpoint, id.String())

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, file)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		text, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return errors.New(string(text))
	}

	return nil
}

func (c *Client) Download(ctx context.Context, localCache *Cache, id build.ID) error {
	url := fmt.Sprintf("%s/file?id=%s", c.endpoint, id.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		text, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(text))
	}

	w, abort, err := localCache.Write(id)
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		_ = abort()
		return err
	}

	return nil
}
