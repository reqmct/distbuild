//go:build !solution

package artifact

import (
	"context"
	"distbuild/pkg/tarstream"
	"errors"
	"fmt"
	"io"
	"net/http"

	"distbuild/pkg/build"
)

// Download artifact from remote cache into local cache.
func Download(ctx context.Context, endpoint string, c *Cache, artifactID build.ID) error {
	url := fmt.Sprintf("%s/artifact?id=%s", endpoint, artifactID.String())

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

	path, commit, abort, err := c.Create(artifactID)
	if err != nil {
		return err
	}

	err = tarstream.Receive(path, resp.Body)

	if err != nil {
		_ = abort()
		return err
	}

	return commit()
}
