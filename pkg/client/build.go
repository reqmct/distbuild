//go:build !solution

package client

import (
	"context"
	"distbuild/pkg/api"
	"distbuild/pkg/filecache"
	"errors"
	"go.uber.org/zap"
	"path/filepath"

	"distbuild/pkg/build"
)

type Client struct {
	l           *zap.Logger
	apiEndpoint string
	sourceDir   string
	buildClient *api.BuildClient
	fileCache   *filecache.Client
}

func NewClient(
	l *zap.Logger,
	apiEndpoint string,
	sourceDir string,
) *Client {
	clientLogger := l.Named("client")
	clientLogger.Info("initializing new client", zap.String("apiEndpoint", apiEndpoint), zap.String("sourceDir", sourceDir))
	return &Client{
		l:           clientLogger,
		apiEndpoint: apiEndpoint,
		sourceDir:   sourceDir,
		buildClient: api.NewBuildClient(clientLogger, apiEndpoint),
		fileCache:   filecache.NewClient(clientLogger, apiEndpoint),
	}
}

type BuildListener interface {
	OnJobStdout(jobID build.ID, stdout []byte) error
	OnJobStderr(jobID build.ID, stderr []byte) error

	OnJobFinished(jobID build.ID) error
	OnJobFailed(jobID build.ID, code int, error string) error
}

func resultToListener(lsn BuildListener, result *api.JobResult) error {
	if result.Error != nil {
		return errors.Join(lsn.OnJobFailed(result.ID, result.ExitCode, *result.Error),
			lsn.OnJobStdout(result.ID, result.Stdout),
			lsn.OnJobStderr(result.ID, result.Stderr),
		)
	}
	return errors.Join(
		lsn.OnJobFinished(result.ID),
		lsn.OnJobStdout(result.ID, result.Stdout),
		lsn.OnJobStderr(result.ID, result.Stderr),
	)
}

func (c *Client) Build(ctx context.Context, graph build.Graph, lsn BuildListener) error {
	c.l.Info("starting build process", zap.Any("graph", graph))
	buildStarted, reader, err := c.buildClient.StartBuild(ctx, &api.BuildRequest{Graph: graph})
	if err != nil {
		c.l.Error("failed to start build", zap.Error(err))
		return err
	}
	defer reader.Close()

	c.l.Info("build started", zap.String("buildID", buildStarted.ID.String()))
	for _, id := range buildStarted.MissingFiles {
		localPath := filepath.Join(c.sourceDir, graph.SourceFiles[id])
		c.l.Info("uploading file to cache", zap.String("fileID", id.String()), zap.String("localPath", localPath))
		if err := c.fileCache.Upload(ctx, id, localPath); err != nil {
			c.l.Error("failed to upload file", zap.String("fileID", id.String()), zap.String("localPath", localPath), zap.Error(err))
			return err
		}
	}

	signal := &api.SignalRequest{
		UploadDone: &api.UploadDone{},
	}
	c.l.Info("signaling build completion", zap.String("buildID", buildStarted.ID.String()))
	if _, err = c.buildClient.SignalBuild(ctx, buildStarted.ID, signal); err != nil {
		c.l.Error("failed to signal build completion", zap.String("buildID", buildStarted.ID.String()), zap.Error(err))
		return err
	}

	for {
		select {
		case <-ctx.Done():
			c.l.Info("build process cancelled")
			return ctx.Err()
		default:
		}
		upd, err := reader.Next()
		if err != nil {
			c.l.Error("error reading build update", zap.Error(err))
			return err
		}

		if upd.JobFinished != nil {
			if err := resultToListener(lsn, upd.JobFinished); err != nil {
				c.l.Error("error processing job result", zap.Error(err))
				return err
			}
		}
		if upd.BuildFailed != nil {
			err := errors.New(upd.BuildFailed.Error)
			c.l.Error("build failed", zap.String("error", err.Error()))
			return err
		}
		if upd.BuildFinished != nil {
			c.l.Info("build finished")
			break
		}
	}

	return nil
}
