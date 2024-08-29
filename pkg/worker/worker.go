//go:build !solution

package worker

import (
	"bytes"
	"context"
	"distbuild/pkg/build"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"

	"distbuild/pkg/api"
	"distbuild/pkg/artifact"
	"distbuild/pkg/filecache"
)

const (
	Delay            = 128 * time.Millisecond
	RunningJobsLimit = 4
)

type Worker struct {
	workerID            api.WorkerID
	coordinatorEndpoint string
	l                   *zap.Logger
	fileCache           *filecache.Cache
	fileCacheClient     *filecache.Client
	artifactsHandler    *artifact.Handler
	artifacts           *artifact.Cache
	heartbeatClient     *api.HeartbeatClient
	runningJobsCount    int
	finishedJobs        []api.JobResult
	jobsCache           map[build.ID]*api.JobResult
	runningJobs         map[build.ID]struct{}
	addedArtifacts      []build.ID
	mu                  sync.RWMutex
	mux                 *http.ServeMux
}

func New(
	workerID api.WorkerID,
	coordinatorEndpoint string,
	l *zap.Logger,
	fileCache *filecache.Cache,
	artifacts *artifact.Cache,
) *Worker {
	mux := http.NewServeMux()
	artifactsHandler := artifact.NewHandler(l, artifacts)
	artifactsHandler.Register(mux)
	l.Info("Created new worker", zap.String("workerID", string(workerID)))
	return &Worker{
		workerID:            workerID,
		coordinatorEndpoint: coordinatorEndpoint,
		l:                   l.Named("worker"),
		fileCache:           fileCache,
		fileCacheClient:     filecache.NewClient(l, coordinatorEndpoint),
		artifactsHandler:    artifactsHandler,
		artifacts:           artifacts,
		heartbeatClient:     api.NewHeartbeatClient(l, coordinatorEndpoint),
		jobsCache:           make(map[build.ID]*api.JobResult),
		runningJobs:         make(map[build.ID]struct{}),
		mux:                 mux,
	}
}

func (w *Worker) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mux.ServeHTTP(rw, r)
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			w.l.Info("worker context done, stopping")
			return ctx.Err()
		case <-time.After(Delay):
			break
		}
		w.mu.Lock()
		req := &api.HeartbeatRequest{
			WorkerID:       w.workerID,
			RunningJobs:    make([]build.ID, 0, len(w.runningJobs)),
			FreeSlots:      RunningJobsLimit - w.runningJobsCount,
			FinishedJob:    w.finishedJobs,
			AddedArtifacts: w.addedArtifacts,
		}

		for id := range w.runningJobs {
			req.RunningJobs = append(req.RunningJobs, id)
		}

		w.addedArtifacts = nil
		w.finishedJobs = nil

		w.mu.Unlock()

		resp, err := w.heartbeatClient.Heartbeat(ctx, req)
		if err != nil {
			w.l.Error("failed to send heartbeat", zap.Error(err))
			return err
		}

		w.mu.Lock()
		w.runningJobsCount += len(resp.JobsToRun)
		w.mu.Unlock()

		for id, jobSpec := range resp.JobsToRun {
			w.runningJobs[id] = struct{}{}
			go func(job api.JobSpec) {
				w.l.Info("starting job", zap.String("jobID", job.ID.String()))
				result := w.runJob(ctx, job)

				w.mu.Lock()
				defer w.mu.Unlock()
				w.runningJobsCount--
				w.finishedJobs = append(w.finishedJobs, result)
				delete(w.runningJobs, job.ID)

				if result.Error == nil {
					w.jobsCache[job.ID] = &result
					w.addedArtifacts = append(w.addedArtifacts, job.ID)
				}
			}(jobSpec)
		}
	}
}

func getErrorJobResult(err error) api.JobResult {
	msg := err.Error()
	return api.JobResult{
		Error: &msg,
	}
}

func (w *Worker) runJob(ctx context.Context, job api.JobSpec) api.JobResult {
	w.mu.RLock()
	oldJobResult, exist := w.jobsCache[job.ID]
	w.mu.RUnlock()

	if exist {
		w.l.Info("job already completed", zap.String("jobID", job.ID.String()))
		return *oldJobResult
	}

	sourceDir, err := w.downloadSourceFiles(ctx, job.SourceFiles)
	if err != nil {
		w.l.Error("failed to download source files", zap.String("jobID", job.ID.String()), zap.Error(err))
		return getErrorJobResult(err)
	}

	deps, err := w.downloadArtifacts(ctx, job.Artifacts)
	if err != nil {
		w.l.Error("failed to download artifacts", zap.String("jobID", job.ID.String()), zap.Error(err))
		return getErrorJobResult(err)
	}

	outputDir, commit, abort, err := w.artifacts.Create(job.ID)
	if err != nil {
		w.l.Error("failed to create artifact output directory", zap.String("jobID", job.ID.String()), zap.Error(err))
		return getErrorJobResult(err)
	}

	jobContext := build.JobContext{
		SourceDir: sourceDir,
		OutputDir: outputDir,
		Deps:      deps,
	}

	stdout := bytes.Buffer{}
	stderr := bytes.Buffer{}

	exitCode := 0

	var globalError error
	for _, rawCmd := range job.Cmds {
		cmd, err := rawCmd.Render(jobContext)
		if err != nil {
			w.l.Error("failed to render command", zap.String("jobID", job.ID.String()), zap.Error(err))
			globalError = err
			break
		}
		exitCode, err = execute(ctx, cmd, &stdout, &stderr)
		if err != nil {
			w.l.Error("error executing command", zap.String("jobID", job.ID.String()),
				zap.Int("exitCode", exitCode), zap.Error(err))
			globalError = err
			break
		}
	}

	jobResult := api.JobResult{
		ID:       job.ID,
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
		ExitCode: exitCode,
		Error:    nil,
	}

	if globalError != nil {
		errMsg := globalError.Error()
		jobResult.Error = &errMsg
		_ = abort()
		return jobResult
	}

	if err := commit(); err != nil {
		w.l.Error("failed to commit artifact", zap.String("jobID", job.ID.String()), zap.Error(err))
		return getErrorJobResult(errors.Join(err, abort()))
	}

	w.l.Info("job completed successfully", zap.String("jobID", job.ID.String()), zap.Int("exitCode", exitCode))
	return jobResult
}

func execute(ctx context.Context, cmd *build.Cmd, stdout io.Writer, stderr io.Writer) (int, error) {
	if cmd.Exec != nil && len(cmd.Exec) != 0 {
		execCmd := exec.CommandContext(ctx, cmd.Exec[0], cmd.Exec[1:]...)
		execCmd.Dir = cmd.WorkingDirectory
		execCmd.Env = cmd.Environ
		execCmd.Stdout = stdout
		execCmd.Stderr = stderr
		err := execCmd.Run()
		exitCode := execCmd.ProcessState.ExitCode()
		return exitCode, err
	} else if len(cmd.CatOutput) != 0 {
		file, err := os.Create(cmd.CatOutput)
		if err != nil {
			return 0, err
		}
		defer file.Close()

		_, err = file.WriteString(cmd.CatTemplate)
		if err != nil {
			return 0, err
		}
		return 0, nil
	}
	return 0, errors.New("incorrect command")
}

func (w *Worker) downloadSourceFiles(ctx context.Context, sourceFiles map[build.ID]string) (string, error) {
	dir, err := os.MkdirTemp("", "source_files")
	if err != nil {
		w.l.Error("failed to create temporary directory for source files", zap.Error(err))
		return "", err
	}

	for id, file := range sourceFiles {
		err := w.downloadFile(ctx, dir, id, file)
		if err != nil {
			_ = os.RemoveAll(dir)
			w.l.Error("failed to download file", zap.String("fileID", id.String()), zap.String("fileName", file), zap.Error(err))
			return "", err
		}
	}

	w.l.Info("downloaded source files", zap.String("directory", dir))
	return dir, nil
}

func (w *Worker) downloadFile(ctx context.Context, dir string, id build.ID, fileName string) error {
	file, err := createFile(ctx, dir, fileName)
	if err != nil {
		w.l.Error("failed to create file", zap.String("fileName", fileName), zap.Error(err))
		return err
	}
	defer file.Close()

	fileCachePath, unlock, err := w.fileCache.Get(id)
	if errors.Is(err, filecache.ErrNotFound) {
		w.l.Info("file not found in cache, downloading", zap.String("fileID", id.String()))
		downloadErr := w.fileCacheClient.Download(ctx, w.fileCache, id)
		if downloadErr != nil {
			w.l.Error("failed to download file", zap.String("fileID", id.String()), zap.Error(downloadErr))
			return downloadErr
		}
		fileCachePath, unlock, err = w.fileCache.Get(id)
	}

	if err != nil {
		w.l.Error("failed to get file from cache", zap.String("fileID", id.String()), zap.Error(err))
		return err
	}
	defer unlock()

	fileCache, err := os.Open(fileCachePath)
	if err != nil {
		w.l.Error("failed to open file from cache", zap.String("filePath", fileCachePath), zap.Error(err))
		return err
	}
	defer fileCache.Close()

	_, err = io.Copy(file, fileCache)
	if err != nil {
		w.l.Error("failed to copy file content", zap.String("fileID", id.String()), zap.Error(err))
		return err
	}

	w.l.Info("successfully downloaded file", zap.String("fileID", id.String()), zap.String("fileName", fileName))
	return nil
}

func createFile(ctx context.Context, dir string, fileName string) (*os.File, error) {
	filePath := path.Join(dir, fileName)

	err := os.MkdirAll(path.Dir(filePath), os.FileMode(0777))
	if err != nil {
		return nil, err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (w *Worker) downloadArtifacts(ctx context.Context, artifacts map[build.ID]api.WorkerID) (map[build.ID]string, error) {
	result := make(map[build.ID]string)
	for artifactID, workerID := range artifacts {
		w.l.Info("starting to download artifact", zap.String("artifactID", artifactID.String()))
		artifactPath, err := w.downloadArtifact(ctx, artifactID, workerID)
		if err != nil {
			w.l.Error("failed to download artifact", zap.String("artifactID", artifactID.String()), zap.Error(err))
			return nil, err
		}
		result[artifactID] = artifactPath
		w.l.Info("successfully downloaded artifact",
			zap.String("artifactID", artifactID.String()),
			zap.String("artifactPath", artifactPath),
		)
	}
	return result, nil
}

func (w *Worker) downloadArtifact(ctx context.Context, artifactID build.ID, workerID api.WorkerID) (string, error) {
	artifactPath, unlock, err := w.artifacts.Get(artifactID)

	if errors.Is(err, artifact.ErrNotFound) {
		w.l.Info("artifact not found in cache, downloading", zap.String("artifactID", artifactID.String()))
		downloadErr := artifact.Download(ctx, workerID.String(), w.artifacts, artifactID)
		if downloadErr != nil {
			w.l.Error("failed to download artifact", zap.String("artifactID", artifactID.String()), zap.Error(downloadErr))
			return "", downloadErr
		}

		w.mu.Lock()
		w.addedArtifacts = append(w.addedArtifacts, artifactID)
		w.mu.Unlock()

		artifactPath, unlock, err = w.artifacts.Get(artifactID)
	}

	if err != nil {
		w.l.Error("failed to get artifact from cache after download", zap.String("artifactID", artifactID.String()), zap.Error(err))
		return "", err
	}
	defer unlock()

	return artifactPath, nil
}
