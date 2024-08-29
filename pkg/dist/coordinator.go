//go:build !solution

package dist

import (
	"context"
	"distbuild/pkg/api"
	"distbuild/pkg/build"
	"errors"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"distbuild/pkg/filecache"
	"distbuild/pkg/scheduler"
)

type buildSpec struct {
	graph build.Graph
	sw    api.StatusWriter
}

type Coordinator struct {
	l                *zap.Logger
	fileCache        *filecache.Cache
	mux              *http.ServeMux
	scheduler        *scheduler.Scheduler
	fileCacheHandler *filecache.Handler
	buildHandler     *api.BuildHandler
	heartbeatHandler *api.HeartbeatHandler
	executableBuilds map[build.ID]buildSpec
	workers          map[api.WorkerID]struct{}
	mu               sync.Mutex
}

var defaultConfig = scheduler.Config{
	CacheTimeout: time.Millisecond * 10,
	DepsTimeout:  time.Millisecond * 100,
}

func NewCoordinator(
	l *zap.Logger,
	fileCache *filecache.Cache,
) *Coordinator {
	coordinator := &Coordinator{
		l:                l.Named("coordinator"),
		fileCache:        fileCache,
		scheduler:        scheduler.NewScheduler(l, defaultConfig),
		fileCacheHandler: filecache.NewHandler(l, fileCache),
		executableBuilds: make(map[build.ID]buildSpec),
		workers:          make(map[api.WorkerID]struct{}),
		mux:              http.NewServeMux(),
	}

	coordinator.fileCacheHandler.Register(coordinator.mux)
	coordinator.heartbeatHandler = api.NewHeartbeatHandler(l, coordinator)
	coordinator.heartbeatHandler.Register(coordinator.mux)
	coordinator.buildHandler = api.NewBuildService(l, coordinator)
	coordinator.buildHandler.Register(coordinator.mux)

	l.Info("coordinator created")

	return coordinator
}

func (c *Coordinator) Stop() {
	c.scheduler.Stop()
	c.l.Info("coordinator stopped")
}

func (c *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mux.ServeHTTP(w, r)
}

func (c *Coordinator) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {

	c.l.Info("processing heartbeat",
		zap.String("workerID", req.WorkerID.String()),
		zap.Int("freeSlots", req.FreeSlots),
	)

	resp := api.HeartbeatResponse{
		JobsToRun: make(map[build.ID]api.JobSpec),
	}
	c.mu.Lock()
	_, exist := c.workers[req.WorkerID]
	if !exist {
		c.workers[req.WorkerID] = struct{}{}
		c.l.Info("new worker registered",
			zap.String("workerID", req.WorkerID.String()),
		)
	}
	c.mu.Unlock()

	if !exist {
		c.scheduler.RegisterWorker(req.WorkerID)
	}
	for i, job := range req.FinishedJob {
		c.l.Info("job completed",
			zap.String("workerID", req.WorkerID.String()),
			zap.String("jobID", job.ID.String()),
		)
		c.scheduler.OnJobComplete(req.WorkerID, job.ID, &req.FinishedJob[i])
	}

	if req.FreeSlots <= 0 {
		return &resp, nil
	}

	pendingJob := c.scheduler.PickJob(ctx, req.WorkerID)
	if pendingJob == nil {
		c.l.Info("no pending jobs for worker",
			zap.String("workerID", req.WorkerID.String()),
		)
		return &resp, nil
	}
	resp.JobsToRun[pendingJob.Job.ID] = *pendingJob.Job

	c.l.Info("job assigned to worker",
		zap.String("workerID", req.WorkerID.String()),
		zap.String("jobID", pendingJob.Job.ID.String()),
	)

	return &resp, nil
}

func (c *Coordinator) StartBuild(ctx context.Context, request *api.BuildRequest, w api.StatusWriter) error {
	buildID := build.NewID()
	c.mu.Lock()
	c.executableBuilds[buildID] = buildSpec{
		graph: request.Graph,
		sw:    w,
	}
	c.mu.Unlock()

	buildStarted := api.BuildStarted{
		ID:           buildID,
		MissingFiles: make([]build.ID, 0),
	}

	c.l.Info("build started",
		zap.String("buildID", buildID.String()),
	)

	for id := range request.Graph.SourceFiles {
		_, unlock, err := c.fileCache.Get(id)
		if errors.Is(err, filecache.ErrNotFound) {
			buildStarted.MissingFiles = append(buildStarted.MissingFiles, id)
			c.l.Warn("missing file for build",
				zap.String("buildID", buildID.String()),
				zap.String("fileID", id.String()),
			)
		} else if err != nil {
			return err
		} else {
			unlock()
		}
	}

	return w.Started(&buildStarted)
}

func (c *Coordinator) SignalBuild(ctx context.Context, buildID build.ID, signal *api.SignalRequest) (*api.SignalResponse, error) {
	if signal.UploadDone != nil {
		c.l.Info("processing build signal",
			zap.String("buildID", buildID.String()),
		)

		c.mu.Lock()
		executableBuild := c.executableBuilds[buildID]
		delete(c.executableBuilds, buildID)
		c.mu.Unlock()

		jobs := build.TopSort(executableBuild.graph.Jobs)
		reverseSourceFiles := make(map[string]build.ID)
		for id, path := range executableBuild.graph.SourceFiles {
			reverseSourceFiles[path] = id
		}

		resultChan := make(chan *api.JobResult, len(jobs))

		go func() {
			for i := 1; i <= len(jobs); i++ {
				res := <-resultChan
				upd := api.StatusUpdate{
					JobFinished: res,
				}

				if i == len(jobs) {
					upd.BuildFinished = &api.BuildFinished{}
				}
				_ = executableBuild.sw.Updated(&upd)
			}
		}()

		for _, job := range jobs {
			jobSpec := api.JobSpec{
				SourceFiles: make(map[build.ID]string),
				Artifacts:   make(map[build.ID]api.WorkerID),
				Job:         job,
			}

			for _, in := range job.Inputs {
				jobSpec.SourceFiles[reverseSourceFiles[in]] = in
			}

			pendingJob := c.scheduler.ScheduleJob(&jobSpec)
			go func() {
				<-pendingJob.Finished
				resultChan <- pendingJob.Result
			}()
		}

		c.l.Info("build signal processed",
			zap.String("buildID", buildID.String()),
		)
	}
	return &api.SignalResponse{}, nil
}
