//go:build !solution

package scheduler

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"distbuild/pkg/api"
	"distbuild/pkg/build"
)

var TimeAfter = time.After

type PendingJob struct {
	Job      *api.JobSpec
	Finished chan struct{}
	Result   *api.JobResult
}

type Config struct {
	CacheTimeout time.Duration
	DepsTimeout  time.Duration
}

type taskChan = chan *PendingJob

type Scheduler struct {
	l              *zap.Logger
	cfg            Config
	mu             sync.RWMutex
	workers        map[api.WorkerID]worker
	pendingJobs    map[build.ID]*PendingJob
	cachedArtifact map[build.ID][]api.WorkerID
	global         chan taskChan
	stopped        chan struct{}
}

type worker struct {
	local1 chan taskChan
	local2 chan taskChan
}

func NewScheduler(l *zap.Logger, config Config) *Scheduler {
	return &Scheduler{
		l:              l.Named("scheduler"),
		cfg:            config,
		workers:        make(map[api.WorkerID]worker),
		pendingJobs:    make(map[build.ID]*PendingJob),
		cachedArtifact: make(map[build.ID][]api.WorkerID),
		global:         make(chan taskChan),
		stopped:        make(chan struct{}),
	}
}

func (c *Scheduler) RegisterWorker(workerID api.WorkerID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[workerID] = worker{
		local1: make(chan taskChan),
		local2: make(chan taskChan),
	}
	c.l.Info("worker registered", zap.String("workerID", string(workerID)))
}

func (c *Scheduler) locateArtifact(id build.ID) (api.WorkerID, bool) {
	ids, ok := c.cachedArtifact[id]
	if !ok || len(ids) == 0 {
		return "", false
	}
	return ids[len(ids)-1], true
}

func (c *Scheduler) LocateArtifact(id build.ID) (api.WorkerID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	workerID, ok := c.locateArtifact(id)
	if ok {
		c.l.Info("artifact located", zap.String("artifactID", id.String()),
			zap.String("workerID", string(workerID)),
		)
	} else {
		c.l.Warn("artifact not found", zap.String("artifactID", id.String()))
	}
	return workerID, ok
}

func (c *Scheduler) OnJobComplete(workerID api.WorkerID, jobID build.ID, res *api.JobResult) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	pendingJob, ok := c.pendingJobs[jobID]

	if ok {
		pendingJob.Result = res
		defer close(pendingJob.Finished)
		delete(c.pendingJobs, jobID)
		c.l.Info("job completed",
			zap.String("workerID", string(workerID)),
			zap.String("jobID", jobID.String()),
		)
	} else {
		c.l.Warn("job not found in pending jobs", zap.String("jobID", jobID.String()))
	}

	tmp := c.cachedArtifact[jobID]
	c.cachedArtifact[jobID] = append(tmp, workerID)

	return ok
}

func (c *Scheduler) ScheduleJob(job *api.JobSpec) *PendingJob {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pendingJob, exist := c.pendingJobs[job.ID]; exist {
		c.l.Info("job already scheduled", zap.String("jobID", job.ID.String()))
		return pendingJob
	}

	pendingJob := &PendingJob{
		Job:      job,
		Finished: make(chan struct{}),
		Result:   nil,
	}
	c.pendingJobs[job.ID] = pendingJob

	c.l.Info("job scheduled", zap.String("jobID", job.ID.String()))

	job.Artifacts = make(map[build.ID]api.WorkerID)
	var waitingFor []*PendingJob
	for _, id := range job.Deps {
		if workerId, ok := c.locateArtifact(id); ok {
			job.Artifacts[id] = workerId
			c.l.Info("dependency found in cache", zap.String("jobID", job.ID.String()),
				zap.String("artifactID", job.ID.String()),
				zap.String("workerID", string(workerId)),
			)
		} else {
			waitingFor = append(waitingFor, c.pendingJobs[id])
			c.l.Info("waiting for dependency",
				zap.String("jobID", job.ID.String()),
				zap.String("artifactID", job.ID.String()),
			)
		}
	}

	go func() {
		for _, dep := range waitingFor {
			select {
			case <-dep.Finished:
				c.l.Info("dependency resolved", zap.String("jobID", job.ID.String()))
			case <-c.stopped:
				c.l.Info("scheduler stopped, exiting dependency wait", zap.String("jobID", job.ID.String()))
				return
			}
			workerId, _ := c.LocateArtifact(dep.Job.ID)
			job.Artifacts[dep.Job.ID] = workerId
		}

		anotherJob := make(chan *PendingJob, 1)
		anotherJob <- pendingJob
		ok := false

		c.mu.RLock()
		for _, id := range c.cachedArtifact[job.ID] {
			ok = true
			if w, exist := c.workers[id]; exist {
				go func() {
					select {
					case w.local1 <- anotherJob:
						c.l.Info("job sent to worker local1", zap.String("jobID", job.ID.String()),
							zap.String("workerID", string(id)),
						)
					case <-c.stopped:
						c.l.Info("scheduler stopped, exiting worker dispatch", zap.String("jobID", job.ID.String()))
						return
					}
				}()
			}
		}
		c.mu.RUnlock()

		if ok {
			select {
			case <-TimeAfter(c.cfg.CacheTimeout):
				c.l.Info("cache timeout exceeded", zap.String("jobID", job.ID.String()))
			case <-c.stopped:
				c.l.Info("scheduler stopped during cache wait", zap.String("jobID", job.ID.String()))
				return
			}
		}

		pendingJobCopy := <-anotherJob
		if pendingJobCopy == nil {
			return
		}
		anotherJob <- pendingJobCopy

		c.mu.RLock()
		for _, jobId := range job.Deps {
			for _, workerId := range c.cachedArtifact[jobId] {
				if w, exist := c.workers[workerId]; exist {
					go func() {
						select {
						case w.local2 <- anotherJob:
							c.l.Info("job sent to worker local2",
								zap.String("jobID", job.ID.String()),
								zap.String("workerID", string(workerId)),
							)
						case <-c.stopped:
							c.l.Info("scheduler stopped, exiting worker dispatch",
								zap.String("jobID", job.ID.String()),
							)
							return
						}
					}()
				}
			}
		}
		c.mu.RUnlock()

		select {
		case <-TimeAfter(c.cfg.DepsTimeout):
			c.l.Info("deps timeout exceeded", zap.String("jobID", job.ID.String()))
		case <-c.stopped:
			c.l.Info("scheduler stopped during deps wait", zap.String("jobID", job.ID.String()))
			return
		}

		pendingJobCopy = <-anotherJob
		if pendingJobCopy == nil {
			return
		}
		anotherJob <- pendingJobCopy

		select {
		case c.global <- anotherJob:
			c.l.Info("job sent to global queue", zap.String("jobID", job.ID.String()))
		case <-c.stopped:
			c.l.Info("scheduler stopped during global queue dispatch", zap.String("jobID", job.ID.String()))
			return
		}
	}()

	return pendingJob
}

func (c *Scheduler) PickJob(ctx context.Context, workerID api.WorkerID) *PendingJob {
	c.mu.RLock()
	w, ok := c.workers[workerID]
	c.mu.RUnlock()
	if !ok {
		c.l.Warn("worker not found", zap.String("workerID", string(workerID)))
		return nil
	}
	var job *PendingJob
	for {
		var curr taskChan
		select {
		case curr = <-c.global:
		case curr = <-w.local1:
		case curr = <-w.local2:
		default:
			c.l.Info("there are no job", zap.String("workerID", workerID.String()))
			return nil
		}
		job = <-curr
		if job != nil {
			c.l.Info("job picked by worker", zap.String("workerID", string(workerID)))
			close(curr)
			return job
		}
	}
}

func (c *Scheduler) Stop() {
	c.l.Info("scheduler stopping...")
	close(c.stopped)
	time.Sleep(time.Second)
}
