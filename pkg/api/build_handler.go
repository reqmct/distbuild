//go:build !solution

package api

import (
	"encoding/json"
	"github.com/reqmct/distbuild/pkg/build"
	"go.uber.org/zap"
	"io"
	"net/http"
	"sync"
)

func NewBuildService(l *zap.Logger, s Service) *BuildHandler {
	return &BuildHandler{
		l:       l.Named("build service"),
		service: s,
	}
}

type BuildHandler struct {
	l       *zap.Logger
	service Service
}

func (h *BuildHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/build", h.handleBuild)
	mux.HandleFunc("/signal", h.handleSignal)
}

func (h *BuildHandler) handleBuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.l.Error("incorrect method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	var buildReq BuildRequest
	if err := json.NewDecoder(r.Body).Decode(&buildReq); err != nil {
		h.l.Error("failed to decode BuildRequest", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	rc := http.NewResponseController(w)

	writer := newJSONStatusWriter(w, h.l, rc)

	err := h.service.StartBuild(ctx, &buildReq, writer)

	if err != nil {
		h.l.Error("failed to start build", zap.Error(err))
		writer.mu.Lock()
		if !writer.isStarted {
			defer writer.mu.Unlock()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writer.mu.Unlock()
		writer.SendBuildFailed(err.Error())
		return
	}
	<-writer.stopped
}

func (h *BuildHandler) handleSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.l.Error("incorrect method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()

	buildID := r.URL.Query().Get("build_id")
	if buildID == "" {
		h.l.Error("missing build_id in query")
		http.Error(w, "missing build_id", http.StatusBadRequest)
		return
	}

	var signalReq SignalRequest
	if err := json.NewDecoder(r.Body).Decode(&signalReq); err != nil {
		h.l.Error("failed to decode SignalRequest", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	id := build.NewID()

	if err := id.UnmarshalText([]byte(buildID)); err != nil {
		h.l.Error("incorrect build id", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := h.service.SignalBuild(ctx, id, &signalReq)
	if err != nil {
		h.l.Error("failed to signal build", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.l.Error("can't encode response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type JSONStatusWriter struct {
	writer    io.Writer
	l         *zap.Logger
	rc        *http.ResponseController
	isStarted bool
	isStopped bool
	mu        sync.Mutex
	stopped   chan struct{}
}

func newJSONStatusWriter(w io.Writer, logger *zap.Logger, rc *http.ResponseController) *JSONStatusWriter {
	return &JSONStatusWriter{
		writer:  w,
		l:       logger.Named("json status writer"),
		rc:      rc,
		stopped: make(chan struct{}),
	}
}

func (w *JSONStatusWriter) Started(rsp *BuildStarted) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.isStarted = true
	w.l.Info("build started", zap.String("buildID", rsp.ID.String()))
	err := json.NewEncoder(w.writer).Encode(rsp)

	if err != nil {
		return err
	}

	return w.rc.Flush()
}

func (w *JSONStatusWriter) Updated(update *StatusUpdate) error {
	w.mu.Lock()
	defer func() {
		if update.BuildFinished != nil || update.BuildFailed != nil {
			if !w.isStopped {
				w.isStopped = true
				close(w.stopped)
			}
		}
		w.mu.Unlock()
	}()
	w.l.Info("status updated")

	err := json.NewEncoder(w.writer).Encode(update)

	if err != nil {
		return err
	}

	return w.rc.Flush()
}

func (w *JSONStatusWriter) SendBuildFailed(errMsg string) {
	update := &StatusUpdate{
		BuildFailed: &BuildFailed{
			Error: errMsg,
		},
	}

	_ = w.Updated(update)
}
