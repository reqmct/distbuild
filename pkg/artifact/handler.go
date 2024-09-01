//go:build !solution

package artifact

import (
	"github.com/reqmct/distbuild/pkg/build"
	"github.com/reqmct/distbuild/pkg/tarstream"
	"net/http"

	"go.uber.org/zap"
)

type Handler struct {
	l *zap.Logger
	c *Cache
}

func NewHandler(l *zap.Logger, c *Cache) *Handler {
	return &Handler{
		l: l.Named("artifact handler"),
		c: c,
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/artifact", h.artifactHandle)
}

func (h *Handler) artifactHandle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.l.Error("incorrect method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	queryId := r.URL.Query().Get("id")
	if queryId == "" {
		h.l.Error("missing id in query")
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}

	id := build.NewID()
	if err := id.UnmarshalText([]byte(queryId)); err != nil {
		h.l.Error("incorrect artifact id", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.c.Get(id)
	if err != nil {
		h.l.Error("can't get artifact", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer unlock()
	err = tarstream.Send(path, w)

	if err != nil {
		h.l.Error("can't serialize artifact", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
