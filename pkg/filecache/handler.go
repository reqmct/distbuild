//go:build !solution

package filecache

import (
	"distbuild/pkg/build"
	"errors"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"net/url"
	"os"

	"go.uber.org/zap"
)

type Handler struct {
	l     *zap.Logger
	cache *Cache
	gr    singleflight.Group
}

func NewHandler(l *zap.Logger, cache *Cache) *Handler {
	return &Handler{
		l:     l.Named("file handler"),
		cache: cache,
	}
}

func (h *Handler) fileHandle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.getFileHandle(w, r)
		return
	case http.MethodPut:
		h.putFileHandle(w, r)
		return
	default:
		h.l.Error("incorrect method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func getID(url *url.URL) (*build.ID, error) {
	queryId := url.Query().Get("id")
	if queryId == "" {
		return nil, errors.New("missing id in query")
	}

	id := build.NewID()
	if err := id.UnmarshalText([]byte(queryId)); err != nil {
		return nil, errors.New("incorrect id")
	}
	return &id, nil
}

func (h *Handler) getFileHandle(w http.ResponseWriter, r *http.Request) {
	id, err := getID(r.URL)
	if err != nil {
		h.l.Error("can't get id", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	path, unlock, err := h.cache.Get(*id)
	if err != nil {
		h.l.Error("can't get file", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer unlock()

	file, err := os.Open(path)
	if err != nil {
		h.l.Error("can't open file", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	_, err = io.Copy(w, file)
	if err != nil {
		h.l.Error("can't copy file", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (h *Handler) putFileHandle(w http.ResponseWriter, r *http.Request) {
	id, err := getID(r.URL)
	if err != nil {
		h.l.Error("can't get id", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err, _ = h.gr.Do(id.String(),
		func() (interface{}, error) {
			w, abort, err := h.cache.Write(*id)
			if errors.Is(err, ErrExists) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			defer w.Close()

			_, err = io.Copy(w, r.Body)
			if err != nil {
				_ = abort()
				return nil, err
			}
			defer r.Body.Close()

			return nil, nil
		},
	)
	if err != nil {
		h.l.Error("can't write file", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/file", h.fileHandle)
}
