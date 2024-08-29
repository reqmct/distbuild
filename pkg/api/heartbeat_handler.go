//go:build !solution

package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"
)

type HeartbeatHandler struct {
	l       *zap.Logger
	service HeartbeatService
}

func NewHeartbeatHandler(l *zap.Logger, s HeartbeatService) *HeartbeatHandler {
	return &HeartbeatHandler{
		l:       l.Named("heartbeat handler"),
		service: s,
	}
}

func (h *HeartbeatHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/heartbeat",
		func(w http.ResponseWriter, r *http.Request) {
			h.l.Info("Received heartbeat request", zap.String("method", r.Method), zap.String("remote_addr", r.RemoteAddr))

			contentType := r.Header.Get("Content-Type")
			if contentType != "application/json" {
				h.l.Warn("Invalid content type", zap.String("content_type", contentType))
				http.Error(w, fmt.Errorf("it's not json").Error(), http.StatusBadRequest)
				return
			}

			heartBeatReq := HeartbeatRequest{}
			if err := json.NewDecoder(r.Body).Decode(&heartBeatReq); err != nil {
				h.l.Error("Failed to decode heartbeat request", zap.Error(err))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			h.l.Debug("Heartbeat request decoded successfully", zap.Any("request", heartBeatReq))

			heartbeatResponse, err := h.service.Heartbeat(r.Context(), &heartBeatReq)
			if err != nil {
				h.l.Error("Heartbeat service error", zap.Error(err))
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(heartbeatResponse); err != nil {
				h.l.Error("Failed to encode response", zap.Error(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			h.l.Info("Heartbeat response sent successfully", zap.Any("response", heartbeatResponse))
		},
	)
}
