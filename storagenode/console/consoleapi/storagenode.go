// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package consoleapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/zeebo/errs"
	"go.uber.org/zap"

	"storj.io/common/storj"
	"storj.io/storj/storagenode/console"
	"storj.io/storj/storagenode/piecestore"
)

// ErrStorageNodeAPI - console storagenode api error type.
var ErrStorageNodeAPI = errs.Class("consoleapi storagenode")

// StorageNode is an api controller that exposes all dashboard related api.
type StorageNode struct {
	service *console.Service

	log *zap.Logger
}

// NewStorageNode is a constructor for sno controller.
func NewStorageNode(log *zap.Logger, service *console.Service) *StorageNode {
	return &StorageNode{
		log:     log,
		service: service,
	}
}

// StorageNode handles StorageNode API requests.
func (dashboard *StorageNode) StorageNode(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	data, err := dashboard.service.GetDashboardData(ctx)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		dashboard.log.Error("failed to encode json response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
		return
	}
}

// Compaction handles compaction statistics API requests.
func (dashboard *StorageNode) Compaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	data, err := dashboard.service.GetCompactionData(ctx)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		dashboard.log.Error("failed to encode json response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
	}
}

// StartCompaction handles requests to start an asynchronous manual full compaction.
func (dashboard *StorageNode) StartCompaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		w.Header().Set("WWW-Authenticate", "Bearer")
		dashboard.serveCompactionError(w, http.StatusUnauthorized, "unauthorized", "a valid multinode API key is required")
		return
	}
	if err := dashboard.service.AuthenticateAPIKey(ctx, fields[1]); err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		dashboard.serveCompactionError(w, http.StatusUnauthorized, "unauthorized", "a valid multinode API key is required")
		return
	}

	job, err := dashboard.service.StartManualCompaction(ctx)
	if err != nil {
		switch {
		case errors.Is(err, piecestore.ErrManualCompactionDisabled):
			dashboard.serveCompactionError(w, http.StatusPreconditionFailed, "manual_log_compaction_disabled", err.Error())
		case errors.Is(err, piecestore.ErrManualCompactionRunning):
			dashboard.serveCompactionError(w, http.StatusConflict, "manual_compaction_running", err.Error())
		case errors.Is(err, piecestore.ErrHashStoreBackendClosed):
			dashboard.serveCompactionError(w, http.StatusServiceUnavailable, "hashstore_closed", err.Error())
		default:
			dashboard.serveCompactionError(w, http.StatusInternalServerError, "manual_compaction_failed", err.Error())
		}
		return
	}

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(struct {
		ManualJob console.ManualCompactionJob `json:"manualJob"`
	}{ManualJob: job}); err != nil {
		dashboard.log.Error("failed to encode manual compaction response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
	}
}

func (dashboard *StorageNode) serveCompactionError(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(struct {
		Code  string `json:"code"`
		Error string `json:"error"`
	}{Code: code, Error: message}); err != nil {
		dashboard.log.Error("failed to encode compaction error", zap.Error(ErrStorageNodeAPI.Wrap(err)))
	}
}

// Satellites handles satellites API request.
func (dashboard *StorageNode) Satellites(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	data, err := dashboard.service.GetAllSatellitesData(ctx)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		dashboard.log.Error("failed to encode json response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
		return
	}
}

// Satellite handles satellite API requests.
func (dashboard *StorageNode) Satellite(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	params := mux.Vars(r)
	id, ok := params["id"]
	if !ok {
		dashboard.serveJSONError(w, http.StatusBadRequest, ErrStorageNodeAPI.Wrap(err))
		return
	}

	satelliteID, err := storj.NodeIDFromString(id)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusBadRequest, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err = dashboard.service.VerifySatelliteID(ctx, satelliteID); err != nil {
		dashboard.serveJSONError(w, http.StatusNotFound, ErrStorageNodeAPI.Wrap(err))
		return
	}

	data, err := dashboard.service.GetSatelliteData(ctx, satelliteID)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		dashboard.log.Error("failed to encode json response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
		return
	}
}

// EstimatedPayout returns estimated payouts from specific satellite or all satellites if current traffic level remains same.
func (dashboard *StorageNode) EstimatedPayout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	now := time.Now()

	queryParams := r.URL.Query()
	id := queryParams.Get("id")
	if id == "" {
		data, err := dashboard.service.GetAllSatellitesEstimatedPayout(ctx, now)
		if err != nil {
			dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
			return
		}

		if err := json.NewEncoder(w).Encode(data); err != nil {
			dashboard.log.Error("failed to encode json response", zap.Error(ErrPayoutAPI.Wrap(err)))
			return
		}
	} else {
		satelliteID, err := storj.NodeIDFromString(id)
		if err != nil {
			dashboard.serveJSONError(w, http.StatusBadRequest, ErrPayoutAPI.Wrap(err))
			return
		}

		data, err := dashboard.service.GetSatelliteEstimatedPayout(ctx, satelliteID, now)
		if err != nil {
			dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
			return
		}

		if err := json.NewEncoder(w).Encode(data); err != nil {
			dashboard.log.Error("failed to encode json response", zap.Error(ErrPayoutAPI.Wrap(err)))
			return
		}
	}
}

// Pricing returns pricing model for specific satellite.
func (dashboard *StorageNode) Pricing(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	defer mon.Task()(&ctx)(&err)

	w.Header().Set(contentType, applicationJSON)

	params := mux.Vars(r)
	id, ok := params["id"]
	if !ok {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}
	satelliteID, err := storj.NodeIDFromString(id)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusBadRequest, ErrStorageNodeAPI.Wrap(err))
		return
	}

	data, err := dashboard.service.GetSatellitePricingModel(ctx, satelliteID)
	if err != nil {
		dashboard.serveJSONError(w, http.StatusInternalServerError, ErrStorageNodeAPI.Wrap(err))
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		dashboard.log.Error("failed to encode json response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
		return
	}
}

// serveJSONError writes JSON error to response output stream.
func (dashboard *StorageNode) serveJSONError(w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)

	var response struct {
		Error string `json:"error"`
	}

	response.Error = err.Error()

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		dashboard.log.Error("failed to write json error response", zap.Error(ErrStorageNodeAPI.Wrap(err)))
		return
	}
}
