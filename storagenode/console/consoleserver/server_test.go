// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package consoleserver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/storagenode"
	"storj.io/storj/storagenode/apikeys"
)

func TestConsole(t *testing.T) {
	testplanet.Run(t,
		testplanet.Config{
			SatelliteCount:   1,
			StorageNodeCount: 1,
		},
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
			satellite := planet.Satellites[0]
			sno := planet.StorageNodes[0]
			console := sno.Console

			addr := console.Listener.Addr()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/sno", addr), nil)
			require.NoError(t, err)
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/sno/satellites", addr), nil)
			require.NoError(t, err)
			res, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/sno/satellite/%s", addr, satellite.ID()), nil)
			require.NoError(t, err)
			res, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/sno/compaction", addr), nil)
			require.NoError(t, err)
			res, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)
			var compaction struct {
				Compacting                 bool `json:"compacting"`
				SalvageEnabled             bool `json:"salvageEnabled"`
				ManualLogCompactionEnabled bool `json:"manualLogCompactionEnabled"`
				ManualJob                  struct {
					State string `json:"state"`
				} `json:"manualJob"`
				RuntimeTotals struct {
					FinishedAttempts uint64 `json:"finishedAttempts"`
				} `json:"runtimeTotals"`
				Salvage struct {
					LostPieces uint64 `json:"lostPieces"`
				} `json:"salvage"`
				Satellites []struct {
					SatelliteID string `json:"satelliteID"`
				} `json:"satellites"`
			}
			require.NoError(t, json.NewDecoder(res.Body).Decode(&compaction))
			require.NoError(t, res.Body.Close())
			require.NotNil(t, compaction.Satellites)
			require.False(t, compaction.ManualLogCompactionEnabled)
			require.Equal(t, "idle", compaction.ManualJob.State)

			req, err = http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/sno/compaction/start", addr), nil)
			require.NoError(t, err)
			res, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = res.Body.Close()
			require.Equal(t, http.StatusUnauthorized, res.StatusCode)

			apiKey, err := apikeys.NewService(sno.DB.APIKeys()).Issue(ctx)
			require.NoError(t, err)
			req, err = http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/sno/compaction/start", addr), nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+apiKey.Secret.String())
			res, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { require.NoError(t, res.Body.Close()) }()
			require.Equal(t, http.StatusPreconditionFailed, res.StatusCode)
			var apiError struct {
				Code string `json:"code"`
			}
			require.NoError(t, json.NewDecoder(res.Body).Decode(&apiError))
			require.Equal(t, "manual_log_compaction_disabled", apiError.Code)
		},
	)
}

func TestConsoleStartManualCompaction(t *testing.T) {
	testplanet.Run(t,
		testplanet.Config{
			SatelliteCount:   1,
			StorageNodeCount: 1,
			Reconfigure: testplanet.Reconfigure{
				StorageNode: func(index int, config *storagenode.Config) {
					config.Hashstore.Compaction.ManualLogCompaction = true
				},
			},
		},
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
			sno := planet.StorageNodes[0]
			baseURL := fmt.Sprintf("http://%s/api/sno/compaction", sno.Console.Listener.Addr())
			apiKey, err := apikeys.NewService(sno.DB.APIKeys()).Issue(ctx)
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/start", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+apiKey.Secret.String())
			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, res.StatusCode)
			var accepted struct {
				ManualJob struct {
					ID    uint64 `json:"id"`
					State string `json:"state"`
				} `json:"manualJob"`
			}
			require.NoError(t, json.NewDecoder(res.Body).Decode(&accepted))
			require.NoError(t, res.Body.Close())
			require.Positive(t, accepted.ManualJob.ID)
			require.Equal(t, "running", accepted.ManualJob.State)

			require.Eventually(t, func() bool {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL, nil)
				if err != nil {
					return false
				}
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					return false
				}
				defer func() { _ = res.Body.Close() }()
				var status struct {
					ManualJob struct {
						State string `json:"state"`
					} `json:"manualJob"`
				}
				return json.NewDecoder(res.Body).Decode(&status) == nil && status.ManualJob.State == "succeeded"
			}, 10*time.Second, 10*time.Millisecond)
		},
	)
}
