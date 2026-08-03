// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package consoleserver_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"storj.io/common/testcontext"
	"storj.io/storj/private/testplanet"
)

func TestConsole(t *testing.T) {
	testplanet.Run(t,
		testplanet.Config{
			SatelliteCount:   1,
			StorageNodeCount: 1,
		},
		func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
			satellite := planet.Satellites[0]
			console := planet.StorageNodes[0].Console

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
				Compacting     bool `json:"compacting"`
				SalvageEnabled bool `json:"salvageEnabled"`
				RuntimeTotals  struct {
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
		},
	)
}
