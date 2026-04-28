// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build debug

package main

import (
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"time"
)

const pprofAddr = "localhost:6060"

func init() {
	go func() {
		// Binds to loopback only. In containers, access via: docker exec <id> curl localhost:6060/debug/pprof/
		slog.Info("Starting profiler", "addr", pprofAddr)
		srv := &http.Server{
			Addr:              pprofAddr,
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Profiler stopped", slog.Any("error", err))
		}
	}()
}
