// Copyright 2026 Aerospike, Inc.
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

package server

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/aerospike/backup-go/pkg/estimates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testJobID     = "527069025"
	testNamespace = "source-ns1"

	// logBackupProgress is the message every progress line is logged under.
	logBackupProgress = "backup progress"
)

func TestProgressPrinterShouldPrint(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name    string
		printer *progressPrinter
		state   infomodels.BackupState
		ratio   float64
		elapsed time.Duration
		want    bool
	}{
		{
			name:    "first observation prints",
			printer: &progressPrinter{},
			state:   infomodels.BackupStateInit,
			ratio:   0,
			elapsed: 0,
			want:    true,
		},
		{
			name: "stage transition prints",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				lastRatio:   0.5,
				lastPrinted: now.Add(-time.Second),
			},
			state:   infomodels.BackupStateIncrScanActive,
			ratio:   0.5,
			elapsed: time.Minute,
			want:    true,
		},
		{
			name: "stalled progress prints on heartbeat",
			printer: &progressPrinter{
				state:       infomodels.BackupStateIncrScanActive,
				lastRatio:   0.95,
				lastPrinted: now.Add(-heartbeatInterval),
			},
			state:   infomodels.BackupStateIncrScanActive,
			ratio:   0.95,
			elapsed: 10 * time.Minute,
			want:    true,
		},
		{
			name: "stalled progress stays silent before heartbeat",
			printer: &progressPrinter{
				state:       infomodels.BackupStateIncrScanActive,
				lastRatio:   0.95,
				lastPrinted: now.Add(-heartbeatInterval + time.Second),
			},
			state:   infomodels.BackupStateIncrScanActive,
			ratio:   0.95,
			elapsed: 10 * time.Minute,
			want:    false,
		},
		{
			// elapsed=1h, ratio=0.5 -> totalDuration=2h -> threshold=5s/2h=6.94e-4.
			// delta = 0.5 - 0.4997 = 3e-4, below the threshold.
			name: "delta below threshold stays silent",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				lastRatio:   0.4997,
				lastPrinted: now.Add(-time.Second),
			},
			state:   infomodels.BackupStateBaseScanActive,
			ratio:   0.5,
			elapsed: time.Hour,
			want:    false,
		},
		{
			// Same projection as above, but delta = 0.01 is well above the threshold.
			name: "delta above threshold prints",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				lastRatio:   0.49,
				lastPrinted: now.Add(-time.Second),
			},
			state:   infomodels.BackupStateBaseScanActive,
			ratio:   0.5,
			elapsed: time.Hour,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status := &infomodels.ResponseBackupState{JobID: testJobID, State: tt.state}

			assert.Equal(t, tt.want, tt.printer.shouldPrint(status, tt.ratio, tt.elapsed, now))
		})
	}
}

func TestProgressPrinterSample(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name       string
		printer    *progressPrinter
		state      infomodels.BackupState
		recs       uint64
		sampleTime time.Time
		want       float64
	}{
		{
			name:       "first sample only seeds the baseline",
			printer:    &progressPrinter{},
			state:      infomodels.BackupStateBaseScanActive,
			recs:       1000,
			sampleTime: now,
			want:       0,
		},
		{
			name: "second sample takes the instant rate",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 1000,
			},
			state:      infomodels.BackupStateBaseScanActive,
			recs:       3000,
			sampleTime: now,
			want:       200,
		},
		{
			name: "later samples are smoothed",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 1000,
				recsPerSec:  100,
			},
			state:      infomodels.BackupStateBaseScanActive,
			recs:       3000,
			sampleTime: now,
			want:       130,
		},
		{
			name: "stage transition restarts the measurement",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 1000,
				recsPerSec:  100,
			},
			state:      infomodels.BackupStateIncrScanActive,
			recs:       50,
			sampleTime: now,
			want:       0,
		},
		{
			// RecsBackedUp sums the base, incremental and change lines, so it is not
			// monotonic. Unsigned arithmetic would wrap a drop into a huge rate.
			name: "counter going backwards is read as no progress",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 3000,
				recsPerSec:  100,
			},
			state:      infomodels.BackupStateBaseScanActive,
			recs:       1000,
			sampleTime: now,
			want:       70,
		},
		{
			name: "repeated sample at the same instant is ignored",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now,
				sampledRecs: 1000,
				recsPerSec:  100,
			},
			state:      infomodels.BackupStateBaseScanActive,
			recs:       3000,
			sampleTime: now,
			want:       100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status := &infomodels.ResponseBackupState{
				JobID:        testJobID,
				State:        tt.state,
				RecsBackedUp: tt.recs,
			}

			tt.printer.sample(status, tt.sampleTime)

			assert.InDelta(t, tt.want, tt.printer.recsPerSec, 1e-9)
		})
	}
}

func TestNextPoll(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name      string
		startTime time.Time
		pct       float64
		want      time.Duration
	}{
		{
			name:      "missing start time returns the minimum",
			startTime: time.Time{},
			pct:       50,
			want:      minPollInterval,
		},
		{
			name:      "warmup returns the minimum",
			startTime: now.Add(-2 * time.Second),
			pct:       10,
			want:      minPollInterval,
		},
		{
			name:      "no progress returns the minimum",
			startTime: now.Add(-time.Hour),
			pct:       0,
			want:      minPollInterval,
		},
		{
			// totalDuration = 10s / 0.5 = 20s, threshold clamps to 0.01 -> 200ms.
			name:      "short backup clamps to the minimum",
			startTime: now.Add(-10 * time.Second),
			pct:       50,
			want:      minPollInterval,
		},
		{
			// totalDuration = 1000s / 0.25 = 4000s, threshold = 5s/4000s = 1.25e-3.
			name:      "average backup polls at the target interval",
			startTime: now.Add(-1000 * time.Second),
			pct:       25,
			want:      estimates.TargetPrintInterval,
		},
		{
			// totalDuration = 1h / 0.01 = 100h, threshold clamps to 1e-4 -> 36s.
			name:      "long backup clamps to the maximum",
			startTime: now.Add(-time.Hour),
			pct:       1,
			want:      maxPollInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status := &infomodels.ResponseBackupState{
				JobID:       testJobID,
				State:       infomodels.BackupStateBaseScanActive,
				ProgressPct: tt.pct,
				StartTime:   tt.startTime,
			}

			assert.InDelta(t, float64(tt.want), float64(nextPoll(status, now)), float64(time.Millisecond))
		})
	}
}

// TestProgressPrinterPrint pins down the progress line itself. It carries the same
// fields as the client side backup progress - completion, estimate and speed - so that a
// server side backup reads the same way.
func TestProgressPrinterPrint(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		printer     *progressPrinter
		status      *infomodels.ResponseBackupState
		wantLogs    []string
		notWantLogs []string
	}{
		{
			name: "reports the progress of a running backup",
			status: &infomodels.ResponseBackupState{
				JobID:        testJobID,
				Namespace:    testNamespace,
				State:        infomodels.BackupStateBaseScanActive,
				ProgressPct:  50,
				RecsBackedUp: 1000,
				RecsBase:     1000,
				StartTime:    now.Add(-time.Hour),
			},
			wantLogs: []string{
				logBackupProgress,
				"backup-id=" + testJobID,
				"pct=50",
				"rec/s=0",
				"remaining=1h0m0s",
			},
		},
		{
			name: "rounds the percentage to two decimals",
			status: &infomodels.ResponseBackupState{
				JobID:       testJobID,
				State:       infomodels.BackupStateBaseScanActive,
				ProgressPct: 33.33333,
				StartTime:   now.Add(-time.Hour),
			},
			wantLogs: []string{"pct=33.33"},
		},
		{
			// Without a start time there is nothing to project the estimate from, and a
			// made up one is worse than none.
			name: "omits the estimate when the cluster reported no start time",
			status: &infomodels.ResponseBackupState{
				JobID:       testJobID,
				State:       infomodels.BackupStateInit,
				ProgressPct: 0,
			},
			wantLogs:    []string{logBackupProgress},
			notWantLogs: []string{"remaining="},
		},
		{
			name: "reports the speed measured since the previous sample",
			printer: &progressPrinter{
				state:       infomodels.BackupStateBaseScanActive,
				sampledAt:   now.Add(-10 * time.Second),
				sampledRecs: 1000,
			},
			status: &infomodels.ResponseBackupState{
				JobID:        testJobID,
				State:        infomodels.BackupStateBaseScanActive,
				ProgressPct:  50,
				RecsBackedUp: 3000,
				StartTime:    now.Add(-time.Hour),
			},
			wantLogs: []string{"rec/s=200"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			printer := tt.printer
			if printer == nil {
				printer = &progressPrinter{}
			}

			printer.logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

			require.True(t, printer.Print(tt.status, now))

			for _, want := range tt.wantLogs {
				assert.Contains(t, buf.String(), want)
			}

			for _, notWant := range tt.notWantLogs {
				assert.NotContains(t, buf.String(), notWant)
			}
		})
	}
}

// TestProgressPrinterPrintSkipsRepeatedStatus covers the state the printer keeps between
// calls, which a single table case cannot reach: a status that repeats the last one
// carries nothing new and must not produce a second line.
func TestProgressPrinterPrintSkipsRepeatedStatus(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	printer := newProgressPrinter(logger)

	now := time.Now()

	status := &infomodels.ResponseBackupState{
		JobID:        testJobID,
		Namespace:    testNamespace,
		State:        infomodels.BackupStateBaseScanActive,
		ProgressPct:  50,
		RecsBackedUp: 1000,
		StartTime:    now.Add(-time.Hour),
	}

	require.True(t, printer.Print(status, now))

	buf.Reset()

	assert.False(t, printer.Print(status, now.Add(time.Second)))
	assert.Empty(t, buf.String())
}

func TestBackupDuration(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 9, 22, 10, 36, 53, 0, time.UTC)

	tests := []struct {
		name       string
		startTime  time.Time
		finishTime time.Time
		want       time.Duration
		wantOK     bool
	}{
		{
			name:       "both ends reported",
			startTime:  start,
			finishTime: start.Add(90 * time.Second),
			want:       90 * time.Second,
			wantOK:     true,
		},
		{
			// The server reports "-" for the finish time of a job that is still running.
			name:      "running job has no finish time",
			startTime: start,
			wantOK:    false,
		},
		{
			name:       "job that never started",
			finishTime: start,
			wantOK:     false,
		},
		{
			// Merging per node statuses takes the earliest start and the latest finish,
			// but a clock skew between nodes can still invert them.
			name:       "finish time before start time",
			startTime:  start,
			finishTime: start.Add(-time.Second),
			wantOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status := &infomodels.ResponseBackupState{
				JobID:      testJobID,
				StartTime:  tt.startTime,
				FinishTime: tt.finishTime,
			}

			got, ok := backupDuration(status)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
