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

package models

import "fmt"

type StorageCommon struct {
	MaxConnsPerHost int
	RequestTimeout  int

	CalculateChecksum bool

	RetryReadBackoff     int
	RetryReadMultiplier  float64
	RetryReadMaxAttempts uint
}

func (s *StorageCommon) Validate(isBackup bool) error {
	if s.MaxConnsPerHost < 0 {
		return fmt.Errorf("max connections per host must be non-negative")
	}

	if s.RequestTimeout < 0 {
		return fmt.Errorf("request timeout must be non-negative")
	}

	if !isBackup {
		if s.RetryReadMultiplier < 1 {
			return fmt.Errorf("retry read multiplier must be positive")
		}

		if s.RetryReadBackoff < 1 {
			return fmt.Errorf("retry read timeout must be positive")
		}
	}

	return nil
}
