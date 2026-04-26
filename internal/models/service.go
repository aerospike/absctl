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

package models

import "fmt"

// ServiceConnection holds the connection parameters for the ABS REST API.
type ServiceConnection struct {
	Host string
	Port int
}

func (s *ServiceConnection) Validate() error {
	if s.Host == "" {
		return fmt.Errorf("service host is required")
	}

	if s.Port <= 0 || s.Port > 65535 {
		return fmt.Errorf("service port must be between 1 and 65535")
	}

	return nil
}

func (s *ServiceConnection) ServerURL() string {
	return fmt.Sprintf("http://%s:%d", s.Host, s.Port)
}
