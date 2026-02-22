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

import (
	"context"
	"fmt"
	"strings"

	"github.com/aerospike/backup-go"
	sa "github.com/aerospike/backup-go/pkg/secret-agent"
)

// SecretAgent contains flags that will be mapped to SecretAgentConfig for backup and restore operations.
type SecretAgent struct {
	ConnectionType     string
	Address            string
	Port               int
	TimeoutMillisecond int

	CaFile   string
	TLSName  string
	CertFile string
	KeyFile  string

	IsBase64 bool

	// Private fields with config for easy use of backup.ParseSecret

	config *backup.SecretAgentConfig
}

// Validate checks if SecretAgent params are valid.
func (s *SecretAgent) Validate() error {
	if s == nil {
		return nil
	}

	if s.ConnectionType == "" {
		return fmt.Errorf("missing connection type")
	}

	if !strings.EqualFold(s.ConnectionType, sa.ConnectionTypeTCP) &&
		!strings.EqualFold(s.ConnectionType, sa.ConnectionTypeUDS) {
		return fmt.Errorf("unsupported connection type: %s", s.ConnectionType)
	}

	return nil
}

func (s *SecretAgent) GetSecret(ctx context.Context, value string) (string, error) {
	return backup.ParseSecret(ctx, s.config, value)
}

func (s *SecretAgent) ToConfig() *backup.SecretAgentConfig {
	if s == nil {
		return nil
	}

	if s.Address == "" {
		return nil
	}

	c := &backup.SecretAgentConfig{}
	c.Address = &s.Address

	if s.ConnectionType != "" {
		ct := strings.ToLower(s.ConnectionType)
		c.ConnectionType = &ct
	}

	if s.Port != 0 {
		c.Port = &s.Port
	}

	if s.TimeoutMillisecond != 0 {
		c.TimeoutMillisecond = &s.TimeoutMillisecond
	}

	if s.CaFile != "" {
		c.CaFile = &s.CaFile
	}

	if s.TLSName != "" {
		c.TLSName = &s.TLSName
	}

	if s.CertFile != "" {
		c.CertFile = &s.CertFile
	}

	if s.KeyFile != "" {
		c.KeyFile = &s.KeyFile
	}

	if s.CertFile != "" {
		c.CertFile = &s.CertFile
	}

	if s.KeyFile != "" {
		c.KeyFile = &s.KeyFile
	}

	if s.IsBase64 {
		c.IsBase64 = &s.IsBase64
	}

	s.config = c

	return c
}
