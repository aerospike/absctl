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

// ConfigClusterFields holds CLI inputs for adding or updating a single
// Aerospike cluster definition (DtoAerospikeCluster).
type ConfigClusterFields struct {
	// Name is the cluster name (URL path parameter).
	Name string

	// SeedNodes is a list of seed-node entries in the form host:port[/tls-name].
	// Repeatable flag (--seed-node host:port).
	SeedNodes []string

	// PreferRacks is the list of acceptable racks in order of preference.
	PreferRacks []int

	ConnTimeout          int
	Label                string
	MaxParallelScans     int
	UseServicesAlternate bool

	// Credentials.* override fields under the credential object.
	CredentialsUser            string
	CredentialsPassword        string
	CredentialsPasswordPath    string
	CredentialsAuthMode        string
	CredentialsSecretAgentName string

	// TLS.* override fields under the tls object.
	TLSCaFile          string
	TLSCaPath          string
	TLSCertFile        string
	TLSCipherSuite     string
	TLSKeyFile         string
	TLSKeyFilePassword string
	TLSName            string
	TLSProtocols       string
}

// Validate checks the bare minimum required by the CLI itself. Server-side
// validation of the request body (e.g. seed-nodes presence) is deferred to the
// API.
func (c *ConfigClusterFields) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("--name is required")
	}

	return nil
}
