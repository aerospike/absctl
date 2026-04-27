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

package flags

import (
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

// ServiceConfigCluster holds flags for the `service config clusters add|update`
// commands. Each leaf field of DtoAerospikeCluster is exposed as an individual
// flag; --file may be used as a base body that flags then override.
type ServiceConfigCluster struct {
	models.ConfigClusterFields
}

func NewServiceConfigCluster() *ServiceConfigCluster {
	return &ServiceConfigCluster{}
}

func (f *ServiceConfigCluster) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Name, "name", "",
		"Cluster name (used as the URL path parameter).")

	flagSet.StringVar(&f.File, "file", "",
		"Path to a JSON file with a DtoAerospikeCluster body. "+
			"Other flags override fields loaded from this file.")

	flagSet.StringArrayVar(&f.SeedNodes, "seed-node", nil,
		"Seed node entry in the form host:port[/tls-name]. Repeatable.")

	flagSet.IntSliceVar(&f.PreferRacks, "prefer-racks", nil,
		"Comma-separated list of acceptable racks in order of preference.")

	flagSet.IntVar(&f.ConnTimeout, "conn-timeout", 0,
		"Connection timeout in milliseconds.")

	flagSet.StringVar(&f.Label, "label", "",
		"Cluster label (optional, used in logs and error messages).")

	flagSet.IntVar(&f.MaxParallelScans, "max-parallel-scans", 0,
		"Maximum number of parallel scans allowed across the cluster.")

	flagSet.BoolVar(&f.UseServicesAlternate, "use-services-alternate", false,
		"Use 'services-alternate' instead of 'services' during cluster tending.")

	flagSet.StringVar(&f.CredentialsUser, "credentials-user", "",
		"Aerospike cluster username.")

	flagSet.StringVar(&f.CredentialsPassword, "credentials-password", "",
		"Aerospike cluster password (plain text or secret-agent path).")

	flagSet.StringVar(&f.CredentialsPasswordPath, "credentials-password-path", "",
		"Path to a file containing the cluster password.")

	flagSet.StringVar(&f.CredentialsAuthMode, "credentials-auth-mode", "",
		"Authentication mode (INTERNAL, EXTERNAL, PKI).")

	flagSet.StringVar(&f.CredentialsSecretAgentName, "credentials-secret-agent-name", "",
		"Name of a preconfigured secret agent for credentials.")

	flagSet.StringVar(&f.TLSCaFile, "tls-ca-file", "",
		"Path to a trusted CA certificate file in PEM format.")

	flagSet.StringVar(&f.TLSCaPath, "tls-ca-path", "",
		"Path to a directory of trusted CA certificates.")

	flagSet.StringVar(&f.TLSCertFile, "tls-cert-file", "",
		"Path to a client certificate file for mutual TLS.")

	flagSet.StringVar(&f.TLSCipherSuite, "tls-cipher-suite", "",
		"TLS cipher selection (OpenSSL Cipher List Format).")

	flagSet.StringVar(&f.TLSKeyFile, "tls-key-file", "",
		"Path to a client private key file for mutual TLS.")

	flagSet.StringVar(&f.TLSKeyFilePassword, "tls-key-file-password", "",
		"Password to load a protected TLS key file (env:VAR, file:PATH or PASSWORD).")

	flagSet.StringVar(&f.TLSName, "tls-name", "",
		"TLS name used for server certificate verification (SNI).")

	flagSet.StringVar(&f.TLSProtocols, "tls-protocols", "",
		"TLS protocol selection (Apache SSL Protocol format).")

	return flagSet
}
