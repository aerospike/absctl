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

type SecretAgent struct {
	models.SecretAgent
}

func NewSecretAgent() *SecretAgent {
	return &SecretAgent{}
}

func (f *SecretAgent) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.ConnectionType, "sa-connection-type",
		models.DefaultSecretAgentConnectionType,
		"Secret Agent connection type. Supported types: TCP, UNIX.")

	flagSet.StringVar(&f.Address, "sa-address",
		models.DefaultSecretAgentAddress,
		"Secret Agent host for TCP connection or socket file path for UDS connection.")

	flagSet.IntVar(&f.Port, "sa-port",
		models.DefaultSecretAgentPort,
		"Secret Agent port (only for TCP connection).")

	flagSet.IntVar(&f.TimeoutMillisecond, "sa-timeout",
		models.DefaultSecretAgentTimeoutMillisecond,
		"Secret Agent connection and reading timeout.")

	flagSet.StringVar(&f.CaFile, "sa-ca-file",
		models.DefaultSecretAgentCaFile,
		"Path to ca file for encrypted connections.")

	flagSet.StringVar(&f.TLSName, "sa-tls-name",
		models.DefaultSecretAgentTLSName,
		"TLS name (SNI) for encrypted connections.")

	flagSet.StringVar(&f.CertFile, "sa-cert-file",
		models.DefaultSecretAgentCertFile,
		"Path to a client certificate file for mutual TLS authentication.")

	flagSet.StringVar(&f.KeyFile, "sa-key-file",
		models.DefaultSecretAgentKeyFile,
		"Path to a client private key file for mutual TLS authentication.")

	flagSet.BoolVar(&f.IsBase64, "sa-is-base64",
		models.DefaultSecretAgentIsBase64,
		"Whether Secret Agent responses are Base64 encoded.")

	return flagSet
}

func (f *SecretAgent) GetSecretAgent() *models.SecretAgent {
	return &f.SecretAgent
}
