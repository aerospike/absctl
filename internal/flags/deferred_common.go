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
	"strings"

	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/pflag"
)

const secretsPrefix = "secrets:"

// Flags for Aerospike connection.
const (
	flagTLSCaFile          = "tls-cafile"
	flagTLSCertFile        = "tls-certfile"
	flagTLSKeyFile         = "tls-keyfile"
	flagTLSCapath          = "tls-capath"
	flagTLSKeyFilePassword = "tls-keyfile-password" //nolint:gosec // It's not a hardcoded password.

	flagHost         = "host"
	flagPort         = "port"
	flagUser         = "user"
	flagPassword     = "password"
	flagTLSName      = "tls-name"
	flagTLSProtocols = "tls-protocols"
)

// deferredCertValue wraps a CertFlag to defer file reading when the value is a
// Secret Agent reference (secrets:resource:key). Without this wrapper,
// CertFlag.Set() attempts to read the value as a file path during flag parsing,
// which fails before PersistentPreRunE can resolve secrets.
//
// After resolution the cert content is assigned directly, bypassing
// CertFlag.Set() which would interpret it as a file path.
type deferredCertValue struct {
	pendingSecret bool
	raw           string
	original      *asFlags.CertFlag
}

func (d *deferredCertValue) Set(val string) error {
	if strings.HasPrefix(val, secretsPrefix) {
		d.pendingSecret = true
		d.raw = val

		return nil
	}

	if d.pendingSecret {
		d.pendingSecret = false
		d.raw = ""
		*d.original = asFlags.CertFlag(val)

		return nil
	}

	return d.original.Set(val)
}

func (d *deferredCertValue) String() string {
	if d.raw != "" {
		return d.raw
	}

	return d.original.String()
}

func (d *deferredCertValue) Type() string {
	return d.original.Type()
}

// deferredSecretValue wraps any pflag.Value to defer validation when the value
// is a Secret Agent reference (secrets:resource:key). Unlike deferredCertValue,
// the resolved value is passed through to the original Set() method, which is
// correct for types like HostTLSPortSliceFlag, TLSProtocolsFlag, CertPathFlag
// and int where Set() can parse the resolved string.
type deferredSecretValue struct {
	pendingSecret bool
	raw           string
	original      pflag.Value
}

func (d *deferredSecretValue) Set(val string) error {
	if strings.HasPrefix(val, secretsPrefix) {
		d.pendingSecret = true
		d.raw = val

		return nil
	}

	if d.pendingSecret {
		d.pendingSecret = false
		d.raw = ""
	}

	return d.original.Set(val)
}

func (d *deferredSecretValue) String() string {
	if d.raw != "" {
		return d.raw
	}

	return d.original.String()
}

func (d *deferredSecretValue) Type() string {
	return d.original.Type()
}

// WrapFlagsForSecrets replaces flag Values on Aerospike connection flags with
// deferred wrappers that accept Secret Agent references (secrets:resource:key).
// Must be called after NewFlagSet() and before the flagset is parsed by cobra.
//
// CertFlag values get a specialised wrapper that bypasses Set() after
// resolution (the resolved value is cert content, not a file path).
// All other custom types get a generic wrapper that delegates to the original
// Set() after resolution.
func WrapFlagsForSecrets(fs *pflag.FlagSet) {
	// CertFlag types: resolved value is cert content, must bypass Set().
	for _, name := range []string{flagTLSCaFile, flagTLSCertFile, flagTLSKeyFile} {
		f := fs.Lookup(name)
		if f == nil {
			continue
		}

		cert, ok := f.Value.(*asFlags.CertFlag)
		if !ok {
			continue
		}

		f.Value = &deferredCertValue{original: cert}
	}

	// Other flag types whose Set() would reject a secrets: value during
	// parsing. The resolved value is passed through to the original Set().
	for _, name := range []string{flagHost, flagPort, flagTLSCapath, flagTLSProtocols} {
		f := fs.Lookup(name)
		if f == nil {
			continue
		}

		f.Value = &deferredSecretValue{original: f.Value}
	}
}
