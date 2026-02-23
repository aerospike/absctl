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

// deferredCertValue wraps a CertFlag to defer file reading when the value is a
// Secret Agent reference (secrets:resource:key). Without this wrapper,
// CertFlag.Set() attempts to read the value as a file path during flag parsing,
// which fails before PersistentPreRunE can resolve secrets.
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
		// Secret was resolved — store the cert content directly,
		// bypassing CertFlag.Set() which would try to read it as a file.
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

// WrapCertFlagsForSecrets replaces CertFlag Values on TLS-related flags with a
// deferred wrapper that accepts Secret Agent references (secrets:resource:key).
// Must be called after NewFlagSet() and before the flagset is parsed by cobra.
func WrapCertFlagsForSecrets(fs *pflag.FlagSet) {
	certFlagNames := []string{"tls-cafile", "tls-certfile", "tls-keyfile"}

	for _, name := range certFlagNames {
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
}
