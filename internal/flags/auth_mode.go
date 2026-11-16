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

package flags

import (
	"fmt"
	"strings"

	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/pflag"
)

const (
	flagAuth = "auth"

	authModeExternalInsecure = "EXTERNAL_INSECURE"
	authModeTypes            = "INTERNAL,EXTERNAL,PKI"
	authModeUsage            = "The authentication mode used by the Aerospike server." +
		" INTERNAL uses standard user/pass. EXTERNAL uses external methods (like LDAP)" +
		" which are configured on the server. EXTERNAL requires TLS. PKI allows TLS" +
		" authentication and authorization based on a certificate. No username needs to be configured."
)

// authModeValue wraps the tools-common-go auth mode flag to reject
// EXTERNAL_INSECURE. The Aerospike Go client refuses external authentication
// without TLS, and with TLS EXTERNAL_INSECURE is the same as EXTERNAL, so the
// mode is hidden from the help and rejected as unknown.
type authModeValue struct {
	original *asFlags.AuthModeFlag
}

func (a *authModeValue) Set(val string) error {
	return SetAuthMode(a.original, val)
}

func (a *authModeValue) String() string {
	return a.original.String()
}

func (a *authModeValue) Type() string {
	return authModeTypes
}

// SetAuthMode sets mode from val, for both the --auth flag and the YAML config.
// Unlike AuthModeFlag.Set, it rejects EXTERNAL_INSECURE, which the Aerospike Go
// client does not support.
func SetAuthMode(mode *asFlags.AuthModeFlag, val string) error {
	if strings.EqualFold(val, authModeExternalInsecure) {
		return fmt.Errorf("unrecognized auth mode")
	}

	return mode.Set(val)
}

// hideInsecureAuthMode replaces the value and usage of the --auth flag so that
// EXTERNAL_INSECURE is neither shown in the help nor accepted.
func hideInsecureAuthMode(fs *pflag.FlagSet) {
	f := fs.Lookup(flagAuth)
	if f == nil {
		return
	}

	mode, ok := f.Value.(*asFlags.AuthModeFlag)
	if !ok {
		return
	}

	f.Value = &authModeValue{original: mode}
	f.Usage = asFlags.DefaultWrapHelpString(authModeUsage)
}
