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
	"fmt"
	"strings"

	"github.com/aerospike/backup-go"
)

const (
	encryptionNone   = "NONE"
	encryptionAES128 = "AES128"
	encryptionAES256 = "AES256"
)

// Encryption contains flags that will be mapped to EncryptionPolicy for backup and restore operations.
type Encryption struct {
	Mode      string
	KeyFile   string
	KeyEnv    string
	KeySecret string
}

// Policy converts Encryption to backup.EncryptionPolicy.
func (e *Encryption) Policy() *backup.EncryptionPolicy {
	if e == nil {
		return nil
	}

	if e.Mode == "" || strings.EqualFold(e.Mode, encryptionNone) {
		return nil
	}

	p := &backup.EncryptionPolicy{
		Mode: strings.ToUpper(e.Mode),
	}

	if e.KeyFile != "" {
		p.KeyFile = &e.KeyFile
	}

	if e.KeyEnv != "" {
		p.KeyEnv = &e.KeyEnv
	}

	if e.KeySecret != "" {
		p.KeySecret = &e.KeySecret
	}

	return p
}

func (e *Encryption) Validate() error {
	if e.Mode != "" {
		if !strings.EqualFold(e.Mode, encryptionAES128) &&
			!strings.EqualFold(e.Mode, encryptionAES256) &&
			!strings.EqualFold(e.Mode, encryptionNone) {
			return fmt.Errorf("invalid encryption mode: %s", e.Mode)
		}
	}

	if (e.KeyFile != "" || e.KeyEnv != "" || e.KeySecret != "") &&
		(e.Mode == "" || strings.EqualFold(e.Mode, encryptionNone)) {
		return fmt.Errorf("--encrypt must be specified when using " +
			"--encryption-key-file, --encryption-key-env, or --encryption-key-secret")
	}

	var count int

	if e.KeyFile != "" {
		count++
	}

	if e.KeyEnv != "" {
		count++
	}

	if e.KeySecret != "" {
		count++
	}

	if count > 1 {
		return fmt.Errorf("only one of --encryption-key-file, " +
			"--encryption-key-env, or --encryption-key-secret can be specified")
	}

	return nil
}
