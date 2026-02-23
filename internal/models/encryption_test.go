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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapEncryptionPolicy_Success(t *testing.T) {
	t.Parallel()

	encryptionModel := &Encryption{
		Mode:      "AES256",
		KeyFile:   "/path/to/keyfile",
		KeyEnv:    "ENV_KEY",
		KeySecret: "secret",
	}

	encryptionPolicy := encryptionModel.Policy()
	assert.NotNil(t, encryptionPolicy)
	assert.Equal(t, "AES256", encryptionPolicy.Mode)
	assert.Equal(t, "/path/to/keyfile", *encryptionPolicy.KeyFile)
	assert.Equal(t, "ENV_KEY", *encryptionPolicy.KeyEnv)
	assert.Equal(t, "secret", *encryptionPolicy.KeySecret)
}

func TestMapEncryptionPolicy_EmptyMode(t *testing.T) {
	t.Parallel()

	encryptionModel := &Encryption{}
	encryptionPolicy := encryptionModel.Policy()
	assert.Nil(t, encryptionPolicy)
}

func TestMapEncryptionPolicy_UpperCaseMode(t *testing.T) {
	t.Parallel()

	encryptionModel := &Encryption{
		Mode: "aes256", // Lowercase mode
	}

	encryptionPolicy := encryptionModel.Policy()
	assert.NotNil(t, encryptionPolicy)
	assert.Equal(t, "AES256", encryptionPolicy.Mode, "Encryption mode should be converted to uppercase")
}
