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

func TestEncryption_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enc     Encryption
		wantErr string
	}{
		{
			name: "empty encryption is valid",
			enc:  Encryption{},
		},
		{
			name: "AES128 with key file",
			enc:  Encryption{Mode: "AES128", KeyFile: "/path/to/key"},
		},
		{
			name: "AES256 with key env",
			enc:  Encryption{Mode: "AES256", KeyEnv: "MY_KEY"},
		},
		{
			name: "lowercase aes128 with key secret",
			enc:  Encryption{Mode: "aes128", KeySecret: "secret:key"},
		},
		{
			name: "lowercase aes256 with key file",
			enc:  Encryption{Mode: "aes256", KeyFile: "/path/to/key"},
		},
		{
			name: "mode with no key source is valid",
			enc:  Encryption{Mode: "AES256"},
		},
		{
			name:    "invalid mode",
			enc:     Encryption{Mode: "BLOWFISH"},
			wantErr: "invalid encryption mode: BLOWFISH",
		},
		{
			name: "NONE mode is valid",
			enc:  Encryption{Mode: "NONE"},
		},
		{
			name: "none mode lowercase is valid",
			enc:  Encryption{Mode: "none"},
		},
		{
			name:    "key file without mode",
			enc:     Encryption{KeyFile: "/path/to/key"},
			wantErr: "--encrypt must be specified",
		},
		{
			name:    "key env without mode",
			enc:     Encryption{KeyEnv: "MY_KEY"},
			wantErr: "--encrypt must be specified",
		},
		{
			name:    "key secret without mode",
			enc:     Encryption{KeySecret: "secret:key"},
			wantErr: "--encrypt must be specified",
		},
		{
			name:    "multiple keys file and env",
			enc:     Encryption{Mode: "AES256", KeyFile: "/path", KeyEnv: "KEY"},
			wantErr: "only one of",
		},
		{
			name:    "multiple keys file and secret",
			enc:     Encryption{Mode: "AES256", KeyFile: "/path", KeySecret: "secret"},
			wantErr: "only one of",
		},
		{
			name:    "multiple keys env and secret",
			enc:     Encryption{Mode: "AES256", KeyEnv: "KEY", KeySecret: "secret"},
			wantErr: "only one of",
		},
		{
			name:    "all three keys",
			enc:     Encryption{Mode: "AES256", KeyFile: "/path", KeyEnv: "KEY", KeySecret: "secret"},
			wantErr: "only one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.enc.Validate()
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
