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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrimTrailingWhitespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		give string
		want string
	}{
		{name: "no trailing whitespace", give: "a\nb\n", want: "a\nb\n"},
		{name: "trailing spaces", give: "a  \nb \n", want: "a\nb\n"},
		{name: "trailing tabs", give: "a\t\nb\t \n", want: "a\nb\n"},
		{name: "keeps leading indentation", give: "  a \n\tb\t\n", want: "  a\n\tb\n"},
		{name: "whitespace-only lines", give: "a\n   \nb", want: "a\n\nb"},
		{name: "empty", give: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, trimTrailingWhitespace(tt.give))
		})
	}
}
