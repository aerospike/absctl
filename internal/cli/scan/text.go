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

// Package scan hosts the "absctl backup/restore" command tree for scan-based
// backups and restores.
//
// All user-facing strings (command names, short/long descriptions and the
// help banner) live in this file so they can be consumed by the documentation
// generator without pulling in the command wiring.
package scan

const (
	textWelcomeMessageBackup  = "Welcome to the Aerospike backup CLI tool!"
	textWelcomeMessageRestore = "Welcome to the Aerospike restore CLI tool!"
)

// Command names (cobra "Use" values).
const (
	UseBackup  = "backup"
	UseRestore = "restore"
)

// Short descriptions (cobra "Short" values).
const (
	ShortBackup  = "Aerospike backup CLI tool"
	ShortRestore = "Aerospike restore CLI tool"
)

// Long descriptions (cobra "Long" values).
const (
	LongBackup  = textWelcomeMessageBackup
	LongRestore = textWelcomeMessageRestore
)
