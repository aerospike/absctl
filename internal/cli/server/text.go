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

// Package server hosts the "absctl server backup/restore" command tree.
//
// All user-facing strings (command names, short/long descriptions and the
// help banner) live in this file so they can be consumed by the documentation
// generator without pulling in the command wiring.
package server

// textWelcomeMessage is the banner printed at the top of every help screen.
const textWelcomeMessage = "Welcome to the Aerospike backup CLI tool!"

// Command names (cobra "Use" values).
const (
	useBackup   = "backup"
	useRestore  = "restore"
	useStart    = "start"
	useList     = "list"
	useProgress = "progress"
	usePrepare  = "prepare"
	useValidate = "validate"
)

// Short descriptions (cobra "Short" values).
const (
	shortBackup         = "Manage server-integrated backups"
	shortBackupStart    = "Start a server-integrated backup"
	shortBackupList     = "List server-integrated backups"
	shortBackupProgress = "Show the progress of a backup"
	shortBackupValidate = "Validate server-integrated backups"
	shortRestore        = "Manage server-integrated restores"
	shortRestoreStart   = "Start a server-integrated restore"
	shortRestorePrepare = "Prepare a server-integrated restore"
)

// Long descriptions (cobra "Long" values).
const (
	longBackupStart    = "Start a server-integrated backup on the Aerospike cluster."
	longBackupList     = "List available server-integrated backups from the configured storage."
	longBackupProgress = "Show the progress of a currently running server-integrated backup."
	longBackupValidate = "Validate available server-integrated backups from the configured storage."
	longRestoreStart   = "Start a server-integrated restore on the Aerospike cluster."
	longRestorePrepare = "Prepare a server-integrated restore on the Aerospike cluster."
)
