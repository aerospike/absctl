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
	UseSnapshotBackup  = "snapshot-backup"
	UseSnapshotRestore = "snapshot-restore"
	UseStart           = "start"
	UseList            = "list"
	UseProgress        = "progress"
	UsePrepare         = "prepare"
	UseValidate        = "validate"
	UseAbort           = "abort"
	UseOpen            = "open"
	UseClearStorage    = "clear-storage"
)

// Short descriptions (cobra "Short" values).
const (
	ShortBackup          = "Manage server-integrated backups"
	ShortBackupStart     = "Start a server-integrated backup"
	ShortBackupList      = "List server-integrated backups"
	ShortBackupProgress  = "Show the progress of a backup"
	ShortBackupValidate  = "Validate server-integrated backups"
	ShortRestore         = "Manage server-integrated restores"
	ShortRestoreStart    = "Start a server-integrated restore"
	ShortRestorePrepare  = "Prepare a server-integrated restore"
	ShortRestoreProgress = "Show the progress of a restore"
	ShortBackupAbort     = "Abort a server-integrated backup"
	ShortRestoreAbort    = "Abort a server-integrated restore"
	ShortRestoreOpen     = "Release the namespace of an aborted cold restore"
	ShortRestoreClear    = "Clear the namespace storage for a cold restore"
)

// Long descriptions (cobra "Long" values).
const (
	LongBackupStart = "Start a server-integrated backup on the Aerospike cluster and\n" +
		"follow its progress until it completes. Interrupting the command\n" +
		"stops the reporting only; use the abort command to stop the backup.\n" +
		"With --async the command returns as soon as the cluster accepts\n" +
		"the backup."
	LongBackupList     = "List available server-integrated backups from the configured storage."
	LongBackupProgress = "Show the progress of a currently running server-integrated backup."
	LongBackupValidate = "Validate available server-integrated backups from the configured storage."
	LongRestoreStart   = "Start a server-integrated restore on the Aerospike cluster and\n" +
		"follow its progress until it completes. Interrupting the command\n" +
		"stops the reporting only; use the abort command to stop the restore.\n" +
		"With --async the command returns as soon as the cluster accepts\n" +
		"the restore."
	LongRestorePrepare  = "Prepare a server-integrated restore on the Aerospike cluster."
	LongRestoreProgress = "Show the progress of a server-integrated restore job.\n" +
		"With --watch the progress is followed until the restore completes."
	LongBackupAbort  = "Abort a server-integrated backup on the Aerospike cluster."
	LongRestoreAbort = "Abort a server-integrated restore on the Aerospike cluster.\n" +
		"A restore job is identified by the namespace it restores into and\n" +
		"by its job id, so both are required. An aborted cold restore keeps\n" +
		"the namespace closed to clients until it is released with the open\n" +
		"command."
	LongRestoreOpen = "Release the namespace held by an aborted cold restore, so that it\n" +
		"opens to clients again, and wait until it does. The data the restore\n" +
		"wrote so far stays in the namespace. With --async the command\n" +
		"returns as soon as the cluster accepts the request."
	LongRestoreClear = "Clear the storage of the namespace a cold restore is preparing, on\n" +
		"every node that asks for it, and wait until the namespace is ready\n" +
		"for the restore. Clearing erases all data of the namespace on those\n" +
		"nodes. With --async the command returns as soon as the nodes accept\n" +
		"the request."
)
