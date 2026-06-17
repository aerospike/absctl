package flags

import (
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

// ServerRestore holds flags for the server restore start command.
type ServerRestore struct {
	models.ServerRestore
}

func NewServerRestore() *ServerRestore {
	return &ServerRestore{}
}

func (f *ServerRestore) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to restore.")
	flagSet.StringVar(&f.StorageType, "object-storage-type", "", "Type of object storage. "+
		"Example: aws-s3")
	flagSet.StringVar(&f.JobID, "backup-id", "", "Job id used for restore.")

	return flagSet
}

func (f *ServerRestore) GetServerRestore() *models.ServerRestore {
	return &f.ServerRestore
}

// ServerRestorePrepare holds flags for the server restore prepare command.
type ServerRestorePrepare struct {
	models.ServerRestorePrepare
}

func NewServerRestorePrepare() *ServerRestorePrepare {
	return &ServerRestorePrepare{}
}

func (f *ServerRestorePrepare) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Namespace, "namespace", "", "The namespace to restore.")
	flagSet.StringVar(&f.JobID, "backup-id", "", "Job id used for restore.")

	return flagSet
}

func (f *ServerRestorePrepare) GetServerRestorePrepare() *models.ServerRestorePrepare {
	return &f.ServerRestorePrepare
}
