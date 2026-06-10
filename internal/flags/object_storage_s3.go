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
	"github.com/aerospike/absctl/internal/models"
	"github.com/spf13/pflag"
)

type ObjectStorageS3 struct {
	models.ObjectStorageS3
}

func NewObjectStorageS3() *ObjectStorageS3 {
	return &ObjectStorageS3{}
}

func (f *ObjectStorageS3) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.BucketName, flagS3BucketName,
		models.DefaultS3BucketName,
		"Existing S3 bucket name")

	flagSet.StringVar(&f.Region, flagS3Region,
		models.DefaultS3Region,
		"The S3 region that the bucket(s) exist in.")

	flagSet.StringVar(&f.Profile, flagS3Profile,
		models.DefaultS3Profile,
		"The S3 profile to use for credentials.")

	flagSet.StringVar(&f.AccessKeyID, flagS3AccessKeyID,
		models.DefaultS3AccessKeyID,
		"S3 access key ID. If not set, profile auth info will be used.")

	flagSet.StringVar(&f.SecretAccessKey, flagS3SecretAccessKey,
		models.DefaultS3SecretAccessKey,
		"S3 secret access key. If not set, profile auth info will be used.")

	flagSet.StringVar(&f.Endpoint, flagS3Endpoint,
		models.DefaultS3Endpoint,
		"An alternate URL endpoint to send S3 API calls to.")

	return flagSet
}

func (f *ObjectStorageS3) GetObjectStorageS3() *models.ObjectStorageS3 {
	return &f.ObjectStorageS3
}

func (f *ObjectStorageS3) ToAwsS3() *models.AwsS3 {
	return &models.AwsS3{
		BucketName:      f.BucketName,
		Region:          f.Region,
		Endpoint:        f.Endpoint,
		Profile:         f.Profile,
		AccessKeyID:     f.AccessKeyID,
		SecretAccessKey: f.SecretAccessKey,
	}
}
